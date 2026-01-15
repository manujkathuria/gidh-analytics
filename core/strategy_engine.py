# core/strategy_engine.py

from common.logger import log
from core.es_logger import ESLogger


class StrategyEngine:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.es = ESLogger()

        # State tracking per stock: {stock_name: state_dict}
        self.states = {}

    async def run_logic(self, bar):
        """
        Processes finalized bars to determine market regime (5m)
        and trigger entries/exits (3m). LVC (Effort) is excluded.
        """
        stock = bar.stock_name
        interval = bar.interval

        # Initialize state for new stocks
        if stock not in self.states:
            self.states[stock] = {
                'regime': 'NO_TRADE',
                'position': 'NONE',
                'path_5m': 0.0,
                'cost_5m': 0.0,
                'pressure_3m': 0.0
            }

        state = self.states[stock]
        div = bar.raw_scores.get('divergence', {})

        # --- 1. REGIME ENGINE (5-Minute Bars) ---
        # Defines the 'What' and 'Where' of the trade
        if interval == "5m":
            # Sensors: PATH (Structure) + COST (Institutional Positioning)
            state['path_5m'] = bar.raw_scores.get('structure_ratio', 0.0)
            state['cost_5m'] = (div.get('price_vs_vwap', 0.0) + div.get('price_vs_obv', 0.0)) / 2

            # Define Regime: Requires both trend and institutional alignment
            if state['path_5m'] > 0.25 and state['cost_5m'] > 0.25:
                state['regime'] = "BULL"
            elif state['path_5m'] < -0.25 and state['cost_5m'] < -0.25:
                state['regime'] = "BEAR"
            else:
                state['regime'] = "NO_TRADE"

            # Check for 5m Exit Rules (Institutional & Trend Stops)
            if state['position'] != 'NONE':
                await self._check_5m_exits(bar, state)

        # --- 2. ENTRY & TIMING ENGINE (3-Minute Bars) ---
        # Defines the 'When' (Tape pullbacks)
        elif interval == "3m":
            # Sensor: PRESSURE (Tape control via CLV)
            state['pressure_3m'] = div.get('price_vs_clv', 0.0)

            # Entry Logic: Only if no current position and in a valid regime
            if state['position'] == 'NONE':
                await self._check_entries(bar, state)
            # Panic Exit Logic: React to extreme tape shifts
            else:
                await self._check_3m_exits(bar, state)

    async def _check_entries(self, bar, state):
        div = bar.raw_scores.get('divergence', {})

        # LONG ENTRY: BULL Regime + Retail Selling (Tape Pullback)
        if state['regime'] == "BULL" and state['pressure_3m'] < -0.4:
            state['position'] = 'LONG'
            await self._fire_log(bar, "ENTRY", "LONG", div, "REGIME_ALIGN_PULLBACK")

        # SHORT ENTRY: BEAR Regime + Retail Chasing (Tape Rally)
        elif state['regime'] == "BEAR" and state['pressure_3m'] > 0.4:
            state['position'] = 'SHORT'
            await self._fire_log(bar, "ENTRY", "SHORT", div, "REGIME_ALIGN_PULLBACK")

    async def _check_5m_exits(self, bar, state):
        div = bar.raw_scores.get('divergence', {})

        # Institutional Stop: Exit if the average institutional cost basis flips
        if (state['position'] == 'LONG' and state['cost_5m'] < 0) or \
                (state['position'] == 'SHORT' and state['cost_5m'] > 0):
            await self._fire_log(bar, "EXIT", state['position'], div, "COST_FLIP")
            state['position'] = 'NONE'

        # Trend Stop: Exit if the trend loses conviction (Path collapses)
        elif -0.15 < state['path_5m'] < 0.15:
            await self._fire_log(bar, "EXIT", state['position'], div, "TREND_BREAK")
            state['position'] = 'NONE'

    async def _check_3m_exits(self, bar, state):
        div = bar.raw_scores.get('divergence', {})

        # Panic Exit: Exit on extreme pressure readings (squeeze or news shock)
        if (state['position'] == 'LONG' and state['pressure_3m'] > 0.8) or \
                (state['position'] == 'SHORT' and state['pressure_3m'] < -0.8):
            await self._fire_log(bar, "EXIT", state['position'], div, "PRESSURE_EXHAUSTION")
            state['position'] = 'NONE'

    async def _fire_log(self, bar, event, side, div, reason):
        """Standardized logging to console and Elasticsearch ingestion."""
        log.info(f"🔔 [{event}] {bar.stock_name} {side} @ {bar.close} | Reason: {reason}")
        await self.es.log_event(
            bar.stock_name,
            event,
            side,
            bar.close,
            bar.session_vwap,
            div,
            reason
        )