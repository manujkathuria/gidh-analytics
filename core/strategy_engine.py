# core/strategy_engine.py

from common.logger import log
from core.es_logger import ESLogger
from common import strategy_config as s_cfg


class StrategyEngine:
    def __init__(self, db_pool):
        """
        Initializes the 3-Sensor Trading Engine.
        Sensors: PATH (Structure), COST (Institutional Value), PRESSURE (Tape Timing).
        """
        self.db_pool = db_pool
        self.es = ESLogger()

        # State tracking per stock: {stock_name: state_dict}
        self.states = {}

    async def run_logic(self, bar):
        """
        Main entry point triggered by the DataPipeline on finalized bars.
        Routes bars to the correct processing engine based on interval.
        """
        stock = bar.stock_name
        interval = bar.interval

        # Initialize state for new stocks if not present
        if stock not in self.states:
            self.states[stock] = {
                'regime': 'NO_TRADE',
                'position': 'NONE',
                'entry_price': 0.0,
                'stop_price': 0.0,
                'path_5m': 0.0,
                'cost_5m': 0.0,
                'pressure_3m': 0.0
            }

        # Route to specific timeframe logic
        if interval == s_cfg.REGIME_INTERVAL:
            await self._process_regime(bar)
        elif interval == s_cfg.TIMING_INTERVAL:
            await self._process_timing(bar)

    async def _process_regime(self, bar):
        """
        5-Minute Engine: Defines Direction and Institutional alignment.
        """
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})

        # Update Sensors
        state['path_5m'] = bar.raw_scores.get('structure_ratio', 0.0)
        state['cost_5m'] = (div.get('price_vs_vwap', 0.0) + div.get('price_vs_obv', 0.0)) / 2

        # Define Market Regime
        if state['path_5m'] > s_cfg.PATH_REGIME_THRESHOLD and state['cost_5m'] > s_cfg.COST_REGIME_THRESHOLD:
            state['regime'] = "BULL"
        elif state['path_5m'] < -s_cfg.PATH_REGIME_THRESHOLD and state['cost_5m'] < -s_cfg.COST_REGIME_THRESHOLD:
            state['regime'] = "BEAR"
        else:
            state['regime'] = "NO_TRADE"

        # If in a trade, check for 5m Exit Rules (Institutional flip or Trend death)
        if state['position'] != 'NONE':
            await self._check_5m_exits(bar, state)

    async def _process_timing(self, bar):
        """
        3-Minute Engine: Handles Entry timing and immediate risk management.
        """
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})

        # Update Tape Sensor
        state['pressure_3m'] = div.get('price_vs_clv', 0.0)

        if state['position'] == 'NONE':
            # Look for pullbacks in valid regimes
            await self._check_entries(bar, state)
        else:
            # Monitor risk on every 3m bar
            await self._check_hard_stop(bar, state)
            await self._check_3m_exits(bar, state)

    async def _check_entries(self, bar, state):
        """Logic for counter-retail pullback entries."""
        div = bar.raw_scores.get('divergence', {})
        price = bar.close

        # LONG: Bull Regime + Tape Pullback (Retail Selling)
        if state['regime'] == "BULL" and state['pressure_3m'] < -s_cfg.PRESSURE_ENTRY_THRESHOLD:
            state['position'] = 'LONG'
            state['entry_price'] = price
            state['stop_price'] = price * (1 - s_cfg.STOP_LOSS_PCT)
            await self._fire_log(bar, "ENTRY", "LONG", div, "REGIME_ALIGN_PULLBACK")

        # SHORT: Bear Regime + Tape Rally (Retail Chasing)
        elif state['regime'] == "BEAR" and state['pressure_3m'] > s_cfg.PRESSURE_ENTRY_THRESHOLD:
            state['position'] = 'SHORT'
            state['entry_price'] = price
            state['stop_price'] = price * (1 + s_cfg.STOP_LOSS_PCT)
            await self._fire_log(bar, "ENTRY", "SHORT", div, "REGIME_ALIGN_PULLBACK")

    async def _check_5m_exits(self, bar, state):
        """5m Rules: Institutional flip or Trend death."""
        div = bar.raw_scores.get('divergence', {})

        # COST Flip: Institutions moved against us
        if (state['position'] == 'LONG' and state['cost_5m'] < s_cfg.COST_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and state['cost_5m'] > s_cfg.COST_EXIT_THRESHOLD):
            await self._fire_log(bar, "EXIT", state['position'], div, "COST_FLIP")
            state['position'] = 'NONE'

        # Trend Break: PATH fell into chop range
        elif -s_cfg.PATH_CHOP_THRESHOLD < state['path_5m'] < s_cfg.PATH_CHOP_THRESHOLD:
            await self._fire_log(bar, "EXIT", state['position'], div, "TREND_BREAK")
            state['position'] = 'NONE'

    async def _check_hard_stop(self, bar, state):
        """Standard 0.30% Price-based Stop Loss."""
        if (state['position'] == 'LONG' and bar.close <= state['stop_price']) or \
                (state['position'] == 'SHORT' and bar.close >= state['stop_price']):
            await self._fire_log(bar, "EXIT", state['position'], {}, "HARD_STOP_LOSS")
            state['position'] = 'NONE'

    async def _check_3m_exits(self, bar, state):
        """3m Rule: Panic exhaustion exit."""
        div = bar.raw_scores.get('divergence', {})

        if (state['position'] == 'LONG' and state['pressure_3m'] > s_cfg.PRESSURE_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and state['pressure_3m'] < -s_cfg.PRESSURE_EXIT_THRESHOLD):
            await self._fire_log(bar, "EXIT", state['position'], div, "PRESSURE_EXHAUSTION")
            state['position'] = 'NONE'

    async def _fire_log(self, bar, event, side, div, reason):
        """Centralized logging for console and Elasticsearch context."""
        state = self.states[bar.stock_name]
        log.info(f"🔔 [{event}] {bar.stock_name} {side} @ {bar.close} | Reason: {reason}")

        await self.es.log_event(
            stock_name=bar.stock_name,
            event_type=event,
            side=side,
            price=bar.close,
            vwap=bar.session_vwap,
            scores=div,
            tick_timestamp=bar.timestamp,  # Market time for ES timeline
            entry_price=state.get('entry_price'),
            stop_loss=state.get('stop_price'),
            reason=reason
        )