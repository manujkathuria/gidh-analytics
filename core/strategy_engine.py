# core/strategy_engine.py

from common.logger import log
from common import strategy_config as s_cfg
from core import db_writer


class StrategyEngine:
    def __init__(self, db_pool):
        """
        Initializes the 3-Sensor Trading Engine.
        Sensors: PATH (Structure), COST (Institutional), PRICE_TIMING (Tape).
        """
        self.db_pool = db_pool
        self.states = {}

    async def run_logic(self, bar):
        """
        Main entry point triggered on finalized bars.
        """
        stock = bar.stock_name
        interval = bar.interval

        if stock not in self.states:
            self.states[stock] = {
                'regime': 'NO_TRADE',
                'position': 'NONE',
                'scaled_out': False,  # Tracks if partial profit was secured
                'entry_price': 0.0,
                'entry_time': None,
                'stop_price': 0.0,
                'path_5m': 0.0,
                'cost_5m': 0.0,
                'price_timing': 0.0  # Renamed for timing interval clarity
            }

        if interval == s_cfg.REGIME_INTERVAL:
            await self._process_regime(bar)
        elif interval == s_cfg.TIMING_INTERVAL:
            await self._process_timing(bar)

    async def _process_regime(self, bar):
        """10-Minute Engine: Defines Direction and Institutional alignment."""
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})

        state['path_5m'] = bar.raw_scores.get('structure_ratio', 0.0)
        state['cost_5m'] = (div.get('price_vs_vwap', 0.0) + div.get('price_vs_obv', 0.0)) / 2

        if state['path_5m'] > s_cfg.PATH_REGIME_THRESHOLD and state['cost_5m'] > s_cfg.COST_REGIME_THRESHOLD:
            state['regime'] = "BULL"
        elif state['path_5m'] < -s_cfg.PATH_REGIME_THRESHOLD and state['cost_5m'] < -s_cfg.COST_REGIME_THRESHOLD:
            state['regime'] = "BEAR"
        else:
            state['regime'] = "NO_TRADE"

        if state['position'] != 'NONE':
            await self._check_5m_exits(bar, state)

    async def _process_timing(self, bar):
        """Timing Engine: Handles pullback entries and exhaustion exits."""
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})
        state['price_timing'] = div.get('price_vs_clv', 0.0)

        if state['position'] == 'NONE':
            await self._check_entries(bar, state)
        else:
            # 1. Structural Stop (Risk Protection with Veto)
            if await self._check_hard_stop(bar, state):
                return
            # 2. Timing Exits (Scaling & Exhaustion)
            await self._check_timing_exits(bar, state)

    async def _check_entries(self, bar, state):
        """Institutional entry: Buy the pullback in a strong regime."""
        price = bar.close
        side = None

        # ENTRY CONDITION: Trend is confirmed, but Tape shows a temporary pullback
        if state['regime'] == "BULL" and state['price_timing'] < -s_cfg.PRESSURE_ENTRY_THRESHOLD:
            side = 'LONG'
            state['stop_price'] = price * (1 - s_cfg.STOP_LOSS_PCT)
        elif state['regime'] == "BEAR" and state['price_timing'] > s_cfg.PRESSURE_ENTRY_THRESHOLD:
            side = 'SHORT'
            state['stop_price'] = price * (1 + s_cfg.STOP_LOSS_PCT)

        if side:
            state['pending_side'] = side
            state['position'] = side
            state['scaled_out'] = False
            state['entry_price'] = price
            state['entry_time'] = bar.timestamp
            await self._fire_signal(bar, "ENTRY", "REGIME_ALIGN_PULLBACK")

    async def _check_timing_exits(self, bar, state):
        """Tiered scaling: Secures 50% profit on resolution, holds 50% for trend."""
        div = bar.raw_scores.get('divergence', {})
        pv_clv = div.get('price_vs_clv', 0.0)
        pv_obv = div.get('price_vs_obv', 0.0)
        pv_vwap = div.get('price_vs_vwap', 0.0)

        # 1️⃣ PARTIAL PROFIT (Capture the "Quick Game")
        if not state['scaled_out']:
            if state['position'] == 'LONG' and pv_clv > 0 and (pv_obv < 0 or pv_vwap < 0):
                await self._handle_partial_exit(bar, state, "DIVERGENCE_RESOLVED")
                return
            if state['position'] == 'SHORT' and pv_clv < 0 and (pv_obv > 0 or pv_vwap > 0):
                await self._handle_partial_exit(bar, state, "DIVERGENCE_RESOLVED")
                return

        # 2️⃣ EXTREME EXHAUSTION (Full Exit)
        if (state['position'] == 'LONG' and pv_clv > s_cfg.PRESSURE_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and pv_clv < -s_cfg.PRESSURE_EXIT_THRESHOLD):
            await self._handle_exit(bar, state, "PRESSURE_EXHAUSTION")

    async def _check_hard_stop(self, bar, state):
        """Strengthened Structural Veto: Trust structure over price hunting."""
        price_hit = (state['position'] == 'LONG' and bar.close <= state['stop_price']) or \
                    (state['position'] == 'SHORT' and bar.close >= state['stop_price'])

        if price_hit:
            # VETO: Hold if PATH (structure) remains above the Chop threshold
            if state['position'] == 'LONG' and state['path_5m'] > s_cfg.PATH_CHOP_THRESHOLD:
                log.info(f"🛡️ [VETO] {bar.stock_name} hit stop-price but structure is trending.")
                return False
            if state['position'] == 'SHORT' and state['path_5m'] < -s_cfg.PATH_CHOP_THRESHOLD:
                log.info(f"🛡️ [VETO] {bar.stock_name} hit stop-price but structure is trending.")
                return False

            await self._handle_exit(bar, state, "STRUCTURAL_STOP_LOSS")
            return True
        return False

    async def _check_5m_exits(self, bar, state):
        """Regime Failure: Exit when institutions flip or trend dies."""
        if (state['position'] == 'LONG' and state['cost_5m'] < s_cfg.COST_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and state['cost_5m'] > s_cfg.COST_EXIT_THRESHOLD):
            await self._handle_exit(bar, state, "COST_FLIP")
        elif -s_cfg.PATH_CHOP_THRESHOLD < state['path_5m'] < s_cfg.PATH_CHOP_THRESHOLD:
            await self._handle_exit(bar, state, "TREND_BREAK")

    async def _handle_partial_exit(self, bar, state, reason):
        """Locks in 50% profit without clearing state."""
        state['scaled_out'] = True
        log.info(f"✂️ [PARTIAL] {bar.stock_name} scaling out | {reason}")
        await self._fire_signal(bar, "PARTIAL_EXIT", reason)

    async def _handle_exit(self, bar, state, reason):
        """Full position close and reset."""
        pnl = (bar.close - state['entry_price']) / state['entry_price'] if state['position'] == 'LONG' else (state[
                                                                                                                 'entry_price'] - bar.close) / \
                                                                                                            state[
                                                                                                                'entry_price']
        await self._fire_signal(bar, "EXIT", reason, pnl_pct=pnl * 100)
        state['position'] = 'NONE'
        state['scaled_out'] = False
        state['entry_time'] = None

    async def _fire_signal(self, bar, event_type, reason, pnl_pct=None):
        """Standardizes logging to database."""
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})
        event_data = {
            'event_time': bar.timestamp, 'stock_name': bar.stock_name, 'event_type': event_type,
            'side': state['position'] if event_type != 'ENTRY' else state['pending_side'],
            'price': bar.close, 'vwap': bar.session_vwap, 'stop_loss': state['stop_price'],
            'indicators': {
                'obv_score': div.get('price_vs_obv', 0),
                'clv_score': div.get('price_vs_clv', 0),
                'structure_ratio': bar.raw_scores.get('structure_ratio', 0)
            },
            'reason': reason, 'pnl_pct': pnl_pct, 'interval': bar.interval
        }
        log.info(f"🔔 [{event_type}] {bar.stock_name} @ {bar.close} | {reason}")
        await db_writer.log_signal_event(self.db_pool, event_data)