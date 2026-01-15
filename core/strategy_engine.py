# core/strategy_engine.py

from common.logger import log
from core.es_logger import ESLogger
from common import strategy_config as s_cfg
from core import db_writer  # Required for signal accounting


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
                'entry_time': None,  # Link to DB signal record
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
        3-Minute Engine: Handles Entry timing, Risk Management, and Profit Resolution.
        """
        state = self.states[bar.stock_name]
        div = bar.raw_scores.get('divergence', {})

        # Update Tape Sensor
        state['pressure_3m'] = div.get('price_vs_clv', 0.0)

        if state['position'] == 'NONE':
            await self._check_entries(bar, state)
        else:
            # 1. Structural Stop (Risk Protection with Veto)
            if await self._check_hard_stop(bar, state):
                return

            # 2. 3m Timing Exits (Divergence Resolution & Exhaustion)
            await self._check_3m_exits(bar, state)

    async def _check_entries(self, bar, state):
        """Entry logic: Counter-retail pullback in valid institutional regimes."""
        div = bar.raw_scores.get('divergence', {})
        price = bar.close

        side = None
        if state['regime'] == "BULL" and state['pressure_3m'] < -s_cfg.PRESSURE_ENTRY_THRESHOLD:
            side = 'LONG'
            state['stop_price'] = price * (1 - s_cfg.STOP_LOSS_PCT)
        elif state['regime'] == "BEAR" and state['pressure_3m'] > s_cfg.PRESSURE_ENTRY_THRESHOLD:
            side = 'SHORT'
            state['stop_price'] = price * (1 + s_cfg.STOP_LOSS_PCT)

        if side:
            state['position'] = side
            state['entry_price'] = price
            state['entry_time'] = bar.timestamp

            # DB INSERT for Expectancy Accounting
            signal_data = {
                'timestamp': bar.timestamp,
                'stock_name': bar.stock_name,
                'interval': bar.interval,
                'side': side,
                'entry_price': price,
                'quantity': 100,  # Example fixed quantity
                'div_obv': div.get('price_vs_obv', 0.0),
                'div_clv': div.get('price_vs_clv', 0.0),
                'status': 'OPEN'
            }
            await db_writer.insert_signal(self.db_pool, signal_data)
            await self._fire_log(bar, "ENTRY", side, div, "REGIME_ALIGN_PULLBACK")

    async def _check_3m_exits(self, bar, state):
        """
        3m Exits: Capture profits when divergence resolves or retail panics.
        """
        div = bar.raw_scores.get('divergence', {})
        pv_clv = div.get('price_vs_clv', 0.0)
        pv_obv = div.get('price_vs_obv', 0.0)
        pv_vwap = div.get('price_vs_vwap', 0.0)

        # 1️⃣ DIVERGENCE RESOLUTION EXIT (Primary Profit Mechanism)
        # Fires when retail selling stops AND institutions stop absorbing
        if state['position'] == 'LONG':
            if pv_clv > 0 and (pv_obv < 0 or pv_vwap < 0):
                await self._handle_exit(bar, state, "DIVERGENCE_RESOLVED")
                return

        if state['position'] == 'SHORT':
            if pv_clv < 0 and (pv_obv > 0 or pv_vwap > 0):
                await self._handle_exit(bar, state, "DIVERGENCE_RESOLVED")
                return

        # 2️⃣ PANIC / NEWS EXHAUSTION (Failsafe)
        if (state['position'] == 'LONG' and pv_clv > s_cfg.PRESSURE_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and pv_clv < -s_cfg.PRESSURE_EXIT_THRESHOLD):
            await self._handle_exit(bar, state, "PRESSURE_EXHAUSTION")

    async def _check_hard_stop(self, bar, state):
        """
        Improved Stop Loss: Structural Veto.
        Prevents noise-based exits if the 5m PATH is still robust.
        """
        price_hit = (state['position'] == 'LONG' and bar.close <= state['stop_price']) or \
                    (state['position'] == 'SHORT' and bar.close >= state['stop_price'])

        if price_hit:
            # Structural Veto Logic: If trend is strong (> 0.20), we HOLD
            if state['position'] == 'LONG' and state['path_5m'] > (s_cfg.PATH_CHOP_THRESHOLD + 0.05):
                log.info(f"🛡️ [VETO] {bar.stock_name} hit SL but 5m PATH is strong ({state['path_5m']:.2f})")
                return False

            if state['position'] == 'SHORT' and state['path_5m'] < -(s_cfg.PATH_CHOP_THRESHOLD + 0.05):
                log.info(f"🛡️ [VETO] {bar.stock_name} hit SL but 5m PATH is strong ({state['path_5m']:.2f})")
                return False

            await self._handle_exit(bar, state, "STRUCTURAL_STOP_LOSS")
            return True
        return False

    async def _check_5m_exits(self, bar, state):
        """5m Rules: Exit on institutional flip or trend death."""
        div = bar.raw_scores.get('divergence', {})

        if (state['position'] == 'LONG' and state['cost_5m'] < s_cfg.COST_EXIT_THRESHOLD) or \
                (state['position'] == 'SHORT' and state['cost_5m'] > s_cfg.COST_EXIT_THRESHOLD):
            await self._handle_exit(bar, state, "COST_FLIP")
        elif -s_cfg.PATH_CHOP_THRESHOLD < state['path_5m'] < s_cfg.PATH_CHOP_THRESHOLD:
            await self._handle_exit(bar, state, "TREND_BREAK")

    async def _handle_exit(self, bar, state, reason):
        """
        Centralized exit handler for Expectancy Accounting.
        Updates the live_signals table with PnL and exit data.
        """
        entry_price = state['entry_price']
        exit_price = bar.close

        # Calculate PnL %: (Win% * AvgWin) - (Loss% * AvgLoss)
        if state['position'] == 'LONG':
            pnl_pct = (exit_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - exit_price) / entry_price

        # Update DB for Accounting
        exit_data = {
            'stock_name': bar.stock_name,
            'entry_time': state.get('entry_time'),
            'exit_timestamp': bar.timestamp,
            'exit_price': exit_price,
            'exit_reason': reason,
            'pnl_pct': pnl_pct * 100,  # Stored as percentage
            'pnl_cash': pnl_pct * entry_price * 100,
            'status': 'CLOSED'
        }

        try:
            await db_writer.update_signal_exit(self.db_pool, exit_data)
        except Exception as e:
            log.error(f"Failed to update signal accounting for {bar.stock_name}: {e}")

        # Final Log
        await self._fire_log(bar, "EXIT", state['position'], bar.raw_scores.get('divergence', {}), reason)

        # Reset position state
        state['position'] = 'NONE'
        state['entry_time'] = None

    async def _fire_log(self, bar, event, side, div, reason):
        """Centralized logging for console and Elasticsearch context."""
        state = self.states[bar.stock_name]
        log.info(f"🔔 [{event}] {bar.stock_name} {side} @ {bar.close} | Reason: {reason}")
        await self.es.log_event(
            stock_name=bar.stock_name, event_type=event, side=side, price=bar.close,
            vwap=bar.session_vwap, scores=div, tick_timestamp=bar.timestamp,
            entry_price=state.get('entry_price'), stop_loss=state.get('stop_price'), reason=reason
        )