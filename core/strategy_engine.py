# core/strategy_engine.py

from common.logger import log
from core.es_logger import ESLogger


class StrategyEngine:
    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.es = ESLogger()
        self.target_interval = "10m"
        self.THRESHOLD = 0.5

        # State tracking: {stock: {'obv': state, 'clv': state, 'active_side': side}}
        self.last_states = {}

    async def run_logic(self, bar):
        stock = bar.stock_name
        if bar.interval != self.target_interval:
            return

        if stock not in self.last_states:
            self.last_states[stock] = {'obv': 'neutral', 'clv': 'neutral', 'active_side': None}

        div = bar.raw_scores.get('divergence', {})
        obv_val = div.get('price_vs_obv', 0.0)
        clv_val = div.get('price_vs_clv', 0.0)

        curr_obv = self._get_state(obv_val)
        curr_clv = self._get_state(clv_val)
        prev = self.last_states[stock]

        # 1. EXIT LOGIC: Conviction Flip (CLV reverses)
        if prev['active_side']:
            if (prev['active_side'] == 'LONG' and curr_clv == 'bearish') or \
                    (prev['active_side'] == 'SHORT' and curr_clv == 'bullish'):
                await self._fire_log(bar, "EXIT", prev['active_side'], div, "CONVICTION_FLIP")
                self.last_states[stock]['active_side'] = None
                # Do not return; we want to check if a new signal starts immediately

        # 2. TRIGGER/WATCH LOGIC (On State Change)
        if curr_obv != prev['obv'] or curr_clv != prev['clv']:
            # BULLISH
            if curr_obv == 'bullish':
                if curr_clv == 'bullish':
                    await self._fire_log(bar, "ENTRY", "LONG", div)
                    self.last_states[stock]['active_side'] = 'LONG'
                else:
                    await self._fire_log(bar, "WATCH", "BULLISH", div)

            # BEARISH
            elif curr_obv == 'bearish':
                if curr_clv == 'bearish':
                    await self._fire_log(bar, "ENTRY", "SHORT", div)
                    self.last_states[stock]['active_side'] = 'SHORT'
                else:
                    await self._fire_log(bar, "WATCH", "BEARISH", div)

            # 3. VWAP REVERSAL: The 14:30 Trap Detection
            dist_pct = abs(bar.close - bar.session_vwap) / bar.session_vwap if bar.session_vwap else 0
            if dist_pct > 0.005:  # Price is >0.5% away from VWAP
                if (bar.close > bar.session_vwap and curr_obv == 'bearish'):
                    await self._fire_log(bar, "REVERSAL_WARN", "SHORT_FADE", div)

        # Update persistent state
        self.last_states[stock].update({'obv': curr_obv, 'clv': curr_clv})

    def _get_state(self, val):
        if val > self.THRESHOLD: return 'bullish'
        if val < -self.THRESHOLD: return 'bearish'
        return 'neutral'

    async def _fire_log(self, bar, event, side, div, reason=None):
        """Console output and ES ingestion."""
        log.info(f"🔔 [{event}] {bar.stock_name} {side} @ {bar.close}")
        await self.es.log_event(bar.stock_name, event, side, bar.close, bar.session_vwap, div, reason)