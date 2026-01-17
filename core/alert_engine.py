# core/alert_engine.py

from collections import deque
from common.logger import log
from common import strategy_config as s_cfg
from core import db_writer


class AlertEngine:
    """
    The GIDH Alert Engine.
    Sensors: COST (Intent), PATH (Structure), ACCEPTANCE (Confirmation).
    Keyed by (stock, interval) for timeframe-specific conviction.
    """

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.states = {}
        # Persistence history for the 3-bar handshake rule
        self.regime_hist = {}

        # Maps interval to its operational authority
        self.authority_map = {
            "1m": "micro",
            "3m": "fast",
            "5m": "trade",
            "10m": "swing",
            "15m": "structural"
        }

    def _update_regime(self, hist: deque, value: float, threshold: float) -> int:
        """Returns +1/-1 only if intent/structure persists for 3 bars."""
        hist.append(value)
        if len(hist) < hist.maxlen:
            return 0
        if all(v > threshold for v in hist):
            return 1
        if all(v < -threshold for v in hist):
            return -1
        return 0

    async def run_logic(self, bar):
        """Main entry point triggered on every finalized bar per interval."""
        stock = bar.stock_name
        interval = bar.interval
        key = (stock, interval)

        state = self.states.setdefault(key, {"position": "NONE"})
        h = self.regime_hist.setdefault(key, {
            "cost": deque(maxlen=3),
            "path": deque(maxlen=3)
        })

        # --- 1. SENSOR MAPPING ---
        # COST: Institutional Intent (OBV Divergence)
        div = bar.raw_scores.get('divergence', {})
        raw_cost = div.get('price_vs_obv', 0.0)
        cost = self._update_regime(h["cost"], raw_cost, s_cfg.COST_REGIME_THRESHOLD)

        # PATH: Directional Bias (Structure Ratio)
        raw_path = bar.raw_scores.get('structure_ratio', 0.0)
        path = self._update_regime(h["path"], raw_path, s_cfg.PATH_REGIME_THRESHOLD)

        # ACCEPTANCE: Confirmation (Range break result from BarAggregator)
        accept = bar.raw_scores.get("price_acceptance", 0)

        # --- 2. ALERT LOGIC ---
        if state["position"] == "NONE":
            # LONG ENTRY: Intent(3-bar) + Confirmation + Structure Not Opposing
            if cost == 1 and accept == 1 and path != -1:
                state["position"] = "LONG"
                await self._fire_alert(bar, "LONG_ENTRY", "COST+PATH+ACCEPTANCE")

            # SHORT ENTRY: Intent(3-bar) + Confirmation + Structure Not Opposing
            elif cost == -1 and accept == -1 and path != 1:
                state["position"] = "SHORT"
                await self._fire_alert(bar, "SHORT_ENTRY", "COST+PATH+ACCEPTANCE")

        elif state["position"] == "LONG":
            # EXIT: Intent fades or structure flips
            if cost < 1 or path < 0:
                state["position"] = "NONE"
                await self._fire_alert(bar, "LONG_EXIT", "INTENT_FADE_OR_PATH_FLIP")

        elif state["position"] == "SHORT":
            # EXIT: Intent fades or structure flips
            if cost > -1 or path > 0:
                state["position"] = "NONE"
                await self._fire_alert(bar, "SHORT_EXIT", "INTENT_FADE_OR_PATH_FLIP")

    async def _fire_alert(self, bar, event_type, reason):
        """Standardized signal logging to the database signals table."""
        authority = self.authority_map.get(bar.interval, "unknown")

        event_data = {
            'event_time': bar.timestamp,
            'stock_name': bar.stock_name,
            'event_type': event_type,
            'side': 'LONG' if 'LONG' in event_type else 'SHORT',
            'price': bar.close,
            'vwap': bar.session_vwap,
            'stop_loss': None,
            'authority': authority,  # <--- ADD THIS LINE
            'indicators': {
                **bar.raw_scores,
                'authority': authority
            },
            'reason': f"[{authority.upper()}] {reason}",
            'pnl_pct': None,
            'interval': bar.interval
        }
        log.info(f"🔔 [{event_type}] {bar.stock_name} ({bar.interval}/{authority}) @ {bar.close} | {reason}")
        await db_writer.log_signal_event(self.db_pool, event_data)