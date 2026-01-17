# core/alert_engine.py

from collections import deque
from common.logger import log
from common import strategy_config as s_cfg
from core import db_writer


class AlertEngine:
    """
    The GIDH Alert Engine.
    Sensors: COST (Intent), PATH (Structure), ACCEPTANCE (Confirmation).
    Keyed by (stock, interval) for timeframe-specific conviction and performance tracking.
    """

    def __init__(self, db_pool):
        self.db_pool = db_pool
        # Stores trade lifecycle state: position, entry_price, peak_price, mae_price
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

        # Initialize state and regime history for the specific (stock, interval)
        state = self.states.setdefault(key, {
            "position": "NONE",
            "entry_price": 0.0,
            "peak_price": 0.0,
            "mae_price": 0.0
        })
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

        # --- 2. LIVE TRADE MONITORING (Track MFE/MAE) ---
        if state["position"] == "LONG":
            state["peak_price"] = max(state["peak_price"], bar.high)
            state["mae_price"] = min(state["mae_price"], bar.low)
        elif state["position"] == "SHORT":
            state["peak_price"] = min(state["peak_price"], bar.low)
            state["mae_price"] = max(state["mae_price"], bar.high)

        # --- 3. ALERT LOGIC ---
        if state["position"] == "NONE":
            # LONG ENTRY
            if cost == 1 and accept == 1 and path != -1:
                state.update({
                    "position": "LONG",
                    "entry_price": bar.close,
                    "peak_price": bar.high,
                    "mae_price": bar.low
                })
                await self._fire_alert(bar, "LONG_ENTRY", "COST+PATH+ACCEPTANCE", cost, path, accept, state)

            # SHORT ENTRY
            elif cost == -1 and accept == -1 and path != 1:
                state.update({
                    "position": "SHORT",
                    "entry_price": bar.close,
                    "peak_price": bar.low,
                    "mae_price": bar.high
                })
                await self._fire_alert(bar, "SHORT_ENTRY", "COST+PATH+ACCEPTANCE", cost, path, accept, state)

        elif state["position"] == "LONG":
            # EXIT: Intent fades or structure flips
            if cost < 1 or path < 0:
                await self._fire_alert(bar, "LONG_EXIT", "INTENT_FADE_OR_PATH_FLIP", cost, path, accept, state)
                state.update({"position": "NONE", "entry_price": 0.0, "peak_price": 0.0, "mae_price": 0.0})

        elif state["position"] == "SHORT":
            # EXIT: Intent fades or structure flips
            if cost > -1 or path > 0:
                await self._fire_alert(bar, "SHORT_EXIT", "INTENT_FADE_OR_PATH_FLIP", cost, path, accept, state)
                state.update({"position": "NONE", "entry_price": 0.0, "peak_price": 0.0, "mae_price": 0.0})

    async def _fire_alert(self, bar, event_type, reason, cost, path, accept, state):
        """Standardized signal logging to the database signals table."""
        authority = self.authority_map.get(bar.interval, "unknown")
        is_exit = "EXIT" in event_type

        # Calculate Final Report metrics on Exit
        mfe, mae, pnl = None, None, None
        if is_exit and state["entry_price"] > 0:
            entry = state["entry_price"]
            if "LONG" in event_type:
                mfe = (state["peak_price"] - entry) / entry
                mae = (state["mae_price"] - entry) / entry
                pnl = (bar.close - entry) / entry
            else:  # SHORT
                mfe = (entry - state["peak_price"]) / entry
                mae = (entry - state["mae_price"]) / entry
                pnl = (entry - bar.close) / entry

        event_data = {
            'event_time': bar.timestamp,
            'stock_name': bar.stock_name,
            'interval': bar.interval,
            'authority': authority,
            'event_type': event_type,
            'side': 'LONG' if 'LONG' in event_type else 'SHORT',
            'price': bar.close,
            'vwap': bar.session_vwap,
            'cost_regime': cost,
            'path_regime': path,
            'accept_regime': accept,
            # For EXIT rows, we populate the full trade report
            'entry_price': state["entry_price"] if is_exit else bar.close,
            'peak_price': state["peak_price"] if is_exit else bar.high,
            'mfe_pct': round(mfe * 100, 4) if mfe is not None else None,
            'mae_pct': round(mae * 100, 4) if mae is not None else None,
            'pnl_pct': round(pnl * 100, 4) if pnl is not None else None,
            'indicators': {
                **bar.raw_scores,
                'authority': authority
            },
            'reason': f"[{authority.upper()}] {reason}"
        }

        log.info(f"🔔 [{event_type}] {bar.stock_name} ({bar.interval}/{authority}) @ {bar.close} | {reason}")
        if is_exit:
            log.info(
                f"📊 Final Report | PnL: {event_data['pnl_pct']}% | MFE: {event_data['mfe_pct']}% | MAE: {event_data['mae_pct']}%")

        await db_writer.log_signal_event(self.db_pool, event_data)