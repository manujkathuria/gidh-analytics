from collections import deque
from typing import Dict, Any
import numpy as np

from common.models import TickData, EnrichedTick
from common.logger import log

# Threshold for confirming a hidden order through refills.
ICEBERG_CONFIRMATION_THRESHOLD = 2


class FeatureEnricher:
    """
    A stateful class that enriches raw TickData with calculated features,
    including trade sign, large trade detection, and market absorption.
    """

    def __init__(self):
        # A dictionary to hold the state for each instrument.
        self.instrument_states: Dict[int, Dict[str, Any]] = {}

    def load_thresholds(self, thresholds: Dict[str, int], token_to_name_map: Dict[int, str]):
        """ Loads pre-calculated thresholds into the state for each instrument. """
        log.info("Loading large trade thresholds into FeatureEnricher state...")
        for token, name in token_to_name_map.items():
            state = self._get_instrument_state(token)
            if name in thresholds:
                state["large_trade_threshold"] = thresholds[name]
                log.info(f"  - Set pre-calculated threshold for {name} to {thresholds[name]}")
            else:
                state["large_trade_threshold"] = float('inf')
                log.warning(f"  - No pre-calculated threshold for {name}. Will use dynamic fallback.")

    def _get_instrument_state(self, instrument_token: int) -> Dict[str, Any]:
        """Initializes and retrieves the state for a given instrument."""
        if instrument_token not in self.instrument_states:
            self.instrument_states[instrument_token] = {
                "last_tick": None,
                "last_best_bid_price": 0.0,
                "last_best_ask_price": 0.0,
                "last_best_bid_qty": 0,
                "last_best_ask_qty": 0,
                "hidden_sell_order_refill_count": 0,
                "hidden_buy_order_refill_count": 0,
                "large_trade_threshold": float('inf'),
                "last_trade_sign": 0,
                # NEW: A rolling window for the dynamic threshold fallback
                "trade_volume_window": deque(maxlen=1000)
            }
        return self.instrument_states[instrument_token]

    def _classify_trade_sign(self, tick: TickData, state: Dict[str, Any]) -> int:
        """Returns +1 (buy), -1 (sell), or 0 (unknown) with robust fallbacks."""
        last_tick = state.get("last_tick")
        lp = tick.last_price

        if lp is None:
            return state.get("last_trade_sign", 0)

        cur_bid_px = tick.depth.buy[0].price if tick.depth and tick.depth.buy else state.get("last_best_bid_price", 0.0)
        cur_ask_px = tick.depth.sell[0].price if tick.depth and tick.depth.sell else state.get("last_best_ask_price",
                                                                                               0.0)

        if cur_bid_px > 0 and cur_ask_px > 0:
            locked = (cur_ask_px == cur_bid_px)
            crossed = (cur_ask_px < cur_bid_px)

            if locked or crossed:
                if last_tick and last_tick.last_price is not None:
                    if lp > last_tick.last_price: return 1
                    if lp < last_tick.last_price: return -1
                return state.get("last_trade_sign", 0)

            if lp >= cur_ask_px: return 1
            if lp <= cur_bid_px: return -1

        if last_tick and last_tick.last_price is not None:
            if lp > last_tick.last_price: return 1
            if lp < last_tick.last_price: return -1

        return state.get("last_trade_sign", 0)

    def enrich_tick(self, tick: TickData, data_window: deque) -> EnrichedTick:
        """
        Calculates enrichment features for a single tick.
        """
        instrument_token = tick.instrument_token
        state = self._get_instrument_state(instrument_token)
        last_tick = state["last_tick"]

        tick_volume = 0
        if last_tick and tick.volume_traded is not None and last_tick.volume_traded is not None:
            tick_volume = tick.volume_traded - last_tick.volume_traded
            if tick_volume < 0: tick_volume = 0

        trade_sign = self._classify_trade_sign(tick, state)

        # --- MODIFIED: Large Trade Logic with Fallback ---
        is_large_trade = False
        if tick_volume > 0:
            threshold = state.get("large_trade_threshold", float('inf'))

            # Primary Method: Use pre-calculated threshold if available
            if threshold != float('inf'):
                if tick_volume >= threshold:
                    is_large_trade = True
            # Fallback Method: Use dynamic rolling percentile
            else:
                window = state["trade_volume_window"]
                # Only calculate if the window has enough data for a stable result
                if len(window) > 200:
                    p99_threshold = np.percentile(list(window), 99)
                    if tick_volume >= p99_threshold:
                        is_large_trade = True
                # Always add the current volume to the window for future calculations
                window.append(tick_volume)

        is_buy_absorption = False
        is_sell_absorption = False
        if tick.depth and tick.depth.buy and tick.depth.sell and tick_volume > 0:
            best_bid = tick.depth.buy[0]
            best_ask = tick.depth.sell[0]

            if best_ask.price != state["last_best_ask_price"]:
                state["hidden_sell_order_refill_count"] = 0
            elif trade_sign == 1 and tick.last_price == state["last_best_ask_price"]:
                if best_ask.quantity > (state["last_best_ask_qty"] - tick_volume):
                    state["hidden_sell_order_refill_count"] += 1

            if best_bid.price != state["last_best_bid_price"]:
                state["hidden_buy_order_refill_count"] = 0
            elif trade_sign == -1 and tick.last_price == state["last_best_bid_price"]:
                if best_bid.quantity > (state["last_best_bid_qty"] - tick_volume):
                    state["hidden_buy_order_refill_count"] += 1

            if state["hidden_sell_order_refill_count"] >= ICEBERG_CONFIRMATION_THRESHOLD:
                is_sell_absorption = True
            if state["hidden_buy_order_refill_count"] >= ICEBERG_CONFIRMATION_THRESHOLD:
                is_buy_absorption = True

        enriched_tick = EnrichedTick(
            timestamp=tick.timestamp, instrument_token=tick.instrument_token,
            stock_name=tick.stock_name, last_price=tick.last_price,
            last_traded_quantity=tick.last_traded_quantity, average_traded_price=tick.average_traded_price,
            volume_traded=tick.volume_traded, total_buy_quantity=tick.total_buy_quantity,
            total_sell_quantity=tick.total_sell_quantity, ohlc_open=tick.ohlc_open,
            ohlc_high=tick.ohlc_high, ohlc_low=tick.ohlc_low, ohlc_close=tick.ohlc_close,
            change=tick.change, depth=tick.depth,
            tick_volume=tick_volume, trade_sign=trade_sign, is_large_trade=is_large_trade,
            is_buy_absorption=is_buy_absorption, is_sell_absorption=is_sell_absorption
        )

        state["last_tick"] = tick
        state["last_trade_sign"] = trade_sign
        if tick.depth and tick.depth.buy:
            state["last_best_bid_price"] = tick.depth.buy[0].price
            state["last_best_bid_qty"] = tick.depth.buy[0].quantity
        if tick.depth and tick.depth.sell:
            state["last_best_ask_price"] = tick.depth.sell[0].price
            state["last_best_ask_qty"] = tick.depth.sell[0].quantity

        return enriched_tick
