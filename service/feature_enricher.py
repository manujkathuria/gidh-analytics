from collections import deque
from typing import Dict, Any
import numpy as np

from service.models import TickData, EnrichedTick
from service.logger import log

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
        # A dictionary to hold the large trade thresholds for each instrument.
        self.large_trade_thresholds: Dict[int, int] = {}

    def load_thresholds(self, thresholds: Dict[str, int], token_to_name_map: Dict[int, str]):
        """ Loads pre-calculated thresholds into the state for each instrument. """
        log.info("Loading large trade thresholds into FeatureEnricher state...")
        for token, name in token_to_name_map.items():
            state = self._get_instrument_state(token)  # Ensures state dictionary exists
            if name in thresholds:
                state["large_trade_threshold"] = thresholds[name]
                log.info(f"  - Set threshold for {name} to {thresholds[name]}")
            else:
                # If a threshold isn't in the DB, set it high to avoid false positives
                state["large_trade_threshold"] = float('inf')
                log.warning(f"  - No large trade threshold found for {name} ({token}).")

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
                "last_trade_sign": 0, # Initialize last trade sign
            }
        return self.instrument_states[instrument_token]

    def _classify_trade_sign(self, tick: TickData, state: Dict[str, Any]) -> int:
        """Returns +1 (buy), -1 (sell), or 0 (unknown) with robust fallbacks."""
        last_tick = state.get("last_tick")
        lp = tick.last_price

        if lp is None:
            return state.get("last_trade_sign", 0)

        # Prefer current quotes from this tick; fallback to stored ones
        cur_bid_px = tick.depth.buy[0].price if tick.depth and tick.depth.buy else state.get("last_best_bid_price", 0.0)
        cur_ask_px = tick.depth.sell[0].price if tick.depth and tick.depth.sell else state.get("last_best_ask_price", 0.0)
        cur_bid_qty = tick.depth.buy[0].quantity if tick.depth and tick.depth.buy else state.get("last_best_bid_qty", 0)
        cur_ask_qty = tick.depth.sell[0].quantity if tick.depth and tick.depth.sell else state.get("last_best_ask_qty", 0)

        prev_bid_px = state.get("last_best_bid_price", 0.0)
        prev_ask_px = state.get("last_best_ask_price", 0.0)
        prev_bid_qty = state.get("last_best_bid_qty", 0)
        prev_ask_qty = state.get("last_best_ask_qty", 0)

        # If we have a book at all, handle special cases first
        if cur_bid_px > 0 and cur_ask_px > 0:
            locked = (cur_ask_px == cur_bid_px)
            crossed = (cur_ask_px < cur_bid_px)

            # Handle locked books: ambiguous, so use tie-breakers and fallbacks
            if locked:
                if lp > cur_ask_px: return 1
                if lp < cur_bid_px: return -1
                # If trade is at the locked price, use queue outflow or tick rule
                if cur_ask_px == prev_ask_px and cur_bid_px == prev_bid_px:
                    if prev_ask_qty and cur_ask_qty < prev_ask_qty: return 1
                    if prev_bid_qty and cur_bid_qty < prev_bid_qty: return -1
                # Fall back to tick rule if outflow is not conclusive
                if last_tick and last_tick.last_price is not None:
                    if lp > last_tick.last_price: return 1
                    if lp < last_tick.last_price: return -1
                return state.get("last_trade_sign", 0)

            # Handle crossed books: quotes are unreliable, so use tick rule directly
            if crossed:
                if last_tick and last_tick.last_price is not None:
                    if lp > last_tick.last_price: return 1
                    if lp < last_tick.last_price: return -1
                return state.get("last_trade_sign", 0)

            # Normal (sane) book: use the standard quote test
            tol = state.get("tick_size", 0.0) # Using 0.0 as tick size is not tracked
            if lp >= cur_ask_px - tol:
                return 1
            if lp <= cur_bid_px + tol:
                return -1

            # Inside the spread: use mid + L1 outflow as tie-breakers
            mid = (cur_bid_px + cur_ask_px) / 2.0
            if cur_ask_px == prev_ask_px and cur_bid_px == prev_bid_px:
                if prev_ask_qty and cur_ask_qty < prev_ask_qty:
                    return 1
                if prev_bid_qty and cur_bid_qty < prev_bid_qty:
                    return -1

            if lp > mid:
                return 1
            if lp < mid:
                return -1

        # Fallback for missing quotes: use the tick rule
        if last_tick and last_tick.last_price is not None:
            if lp > last_tick.last_price:
                return 1
            if lp < last_tick.last_price:
                return -1

        # Last resort: carry forward previous sign
        return state.get("last_trade_sign", 0)

    def enrich_tick(self, tick: TickData, data_window: deque) -> EnrichedTick:
        """
        Calculates enrichment features for a single tick.
        """
        instrument_token = tick.instrument_token
        state = self._get_instrument_state(instrument_token)
        last_tick = state["last_tick"]

        # --- 1. Calculate TickVolume ---
        tick_volume = 0
        if last_tick and tick.volume_traded is not None and last_tick.volume_traded is not None:
            tick_volume = tick.volume_traded - last_tick.volume_traded
            if tick_volume < 0:
                tick_volume = 0

        # --- 2. Determine TradeSign (using new robust classifier) ---
        trade_sign = self._classify_trade_sign(tick, state)

        # --- 3. Calculate IsLargeTrade ---
        is_large_trade = False
        if tick_volume > 0:
            threshold = state.get("large_trade_threshold", float('inf'))
            # Using >= as suggested for "at or above" threshold
            if tick_volume >= threshold:
                is_large_trade = True

        # --- 4. Detect Buy/Sell Absorption (Iceberg Orders) ---
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

        # --- Assemble the EnrichedTick ---
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

        # --- 5. Update state for the next tick (FIXED: Independent updates) ---
        state["last_tick"] = tick
        state["last_trade_sign"] = trade_sign # Store for carry-forward
        if tick.depth and tick.depth.buy:
            state["last_best_bid_price"] = tick.depth.buy[0].price
            state["last_best_bid_qty"] = tick.depth.buy[0].quantity
        if tick.depth and tick.depth.sell:
            state["last_best_ask_price"] = tick.depth.sell[0].price
            state["last_best_ask_qty"] = tick.depth.sell[0].quantity

        return enriched_tick
