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
                # Add a default threshold. It will be overwritten by load_thresholds.
                "large_trade_threshold": float('inf'),
            }
        return self.instrument_states[instrument_token]

    def enrich_tick(self, tick: TickData, data_window: deque) -> EnrichedTick:
        """
        Calculates enrichment features for a single tick, translating the Go logic.

        Args:
            tick: The raw TickData object to enrich.
            data_window: The pipeline's global data window for calculating statistics.

        Returns:
            An EnrichedTick object with new features calculated.
        """
        instrument_token = tick.instrument_token
        state = self._get_instrument_state(instrument_token)
        last_tick = state["last_tick"]

        # --- 1. Calculate TickVolume ---
        tick_volume = 0
        if last_tick and tick.volume_traded is not None and last_tick.volume_traded is not None:
            tick_volume = tick.volume_traded - last_tick.volume_traded
            if tick_volume < 0: tick_volume = 0  # Handle resets

        # --- 2. Determine TradeSign (Aggressor) using the more robust logic ---
        trade_sign = 0
        if tick.last_price is not None:
            # Primary logic: check against previous best bid/ask
            if state["last_best_ask_price"] > 0 and tick.last_price >= state["last_best_ask_price"]:
                trade_sign = 1  # Buy aggressor
            elif state["last_best_bid_price"] > 0 and tick.last_price <= state["last_best_bid_price"]:
                trade_sign = -1  # Sell aggressor
            # Fallback logic: compare with last price
            elif last_tick and last_tick.last_price is not None:
                if tick.last_price > last_tick.last_price:
                    trade_sign = 1
                elif tick.last_price < last_tick.last_price:
                    trade_sign = -1

        # --- 3. Calculate IsLargeTrade (MODIFIED LOGIC) ---
        is_large_trade = False
        if tick_volume > 0:
            # Get the pre-calculated threshold from this instrument's state
            threshold = state.get("large_trade_threshold", float('inf'))
            if tick_volume > threshold:
                is_large_trade = True
        # --- (The old dynamic calculation is now removed) ---

        # --- 4. Detect Buy/Sell Absorption (Iceberg Orders) ---
        is_buy_absorption = False
        is_sell_absorption = False

        if tick.depth and tick.depth.buy and tick.depth.sell and tick_volume > 0:
            best_bid = tick.depth.buy[0]
            best_ask = tick.depth.sell[0]

            # --- Check for Sell-Side Absorption (Resistance) ---
            # If the best ask price has changed, a hidden order at the previous price is gone. Reset.
            if best_ask.price != state["last_best_ask_price"]:
                state["hidden_sell_order_refill_count"] = 0
            # Else, if a buy trade occurred at this price level...
            elif trade_sign == 1:
                # ...and the new quantity is greater than what it should have been, it implies a refill.
                if best_ask.quantity > (state["last_best_ask_qty"] - tick_volume):
                    state["hidden_sell_order_refill_count"] += 1

            # --- Check for Buy-Side Absorption (Support) ---
            # If the best bid price has changed, a hidden order at the previous price is gone. Reset.
            if best_bid.price != state["last_best_bid_price"]:
                state["hidden_buy_order_refill_count"] = 0
            # Else, if a sell trade occurred at this price level...
            elif trade_sign == -1:
                # ...and the new quantity is greater than what it should have been, it implies a refill.
                if best_bid.quantity > (state["last_best_bid_qty"] - tick_volume):
                    state["hidden_buy_order_refill_count"] += 1

            # Set the final boolean flags if the confirmation threshold is met
            if state["hidden_sell_order_refill_count"] >= ICEBERG_CONFIRMATION_THRESHOLD:
                is_sell_absorption = True

            if state["hidden_buy_order_refill_count"] >= ICEBERG_CONFIRMATION_THRESHOLD:
                is_buy_absorption = True

        # --- Assemble the EnrichedTick ---
        enriched_tick = EnrichedTick(
            # Copy original fields
            timestamp=tick.timestamp,
            instrument_token=tick.instrument_token,
            stock_name=tick.stock_name,
            last_price=tick.last_price,
            last_traded_quantity=tick.last_traded_quantity,
            average_traded_price=tick.average_traded_price,
            volume_traded=tick.volume_traded,
            total_buy_quantity=tick.total_buy_quantity,
            total_sell_quantity=tick.total_sell_quantity,
            ohlc_open=tick.ohlc_open,
            ohlc_high=tick.ohlc_high,
            ohlc_low=tick.ohlc_low,
            ohlc_close=tick.ohlc_close,
            change=tick.change,
            depth=tick.depth,

            # Add new fields
            tick_volume=tick_volume,
            trade_sign=trade_sign,
            is_large_trade=is_large_trade,
            is_buy_absorption=is_buy_absorption,
            is_sell_absorption=is_sell_absorption
        )

        # --- 5. Update state for the next tick ---
        state["last_tick"] = tick
        if tick.depth and tick.depth.buy:
            state["last_best_bid_price"] = tick.depth.buy[0].price
            state["last_best_bid_qty"] = tick.depth.buy[0].quantity
        if tick.depth and tick.depth.sell:
            state["last_best_ask_price"] = tick.depth.sell[0].price
            state["last_best_ask_qty"] = tick.depth.sell[0].quantity

        return enriched_tick