from collections import deque
from typing import Dict
import numpy as np

from service.models import TickData, EnrichedTick
from service.logger import log


class FeatureEnricher:
    """
    A stateful class that enriches raw TickData with calculated features.
    """

    def __init__(self):
        # Dictionaries to store the last known state for each instrument
        self.last_tick_map: Dict[int, TickData] = {}

    def enrich_tick(self, tick: TickData, data_window: deque) -> EnrichedTick:
        """
        Calculates enrichment features for a single tick.

        Args:
            tick: The raw TickData object to enrich.
            data_window: The pipeline's global data window for calculating statistics.

        Returns:
            An EnrichedTick object with new features calculated.
        """
        instrument_token = tick.instrument_token
        last_tick = self.last_tick_map.get(instrument_token)

        # --- 1. Calculate TickVolume ---
        tick_volume = 0
        if last_tick and tick.volume_traded is not None and last_tick.volume_traded is not None:
            # The true volume is the change in cumulative volume
            tick_volume = tick.volume_traded - last_tick.volume_traded
            if tick_volume < 0:  # Handle potential data resets or errors
                tick_volume = 0

        # --- 2. Calculate TradeSign ---
        trade_sign = 0
        if last_tick and tick.last_price is not None and last_tick.last_price is not None:
            if tick.last_price > last_tick.last_price:
                trade_sign = 1  # Uptick
            elif tick.last_price < last_tick.last_price:
                trade_sign = -1  # Downtick

        # --- 3. Calculate IsLargeTrade ---
        is_large_trade = False
        if tick_volume > 0 and len(data_window) > 10:  # Ensure we have enough data
            # Get recent trade volumes for this specific instrument from the window
            recent_volumes = [
                et.tick_volume for ts, et in data_window
                if et.instrument_token == instrument_token and et.tick_volume > 0
            ]

            if len(recent_volumes) > 10:
                # Calculate the dynamic threshold
                avg_vol = np.mean(recent_volumes)
                std_dev_vol = np.std(recent_volumes)
                # A trade is "large" if it's more than 2 standard deviations above the mean
                large_trade_threshold = avg_vol + (2 * std_dev_vol)

                if tick_volume > large_trade_threshold:
                    is_large_trade = True

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
            is_large_trade=is_large_trade
        )

        # Update the last tick map for the next iteration
        self.last_tick_map[instrument_token] = tick

        return enriched_tick
