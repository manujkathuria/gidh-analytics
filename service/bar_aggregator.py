# service/bar_aggregator.py

import asyncio
from collections import deque
from datetime import timedelta, datetime
from typing import Dict, Deque, Optional, List, Tuple

from service.logger import log
from service.models import EnrichedTick, BarData
from service.divergence import PatternDetector

# --- Configuration ---
INDICATOR_PERIOD = 14
BAR_INTERVALS_MINUTES = [1, 3, 5, 10, 15]
BAR_INTERVALS = [timedelta(minutes=m) for m in BAR_INTERVALS_MINUTES]


class BarAggregator:
    def __init__(self, stock_name: str, instrument_token: int, interval: timedelta):
        self.stock_name = stock_name
        self.instrument_token = instrument_token
        self.interval = interval
        self.interval_str = f"{int(interval.total_seconds() / 60)}m"

        self.building_bar: Optional[BarData] = None
        self.bar_total_price_volume: float = 0.0

        self.bar_history: Deque[BarData] = deque(maxlen=200)
        self.delta_history_5m: Deque[int] = deque(maxlen=int(5 / (interval.total_seconds() / 60)))
        self.delta_history_10m: Deque[int] = deque(maxlen=int(10 / (interval.total_seconds() / 60)))
        self.delta_history_30m: Deque[int] = deque(maxlen=int(30 / (interval.total_seconds() / 60)))

        # --- State for RSI ---
        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0
        self.is_rsi_initialized: bool = False

        # --- State for MFI ---
        self.money_flow_history: Deque[Tuple[float, float]] = deque(maxlen=INDICATOR_PERIOD)

        self.pattern_detector = PatternDetector()

    def add_tick(self, tick: EnrichedTick) -> Optional[BarData]:
        if not tick.last_price:
            return None

        bar_timestamp = tick.timestamp.replace(second=0, microsecond=0)
        minute_val = (bar_timestamp.minute // (self.interval.seconds // 60)) * (self.interval.seconds // 60)
        bar_timestamp = bar_timestamp.replace(minute=minute_val)

        completed_bar = None
        if not self.building_bar or self.building_bar.timestamp != bar_timestamp:
            if self.building_bar:
                completed_bar = self._finalize_bar()
            self._start_new_bar(bar_timestamp, tick)

        self._update_bar_data(tick)
        return completed_bar

    def _start_new_bar(self, bar_timestamp: datetime, tick: EnrichedTick):
        self.building_bar = BarData(
            timestamp=bar_timestamp, stock_name=self.stock_name, instrument_token=self.instrument_token,
            interval=self.interval_str, open=tick.last_price, high=tick.last_price, low=tick.last_price,
            close=tick.last_price, volume=0, bar_vwap=0.0, session_vwap=tick.average_traded_price,
            bar_count=len(self.bar_history) + 1, raw_scores={}
        )
        self.bar_total_price_volume = 0.0
        self._recalculate_bar_features()  # Calculate initial features for the new bar

    def _update_bar_data(self, tick: EnrichedTick):
        bar = self.building_bar
        if tick.last_price > bar.high: bar.high = tick.last_price
        if tick.last_price < bar.low: bar.low = tick.last_price
        bar.close = tick.last_price
        bar.session_vwap = tick.average_traded_price

        if tick.tick_volume > 0:
            bar.volume += tick.tick_volume
            self.bar_total_price_volume += tick.last_price * tick.tick_volume
            if bar.volume > 0: bar.bar_vwap = self.bar_total_price_volume / bar.volume

            scores = bar.raw_scores
            scores['bar_delta'] = scores.get('bar_delta', 0) + tick.tick_volume * tick.trade_sign
            if tick.is_large_trade:
                if tick.trade_sign == 1:
                    scores['large_buy_volume'] = scores.get('large_buy_volume', 0) + tick.tick_volume
                else:
                    scores['large_sell_volume'] = scores.get('large_sell_volume', 0) + tick.tick_volume
            if tick.is_buy_absorption: scores['passive_buy_volume'] = scores.get('passive_buy_volume',
                                                                                 0) + tick.tick_volume
            if tick.is_sell_absorption: scores['passive_sell_volume'] = scores.get('passive_sell_volume',
                                                                                   0) + tick.tick_volume

        self._recalculate_bar_features()

    def _finalize_bar(self) -> BarData:
        self._recalculate_bar_features()  # Final calculation before storing
        final_bar = self.building_bar

        # --- Update State After Bar is Complete ---
        # 1. Update RSI state
        prev_close = self.bar_history[-1].close if self.bar_history else final_bar.open
        change = final_bar.close - prev_close
        gain = change if change > 0 else 0
        loss = -change if change < 0 else 0
        if not self.is_rsi_initialized:
            # Simple average for the first period
            if len(self.bar_history) < INDICATOR_PERIOD:
                self.avg_gain = (self.avg_gain * len(self.bar_history) + gain) / (len(self.bar_history) + 1)
                self.avg_loss = (self.avg_loss * len(self.bar_history) + loss) / (len(self.bar_history) + 1)
            if len(self.bar_history) == INDICATOR_PERIOD - 1:
                self.is_rsi_initialized = True
        else:  # Smoothed average after initialization
            self.avg_gain = (self.avg_gain * (INDICATOR_PERIOD - 1) + gain) / INDICATOR_PERIOD
            self.avg_loss = (self.avg_loss * (INDICATOR_PERIOD - 1) + loss) / INDICATOR_PERIOD

        # 2. Update MFI history
        if self.bar_history:
            tp = (final_bar.high + final_bar.low + final_bar.close) / 3
            prev_tp = (self.bar_history[-1].high + self.bar_history[-1].low + self.bar_history[-1].close) / 3
            self.money_flow_history.append((tp * final_bar.volume, 1 if tp > prev_tp else -1))

        # 3. Update CVD history
        self.delta_history_5m.append(final_bar.raw_scores.get('bar_delta', 0))
        self.delta_history_10m.append(final_bar.raw_scores.get('bar_delta', 0))
        self.delta_history_30m.append(final_bar.raw_scores.get('bar_delta', 0))

        self.bar_history.append(final_bar)
        self.building_bar = None
        return final_bar

    def _recalculate_bar_features(self):
        if not self.building_bar: return
        bar, scores = self.building_bar, self.building_bar.raw_scores

        prev_bar = self.bar_history[-1] if self.bar_history else None
        prev_close = prev_bar.close if prev_bar else bar.open
        prev_obv = prev_bar.raw_scores.get('obv', 0) if prev_bar else 0
        prev_lvc_delta = prev_bar.raw_scores.get('lvc_delta', 0) if prev_bar else 0

        scores['cvd_30m'] = sum(self.delta_history_30m) + scores.get('bar_delta', 0)
        scores['rsi'] = self._calculate_rsi(bar.close, prev_close)
        scores['mfi'] = self._calculate_mfi(bar, prev_bar)
        scores['obv'] = self._calculate_obv(bar.close, prev_close, bar.volume, prev_obv)
        scores['lvc_delta'] = prev_lvc_delta + scores.get('large_buy_volume', 0) - scores.get('large_sell_volume', 0)

        bar_range = bar.high - bar.low
        scores['clv'] = ((bar.close - bar.low) - (bar.high - bar.close)) / bar_range if bar_range > 0 else 0.0
        scores['divergence'] = self.pattern_detector.calculate_scores(bar, self.bar_history)

    def _calculate_rsi(self, current_close: float, prev_close: float) -> float:
        change = current_close - prev_close
        gain = change if change > 0 else 0
        loss = -change if change < 0 else 0

        # Calculate using the state of the *last completed bar*
        current_avg_gain = (self.avg_gain * (
                    INDICATOR_PERIOD - 1) + gain) / INDICATOR_PERIOD if self.is_rsi_initialized else (
                                                                                                                 self.avg_gain * len(
                                                                                                             self.bar_history) + gain) / (
                                                                                                                 len(self.bar_history) + 1)
        current_avg_loss = (self.avg_loss * (
                    INDICATOR_PERIOD - 1) + loss) / INDICATOR_PERIOD if self.is_rsi_initialized else (
                                                                                                                 self.avg_loss * len(
                                                                                                             self.bar_history) + loss) / (
                                                                                                                 len(self.bar_history) + 1)

        if current_avg_loss == 0: return 100.0
        rs = current_avg_gain / current_avg_loss
        return 100 - (100 / (1 + rs)) if (1 + rs) != 0 else 100.0

    def _calculate_mfi(self, current_bar: BarData, prev_bar: Optional[BarData]) -> float:
        if not prev_bar: return 50.0

        tp = (current_bar.high + current_bar.low + current_bar.close) / 3
        prev_tp = (prev_bar.high + prev_bar.low + prev_bar.close) / 3

        # Create a temporary history for calculation
        temp_mf_history = list(self.money_flow_history)
        temp_mf_history.append((tp * current_bar.volume, 1 if tp > prev_tp else -1))
        if len(temp_mf_history) > INDICATOR_PERIOD:
            temp_mf_history.pop(0)

        pos_flow = sum(flow for flow, sign in temp_mf_history if sign == 1)
        neg_flow = sum(flow for flow, sign in temp_mf_history if sign == -1)

        if neg_flow == 0: return 100.0
        mf_ratio = pos_flow / neg_flow
        return 100 - (100 / (1 + mf_ratio))

    def _calculate_obv(self, current_close: float, prev_close: float, volume: int, prev_obv: int) -> int:
        if current_close > prev_close: return prev_obv + volume
        if current_close < prev_close: return prev_obv - volume
        return prev_obv


class BarAggregatorProcessor:
    def __init__(self):
        self.aggregators: Dict[str, BarAggregator] = {}

    def process_tick(self, tick: EnrichedTick) -> List[BarData]:
        updated_bars = []
        for interval in BAR_INTERVALS:
            agg_key = f"{tick.stock_name}-{int(interval.total_seconds())}"
            if agg_key not in self.aggregators:
                log.info(f"Creating new bar aggregator for {tick.stock_name} at {interval}.")
                self.aggregators[agg_key] = BarAggregator(
                    tick.stock_name, tick.instrument_token, interval
                )

            agg = self.aggregators[agg_key]
            completed_bar = agg.add_tick(tick)
            if completed_bar:
                updated_bars.append(completed_bar)
            if agg.building_bar:
                updated_bars.append(agg.building_bar)
        return updated_bars