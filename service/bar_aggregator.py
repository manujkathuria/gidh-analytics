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
        self.bar_count: int = 0

        self.bar_history: Deque[BarData] = deque(maxlen=200)
        self.delta_history_5m: Deque[int] = deque(maxlen=int(5 / (interval.total_seconds() / 60)))
        self.delta_history_10m: Deque[int] = deque(maxlen=int(10 / (interval.total_seconds() / 60)))
        self.delta_history_30m: Deque[int] = deque(maxlen=int(30 / (interval.total_seconds() / 60)))

        self.avg_gain: float = 0.0
        self.avg_loss: float = 0.0

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

        self._update_bar(tick)
        return completed_bar

    def _start_new_bar(self, bar_timestamp: datetime, tick: EnrichedTick):
        self.bar_count += 1
        self.building_bar = BarData(
            timestamp=bar_timestamp,
            stock_name=self.stock_name,
            instrument_token=self.instrument_token,
            interval=self.interval_str,
            open=tick.last_price,
            high=tick.last_price,
            low=tick.last_price,
            close=tick.last_price,
            volume=0,
            bar_vwap=0.0,
            session_vwap=tick.average_traded_price,
            bar_count=self.bar_count
        )
        self.bar_total_price_volume = 0.0

        last_rsi, last_obv, last_mfi, last_lvc_delta = 50.0, 0, 50.0, 0
        if self.bar_history:
            last_scores = self.bar_history[-1].raw_scores
            last_rsi = last_scores.get('rsi', 50.0)
            last_obv = last_scores.get('obv', 0)
            last_mfi = last_scores.get('mfi', 50.0)
            last_lvc_delta = last_scores.get('lvc_delta', 0)

        self.building_bar.raw_scores = {
            'bar_delta': 0,
            'large_buy_volume': 0,
            'large_sell_volume': 0,
            'passive_buy_volume': 0,
            'passive_sell_volume': 0,
            'rsi': last_rsi,
            'obv': last_obv,
            'mfi': last_mfi,
            'lvc_delta': last_lvc_delta,
            'divergence': {}
        }

    def _update_bar(self, tick: EnrichedTick):
        bar = self.building_bar
        if tick.last_price > bar.high: bar.high = tick.last_price
        if tick.last_price < bar.low: bar.low = tick.last_price
        bar.close = tick.last_price
        bar.session_vwap = tick.average_traded_price

        if tick.tick_volume > 0:
            bar.volume += tick.tick_volume
            self.bar_total_price_volume += tick.last_price * tick.tick_volume
            if bar.volume > 0:
                bar.bar_vwap = self.bar_total_price_volume / bar.volume

            scores = bar.raw_scores
            scores['bar_delta'] += tick.tick_volume * tick.trade_sign
            if tick.is_large_trade:
                if tick.trade_sign == 1:
                    scores['large_buy_volume'] += tick.tick_volume
                else:
                    scores['large_sell_volume'] += tick.tick_volume
            if tick.is_buy_absorption:
                scores['passive_buy_volume'] += tick.tick_volume
            if tick.is_sell_absorption:
                scores['passive_sell_volume'] += tick.tick_volume

        # ** FIX: Recalculate features on every tick for real-time updates **
        self._recalculate_bar_features()

    def _finalize_bar(self) -> BarData:
        # ** FIX: Recalculate features one last time before finalizing **
        self._recalculate_bar_features()
        final_bar = self.building_bar

        # Update historical delta for next bar's calculation
        self.delta_history_5m.append(final_bar.raw_scores.get('bar_delta', 0))
        self.delta_history_10m.append(final_bar.raw_scores.get('bar_delta', 0))
        self.delta_history_30m.append(final_bar.raw_scores.get('bar_delta', 0))

        # Add the completed bar to history
        self.bar_history.append(final_bar)
        self.building_bar = None  # Clear the building bar
        return final_bar

    def _recalculate_bar_features(self):
        """
        Calculates all advanced features for the current building_bar.
        This is called on every tick to provide real-time updates.
        """
        if not self.building_bar:
            return

        bar = self.building_bar
        scores = bar.raw_scores

        # --- Cumulative Volume Delta (CVD) ---
        # Sum of historical delta + the delta of the current, building bar
        scores['cvd_5m'] = sum(self.delta_history_5m) + scores.get('bar_delta', 0)
        scores['cvd_10m'] = sum(self.delta_history_10m) + scores.get('bar_delta', 0)
        scores['cvd_30m'] = sum(self.delta_history_30m) + scores.get('bar_delta', 0)

        # --- RSI, OBV, MFI ---
        prev_bar = self.bar_history[-1] if self.bar_history else None
        prev_close = prev_bar.close if prev_bar else bar.open
        prev_obv = prev_bar.raw_scores.get('obv', 0) if prev_bar else 0
        scores['rsi'] = self._calculate_rsi(bar.close, prev_close)
        scores['obv'] = self._calculate_obv(bar.close, prev_close, bar.volume, prev_obv)
        scores['mfi'] = self._calculate_mfi(bar.high, bar.low, bar.close, bar.volume)

        # --- Large Volume Commitment (LVC) Delta ---
        prev_lvc_delta = prev_bar.raw_scores.get('lvc_delta', 0) if prev_bar else 0
        net_large_volume = scores.get('large_buy_volume', 0) - scores.get('large_sell_volume', 0)
        scores['lvc_delta'] = prev_lvc_delta + net_large_volume

        # --- Close Location Value (CLV) ---
        bar_range = bar.high - bar.low
        if bar_range > 0:
            scores['clv'] = ((bar.close - bar.low) - (bar.high - bar.close)) / bar_range
        else:
            scores['clv'] = 0.0

        # --- Divergence Scores ---
        scores["divergence"] = self.pattern_detector.calculate_scores(bar, self.bar_history)

    def _calculate_rsi(self, current_close: float, prev_close: float) -> float:
        change = current_close - prev_close
        gain = change if change > 0 else 0
        loss = -change if change < 0 else 0

        # Use smoothed moving average for RSI
        if len(self.bar_history) < INDICATOR_PERIOD - 1:
            # Use simple moving average for the initial period
            self.avg_gain = ((self.avg_gain * (
                        self.bar_count - 1)) + gain) / self.bar_count if self.bar_count > 0 else 0
            self.avg_loss = ((self.avg_loss * (
                        self.bar_count - 1)) + loss) / self.bar_count if self.bar_count > 0 else 0.0001
        else:
            self.avg_gain = ((self.avg_gain * (INDICATOR_PERIOD - 1)) + gain) / INDICATOR_PERIOD
            self.avg_loss = ((self.avg_loss * (INDICATOR_PERIOD - 1)) + loss) / INDICATOR_PERIOD

        if self.avg_loss == 0:
            return 100.0

        rs = self.avg_gain / self.avg_loss
        return 100 - (100 / (1 + rs))

    def _calculate_obv(self, current_close: float, prev_close: float, volume: int, prev_obv: int) -> int:
        if current_close > prev_close: return prev_obv + volume
        if current_close < prev_close: return prev_obv - volume
        return prev_obv

    def _calculate_mfi(self, high: float, low: float, close: float, volume: int) -> float:
        if not self.bar_history:
            return 50.0

        typical_price = (high + low + close) / 3
        # Get the typical price from the last *completed* bar in history
        prev_typical_price = (self.bar_history[-1].high + self.bar_history[-1].low + self.bar_history[-1].close) / 3

        raw_money_flow = typical_price * volume

        # We need to handle the money flow history carefully for the building bar
        current_flow = (raw_money_flow, 1 if typical_price > prev_typical_price else -1)

        # Combine historical flows with the current, temporary flow
        combined_flows = list(self.money_flow_history)
        combined_flows.append(current_flow)

        # Ensure we don't exceed the period
        if len(combined_flows) > INDICATOR_PERIOD:
            combined_flows = combined_flows[-INDICATOR_PERIOD:]

        positive_flow = sum(flow for flow, sign in combined_flows if sign == 1)
        negative_flow = sum(flow for flow, sign in combined_flows if sign == -1)

        if negative_flow == 0:
            return 100.0

        money_flow_ratio = positive_flow / negative_flow
        return 100 - (100 / (1 + money_flow_ratio))


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
                # Add the finalized bar to the list to be written
                updated_bars.append(completed_bar)

            # Always add the currently building (and now real-time updated) bar
            if agg.building_bar:
                updated_bars.append(agg.building_bar)

        return updated_bars