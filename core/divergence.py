# service/divergence.py

from collections import deque
from datetime import timedelta
from typing import Dict

from common.models import BarData

# Configuration
COMPOSITE_FEATURE_LOOKBACK_DURATION = timedelta(minutes=30)
MIN_LOOKBACK_DURATION = timedelta(minutes=5)
DIVERGENCE_MULTIPLIER = 2.0


def _calculate_divergence_score(primary_change: float, secondary_change: float) -> float:
    """Calculates a normalized divergence score between a primary and secondary metric."""
    bullish_divergence = secondary_change - (primary_change * DIVERGENCE_MULTIPLIER)
    bearish_divergence = (primary_change * DIVERGENCE_MULTIPLIER) - secondary_change

    if bullish_divergence > 0:
        return min(1.0, bullish_divergence * 10)
    elif bearish_divergence > 0:
        return -min(1.0, bearish_divergence * 10)
    return 0.0


class PatternDetector:
    def calculate_scores(self, current_bar: BarData, bar_history: deque) -> Dict[str, float]:
        scores = {}

        # Determine lookback period
        interval_str = getattr(current_bar, 'interval', '1m')
        interval_minutes = int(interval_str.replace('m', ''))
        current_lookback_duration = len(bar_history) * timedelta(minutes=interval_minutes)

        if current_lookback_duration < MIN_LOOKBACK_DURATION:
            return scores

        if current_lookback_duration > COMPOSITE_FEATURE_LOOKBACK_DURATION:
            current_lookback_duration = COMPOSITE_FEATURE_LOOKBACK_DURATION

        lookback_bars = int(current_lookback_duration.total_seconds() / (interval_minutes * 60))

        if len(bar_history) < lookback_bars:
            return scores

        start_bar = bar_history[-lookback_bars]

        # --- 1. Calculate Base Changes ---
        if start_bar.close == 0: return scores
        price_change = (current_bar.close - start_bar.close) / start_bar.close

        volume_in_window = sum(b.volume for b in list(bar_history)[-lookback_bars:])
        if volume_in_window == 0: volume_in_window = 1

        large_volume_in_window = sum(
            b.raw_scores.get('large_buy_volume', 0) + b.raw_scores.get('large_sell_volume', 0)
            for b in list(bar_history)[-lookback_bars:]
        )
        if large_volume_in_window == 0: large_volume_in_window = 1

        # --- 2. Calculate Normalized Indicator Changes (USING SMOOTHED VALUES) ---
        cvd_change = float(current_bar.raw_scores.get('cvd_5m_smoothed', 0) - start_bar.raw_scores.get('cvd_5m_smoothed', 0)) / float(
            volume_in_window)
        obv_change = float(current_bar.raw_scores.get('obv', 0) - start_bar.raw_scores.get('obv', 0)) / float(
            volume_in_window)
        lvc_change = float(
            current_bar.raw_scores.get('lvc_delta', 0) - start_bar.raw_scores.get('lvc_delta', 0)) / float(
            large_volume_in_window)
        rsi_change = float(current_bar.raw_scores.get('rsi_smoothed', 50) - start_bar.raw_scores.get('rsi_smoothed', 50)) / 100.0
        mfi_change = float(current_bar.raw_scores.get('mfi_smoothed', 50) - start_bar.raw_scores.get('mfi_smoothed', 50)) / 100.0
        clv_change = float(current_bar.raw_scores.get('clv_smoothed', 0) - start_bar.raw_scores.get('clv_smoothed', 0))


        # --- 3. Calculate All Divergence Scores ---
        # Tier 1: Price vs. Features
        scores["price_vs_lvc"] = _calculate_divergence_score(price_change, lvc_change)
        scores["price_vs_cvd"] = _calculate_divergence_score(price_change, cvd_change)
        scores["price_vs_obv"] = _calculate_divergence_score(price_change, obv_change)
        scores["price_vs_rsi"] = _calculate_divergence_score(price_change, rsi_change)
        scores["price_vs_mfi"] = _calculate_divergence_score(price_change, mfi_change)
        scores["price_vs_clv"] = _calculate_divergence_score(price_change, clv_change)


        # Tier 2: LVC vs. Features
        scores["lvc_vs_cvd"] = _calculate_divergence_score(lvc_change, cvd_change)
        scores["lvc_vs_obv"] = _calculate_divergence_score(lvc_change, obv_change)
        scores["lvc_vs_rsi"] = _calculate_divergence_score(lvc_change, rsi_change)
        scores["lvc_vs_mfi"] = _calculate_divergence_score(lvc_change, mfi_change)

        return scores