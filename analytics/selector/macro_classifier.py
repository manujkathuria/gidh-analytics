from enum import Enum
from typing import List
from common.models import Candle


# -------------------------------
# ENUMS
# -------------------------------

class Phase(Enum):
    FALLING = "FALLING"
    RISING = "RISING"
    NEUTRAL = "NEUTRAL"


class Trend(Enum):
    BEARISH = "BEARISH"
    BULLISH = "BULLISH"
    NEUTRAL = "NEUTRAL"


# -------------------------------
# PHASE CLASSIFIER (MACRO)
# -------------------------------

def classify_phase(candles: List[Candle]) -> Phase:
    """
    Determines long-term market structure.
    Uses ~60 candles.
    """
    if len(candles) < 60:
        return Phase.NEUTRAL

    window = candles[-60:]

    closes = [c.close for c in window]
    highs = [c.high for c in window]
    lows  = [c.low for c in window]

    # Slope of structure
    slope = (closes[-1] - closes[0]) / len(closes)

    # Relative position in range
    total_range = max(highs) - min(lows)
    if total_range == 0:
        return Phase.NEUTRAL

    position = (closes[-1] - min(lows)) / total_range

    # Structure checks
    recent = window[-20:]
    older  = window[-40:-20]

    higher_lows = min(c.low for c in recent) > min(c.low for c in older)
    lower_highs = max(c.high for c in recent) < max(c.high for c in older)

    # Final classification
    if slope > 0 and higher_lows and position > 0.6:
        return Phase.RISING

    if slope < 0 and lower_highs and position < 0.4:
        return Phase.FALLING

    return Phase.NEUTRAL


# -------------------------------
# TREND CLASSIFIER (SHORT-TERM)
# -------------------------------

def classify_trend(candles: List[Candle]) -> Trend:
    """
    Detects short-term pressure.
    Uses ~10–12 candles.
    """

    if len(candles) < 15:
        return Trend.NEUTRAL

    recent = candles[-10:]
    older = candles[-20:-10]

    recent_high = max(c.high for c in recent)
    recent_low  = min(c.low for c in recent)

    older_high = max(c.high for c in older)
    older_low  = min(c.low for c in older)

    # Trend logic
    if recent_high > older_high and recent_low > older_low:
        return Trend.BULLISH

    if recent_high < older_high and recent_low < older_low:
        return Trend.BEARISH

    return Trend.NEUTRAL
