from enum import Enum
from typing import List
from common.models import Candle


class Phase(Enum):
    FALLING = "FALLING"
    RISING = "RISING"
    ACCUMULATING = "ACCUMULATING"
    SELLING = "SELLING"
    NO_PHASE = "NO_PHASE"


def classify_phase(candles: List[Candle]) -> Phase:
    if len(candles) < 30:
        return Phase.NO_PHASE

    # Priority order matters (strongest first)
    if _is_falling(candles):
        return Phase.FALLING

    if _is_rising(candles):
        return Phase.RISING

    if _is_accumulating(candles):
        return Phase.ACCUMULATING

    if _is_selling(candles):
        return Phase.SELLING

    return Phase.NO_PHASE


# -------------------------------------------------
# PHASE DETECTORS
# -------------------------------------------------

def _is_falling(candles: List[Candle]) -> bool:
    # Directional bias
    if candles[-1].close >= candles[0].close:
        return False

    recent = candles[-10:]
    older = candles[-20:-10]

    # Lower highs
    if max(c.high for c in recent) >= max(c.high for c in older):
        return False

    # Price near lower part of range
    low = min(c.low for c in candles)
    high = max(c.high for c in candles)
    if (candles[-1].close - low) / (high - low) > 0.35:
        return False

    return True


def _is_rising(candles: List[Candle]) -> bool:
    if candles[-1].close <= candles[0].close:
        return False

    recent = candles[-10:]
    older = candles[-20:-10]

    # Higher lows
    if min(c.low for c in recent) <= min(c.low for c in older):
        return False

    # Price near top of range
    low = min(c.low for c in candles)
    high = max(c.high for c in candles)
    if (candles[-1].close - low) / (high - low) < 0.65:
        return False

    return True


def _is_accumulating(candles: List[Candle]) -> bool:
    # Sideways + low volatility
    closes = [c.close for c in candles]
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]

    range_pct = (max(highs) - min(lows)) / max(closes[-1], 1e-9)

    # Flat price movement
    if abs(closes[-1] - closes[0]) > 0.02 * closes[0]:
        return False

    # Compression
    if range_pct > 0.05:
        return False

    return True


def _is_selling(candles: List[Candle]) -> bool:
    # Distribution: high volatility, loss of upward momentum
    closes = [c.close for c in candles]
    highs = [c.high for c in candles]

    volatility = (max(highs) - min(c.low for c in candles)) / closes[-1]

    # Reject highs + unstable structure
    if volatility < 0.05:
        return False

    if closes[-1] > closes[0]:
        return False

    return True
