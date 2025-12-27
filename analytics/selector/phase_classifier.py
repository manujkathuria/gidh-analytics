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

    # Example: v1 FALLING Logic Implementation
    if _is_falling(candles):
        return Phase.FALLING

    # Priority order: FALLING > RISING > ACCUMULATING > SELLING
    # ... implement others ...

    return Phase.NO_PHASE


def _is_falling(candles: List[Candle]) -> bool:
    # 1. Directional Bias: Current close lower than start of window
    if candles[-1].close >= candles[0].close:
        return False

    # 2. Lower Highs / Lower Lows (Simplified v1 check)
    recent = candles[-10:]
    older = candles[-20:-10]
    if max(c.high for c in recent) >= max(c.high for c in older):
        return False

    # 3. Range Location: Price in bottom 30% of 60-day range
    full_min = min(c.low for c in candles)
    full_max = max(c.high for c in candles)
    price_range = full_max - full_min
    if candles[-1].close > (full_min + 0.3 * price_range):
        return False

    return True