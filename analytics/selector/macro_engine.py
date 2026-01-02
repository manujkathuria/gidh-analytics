import pandas as pd
import numpy as np


class MacroEngine:
    def __init__(self):
        pass

    def calculate_metrics(self, candles):
        """
        candles: list of dicts from Kite historical API
        returns: dict of computed metrics
        """

        if not candles or len(candles) < 30:
            return None

        df = pd.DataFrame(candles)

        # Ensure correct columns
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        # ----------------------------
        # PRICE & VOLUME BASICS
        # ----------------------------
        close_today = df["close"].iloc[-1]
        volume_today = df["volume"].iloc[-1]

        avg_vol_1w = df["volume"].tail(5).mean()
        avg_vol_1m = df["volume"].tail(20).mean()

        # ----------------------------
        # VOLUME STATE
        # ----------------------------
        if volume_today > avg_vol_1w * 1.2:
            vol_state = "higher"
        elif volume_today < avg_vol_1w * 0.8:
            vol_state = "lower"
        else:
            vol_state = "normal"

        # ----------------------------
        # MOMENTUM (DESCRIPTIVE)
        # ----------------------------
        price_5d_ago = df["close"].iloc[-6]
        price_20d_ago = df["close"].iloc[-21]

        price_change_5d = ((close_today - price_5d_ago) / price_5d_ago) * 100
        price_change_20d = ((close_today - price_20d_ago) / price_20d_ago) * 100

        # ----------------------------
        # PRICE POSITION (RANGE BASED)
        # ----------------------------
        recent_high = df["close"].tail(20).max()
        recent_low = df["close"].tail(20).min()

        if close_today >= recent_high * 0.97:
            price_position = "upper_range"
        elif close_today <= recent_low * 1.03:
            price_position = "lower_range"
        else:
            price_position = "mid_range"

        # ----------------------------
        # TREND BIAS (DESCRIPTIVE)
        # ----------------------------
        if price_change_5d > 0 and price_change_20d > 0:
            trend_bias = "up"
        elif price_change_5d < 0 and price_change_20d < 0:
            trend_bias = "down"
        else:
            trend_bias = "sideways"

        return {
            "close": round(close_today, 2),
            "volume": int(volume_today),
            "avg_vol_1w": int(avg_vol_1w),
            "avg_vol_1m": int(avg_vol_1m),
            "volume_state": vol_state,
            "price_change_5d": round(price_change_5d, 2),
            "price_change_20d": round(price_change_20d, 2),
            "price_position": price_position,
            "trend_bias": trend_bias
        }
