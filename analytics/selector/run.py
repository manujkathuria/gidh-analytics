# analytics/selector/run.py

import asyncio
import os
import asyncpg
from dotenv import load_dotenv

from common.parameters import REALTIME_INSTRUMENTS
from analytics.selector.macro_classifier import classify_phase, classify_trend
from analytics.selector.kite_adapter import KiteHistoricalAdapter

# Load .env relative to this subpackage
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.env'))
load_dotenv(dotenv_path=dotenv_path)


async def run_selection():
    """
    STANDALONE ANALYSIS SCRIPT:
    Fetches data, classifies, and updates the separate table.
    """
    adapter = KiteHistoricalAdapter()

    # Connect directly using env variables
    try:
        conn = await asyncpg.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("✅ Analysis script connected to DB.")

        print(f"Analyzing {len(REALTIME_INSTRUMENTS)} instruments...")

        for symbol, token in REALTIME_INSTRUMENTS.items():
            try:
                # 1. Fetch historical data (60 trading days)
                lookback_days = 60
                fetch_days = 90  # buffer for weekends & holidays

                candles = adapter.fetch_daily_candles(token, days=fetch_days)
                candles = candles[-lookback_days:]

                if not candles or len(candles) < 60:
                    continue

                # 2. Classify market state
                phase = classify_phase(candles)
                trend = classify_trend(candles)

                # 3. Store results
                await conn.execute(
                    """
                    INSERT INTO stock_state_history (symbol, phase, trend, recorded_at)
                    VALUES ($1, $2, $3, NOW())
                    """,
                    symbol,
                    phase.value,
                    trend.value
                )

                print(f"{symbol} → Phase: {phase.value}, Trend: {trend.value}")

            except Exception as e:
                print(f"❌ Error processing {symbol}: {e}")
    finally:
        await conn.close()
        print("📡 Analysis complete. DB connection closed.")


if __name__ == "__main__":
    asyncio.run(run_selection())
