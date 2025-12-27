# analytics/selector/run.py

import asyncio
import os
import asyncpg
from dotenv import load_dotenv

from common.parameters import REALTIME_INSTRUMENTS
from analytics.selector.phase_classifier import classify_phase
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
                # 1. Fetch historical daily data
                candles = adapter.fetch_daily_candles(token, days=60)
                if not candles: continue

                # 2. Classify phase
                phase = classify_phase(candles)

                # 3. Update the SEPARATE table
                await conn.execute("""
                                   INSERT INTO public.stock_selections (symbol, phase, last_updated)
                                   VALUES ($1, $2, NOW()) ON CONFLICT (symbol) DO
                                   UPDATE
                                       SET phase = EXCLUDED.phase, last_updated = NOW();
                                   """, symbol, phase.value)

                print(f"  → {symbol}: {phase.value}")

            except Exception as e:
                print(f"  ❌ Error for {symbol}: {e}")

    finally:
        await conn.close()
        print("📡 Analysis complete. DB connection closed.")


if __name__ == "__main__":
    asyncio.run(run_selection())