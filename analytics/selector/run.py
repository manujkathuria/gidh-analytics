# scripts/run.py

import asyncio
import os
import asyncpg
from dotenv import load_dotenv

# Import from your existing service modules
from common.parameters import REALTIME_INSTRUMENTS
from analytics.selector.phase_classifier import classify_phase
from analytics.selector.kite_adapter import KiteHistoricalAdapter

# Load environment variables from the project's .env file
# Pattern matched from scripts/calculate_thresholds_eod.py
load_dotenv(dotenv_path='../../.env')


async def get_db_pool():
    """
    Establishes an asynchronous connection pool to the PostgreSQL database.
    Modeled after the connection logic in scripts/calculate_thresholds_eod.py.
    """
    try:
        # Using a pool is more efficient for iterating through multiple stocks
        pool = await asyncpg.create_pool(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("✅ Database connection pool established successfully.")
        return pool
    except Exception as e:
        print(f"❌ Error connecting to the database: {e}")
        return None


async def run_selection():
    """
    Main runner logic: fetches data, classifies phases, and saves to DB.
    """
    # 1. Initialize Adapter and DB Pool
    adapter = KiteHistoricalAdapter()
    db_pool = await get_db_pool()

    if not db_pool:
        return

    print(f"Starting Phase-Based Stock Selection for {len(REALTIME_INSTRUMENTS)} instruments...")

    try:
        async with db_pool.acquire() as connection:
            for symbol, token in REALTIME_INSTRUMENTS.items():
                try:
                    # 2. Fetch Historical Data (v1 requires ~60 days)
                    # adapter.fetch_daily_candles returns a List[Candle]
                    candles = adapter.fetch_daily_candles(token, days=60)

                    if not candles:
                        print(f"  ⚠️ No data fetched for {symbol}. Skipping.")
                        continue

                    # 3. Classify Phase using deterministic v1 logic
                    phase = classify_phase(candles)
                    print(f"  → {symbol}: {phase.value}")

                    # 4. Upsert into stock_selections table
                    # Pattern matched from service/db_writer.py logic
                    await connection.execute("""
                                             INSERT INTO public.stock_selections (symbol, phase, last_updated)
                                             VALUES ($1, $2, NOW()) ON CONFLICT (symbol) DO
                                             UPDATE
                                                 SET
                                                     phase = EXCLUDED.phase,
                                                 last_updated = NOW();
                                             """, symbol, phase.value)

                except Exception as e:
                    print(f"  ❌ Error processing {symbol}: {e}")

        print("✅ Stock selection process completed.")

    finally:
        await db_pool.close()
        print("Database connection pool closed.")


if __name__ == "__main__":
    # Ensure the script runs within the asyncio event loop
    asyncio.run(run_selection())