# analytics/calculate_thresholds_eod.py

import asyncio
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import asyncpg
from datetime import datetime, timedelta

# Load environment variables from the project's .env file
load_dotenv(dotenv_path='../.env')

# --- Configuration ---
# The cutoff for the Modified Z-score. A value of 3.5 is standard in statistics for identifying outliers.
MODIFIED_Z_SCORE_CUTOFF = 3.5

async def get_db_connection():
    """Establishes an async connection to the PostgreSQL database."""
    try:
        return await asyncpg.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
    except Exception as e:
        print(f"❌ Error connecting to the database: {e}")
        return None

async def fetch_historical_volumes(conn, days_lookback=7):
    """Fetches the last N days of tick data needed for volume analysis."""
    print(f"Fetching raw volume data for the last {days_lookback} days...")
    query = """
        SELECT stock_name, timestamp, volume_traded
        FROM public.live_ticks
        WHERE timestamp >= $1
        ORDER BY stock_name, timestamp ASC;
    """
    start_date = datetime.now() - timedelta(days=days_lookback)
    records = await conn.fetch(query, start_date)
    if not records:
        print("No data found for the specified period.")
        return pd.DataFrame()
    print(f"✅ Successfully fetched {len(records)} rows.")
    return pd.DataFrame(records, columns=['stock_name', 'timestamp', 'volume_traded'])

def calculate_tick_volumes(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates individual trade volumes from the cumulative `volume_traded` field."""
    print("Calculating individual trade volumes...")
    df = df.sort_values(by=['stock_name', 'timestamp'])
    # Calculate the difference from the previous tick's volume, grouped by stock
    df['tick_volume'] = df.groupby('stock_name')['volume_traded'].diff().fillna(0)
    # Correct for daily volume resets where the difference would be negative
    df.loc[df['tick_volume'] < 0, 'tick_volume'] = 0
    print("✅ Trade volumes calculated.")
    # Return only actual trades
    return df[df['tick_volume'] > 0].copy()

def calculate_thresholds_with_mad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the large trade threshold using the robust Median Absolute Deviation (MAD) method.
    """
    print("Calculating thresholds using Median Absolute Deviation...")
    all_thresholds = []

    for stock_name, group in df.groupby('stock_name'):
        volumes = group['tick_volume']
        if len(volumes) < 50:  # Skip if there's not enough data
            continue

        # 1. Calculate Median and MAD
        median_vol = np.median(volumes)
        mad = np.median(np.abs(volumes - median_vol))

        if mad == 0: # Avoid division by zero if all trades are the same size
            continue

        # 2. Calculate Modified Z-Score for each trade
        # The 0.6745 constant is a scaling factor that makes the result comparable to a standard Z-score.
        modified_z_score = 0.6745 * (volumes - median_vol) / mad

        # 3. Identify outliers
        outliers = volumes[modified_z_score > MODIFIED_Z_SCORE_CUTOFF]

        # 4. The threshold is the smallest of these outlier trades
        if not outliers.empty:
            threshold = int(outliers.min())
            all_thresholds.append({'stock_name': stock_name, 'large_trade_threshold': threshold})

    print("✅ Thresholds calculated.")
    return pd.DataFrame(all_thresholds)


async def upsert_thresholds(conn, thresholds_df: pd.DataFrame):
    """Saves the calculated thresholds back to the database."""
    print(f"Upserting {len(thresholds_df)} thresholds into the database...")
    query = """
        INSERT INTO public.instrument_thresholds (stock_name, large_trade_threshold, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (stock_name) DO UPDATE
        SET
            large_trade_threshold = EXCLUDED.large_trade_threshold,
            updated_at = NOW();
    """
    try:
        await conn.executemany(query, thresholds_df.to_records(index=False))
        print("✅ All thresholds successfully updated in the database.")
    except Exception as e:
        print(f"❌ Database upsert failed: {e}")


async def main():
    """Main function to run the EOD threshold calculation."""
    conn = await get_db_connection()
    if not conn:
        return

    try:
        volume_data = await fetch_historical_volumes(conn, days_lookback=7)
        if volume_data.empty:
            return

        trade_volumes = calculate_tick_volumes(volume_data)
        thresholds = calculate_thresholds_with_mad(trade_volumes)

        if thresholds.empty:
            print("No new thresholds were calculated. Exiting.")
            return

        print("\n--- Calculated Thresholds ---")
        print(thresholds)
        print("---------------------------\n")

        await upsert_thresholds(conn, thresholds)

    finally:
        await conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    # Ensure the script can be run standalone
    asyncio.run(main())