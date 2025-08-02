# analytics/db_connector.py

import os
import pandas as pd
from dotenv import load_dotenv
import asyncpg
import asyncio
from datetime import datetime  # Import the datetime class

# Load environment variables from the .env file in the parent directory
load_dotenv(dotenv_path='../.env')


async def get_db_connection():
    """Establishes an asynchronous connection to the PostgreSQL database."""
    try:
        conn = await asyncpg.connect(
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("Database connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


async def fetch_features_data(stock_name: str, interval: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetches data asynchronously from the grafana_features_view for a specific stock and date range.

    Args:
        stock_name: The name of the stock (e.g., 'BOSCHLTD').
        interval: The bar interval (e.g., '5m').
        start_date: The start date in 'YYYY-MM-DD' format.
        end_date: The end date in 'YYYY-MM-DD' format.

    Returns:
        A pandas DataFrame containing the feature data, or an empty DataFrame on error.
    """
    conn = await get_db_connection()
    if conn is None:
        return pd.DataFrame()

    query = """
        SELECT
            timestamp,
            close,
            volume,
            cvd_30m,
            obv,
            mfi,
            rsi,
            lvc_delta,
            -- Tier 1 Divergences
            div_price_lvc,
            div_price_cvd,
            div_price_obv,
            div_price_rsi,
            div_price_mfi,
            -- Tier 2 Divergences
            div_lvc_cvd,
            div_lvc_obv,
            div_lvc_rsi,
            div_lvc_mfi
        FROM public.grafana_features_view
        WHERE
            stock_name = $1 AND
            interval = $2 AND
            timestamp >= $3 AND
            timestamp <= $4
        ORDER BY timestamp;
    """

    try:
        print(f"Fetching data for {stock_name} ({interval}) from {start_date} to {end_date}...")

        # FIX: Convert date strings to datetime objects before passing to the query
        start_datetime = datetime.strptime(f'{start_date} 00:00:00', '%Y-%m-%d %H:%M:%S')
        end_datetime = datetime.strptime(f'{end_date} 23:59:59', '%Y-%m-%d %H:%M:%S')

        records = await conn.fetch(
            query,
            stock_name,
            interval,
            start_datetime,  # Pass the datetime object
            end_datetime  # Pass the datetime object
        )

        if not records:
            print("No data found for the specified criteria.")
            return pd.DataFrame()

        df = pd.DataFrame(records, columns=records[0].keys())
        print(f"Successfully fetched {len(df)} rows.")
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            await conn.close()
            print("Database connection closed.")


async def main():
    """Main function to run the example data fetch."""
    data = await fetch_features_data(
        stock_name='BOSCHLTD',
        interval='5m',
        start_date='2025-08-01',
        end_date='2025-08-01'
    )
    if not data.empty:
        print("\n--- Sample Data ---")
        print(data.head())
        print("\n--- Data Info ---")
        data.info()


if __name__ == '__main__':
    asyncio.run(main())
