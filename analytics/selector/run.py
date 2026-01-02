import asyncio
import os
import asyncpg
from dotenv import load_dotenv

from common.parameters import REALTIME_INSTRUMENTS
from analytics.selector.kite_adapter import KiteAdapter
from analytics.selector.macro_engine import MacroEngine


# Load env
load_dotenv()


async def run():
    print("🚀 Starting Daily Stock Snapshot Job")

    # Init services
    adapter = KiteAdapter()
    engine = MacroEngine()

    conn = await asyncpg.connect(
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )

    for stock_name, token in REALTIME_INSTRUMENTS.items():
        try:
            candles = adapter.fetch_daily_candles(token, days=45)

            if not candles or len(candles) < 30:
                print(f"⚠️ Skipping {stock_name}: insufficient data")
                continue

            metrics = engine.calculate_metrics(candles)
            if not metrics:
                continue

            await conn.execute(
                """
                INSERT INTO daily_stock_snapshot (
                    trade_date,
                    stock_name,
                    close_price,
                    volume,
                    avg_vol_1w,
                    avg_vol_1m,
                    volume_state,
                    price_change_5d,
                    price_change_20d,
                    price_position,
                    trend_bias
                )
                VALUES (
                    CURRENT_DATE,
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10
                )
                ON CONFLICT (trade_date, stock_name)
                DO UPDATE SET
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    avg_vol_1w = EXCLUDED.avg_vol_1w,
                    avg_vol_1m = EXCLUDED.avg_vol_1m,
                    volume_state = EXCLUDED.volume_state,
                    price_change_5d = EXCLUDED.price_change_5d,
                    price_change_20d = EXCLUDED.price_change_20d,
                    price_position = EXCLUDED.price_position,
                    trend_bias = EXCLUDED.trend_bias
                ;
                """,
                stock_name,
                metrics["close"],
                metrics["volume"],
                metrics["avg_vol_1w"],
                metrics["avg_vol_1m"],
                metrics["volume_state"],
                metrics["price_change_5d"],
                metrics["price_change_20d"],
                metrics["price_position"],
                metrics["trend_bias"]
            )

            print(f"✅ {stock_name} updated")

        except Exception as e:
            print(f"❌ Error processing {stock_name}: {e}")

    await conn.close()
    print("🎯 Daily snapshot completed.")


if __name__ == "__main__":
    asyncio.run(run())
