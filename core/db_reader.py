# service/db_reader.py

import asyncpg
from typing import Dict
from common.logger import log
from datetime import datetime


async def fetch_live_thresholds(db_pool: asyncpg.Pool, refresh: bool = True) -> Dict[str, int]:
    """
    Fetches large trade thresholds from the materialized view.
    """
    thresholds = {}
    query = "SELECT stock_name, p99_volume FROM public.large_trade_thresholds_mv;"
    try:
        async with db_pool.acquire() as connection:
            if refresh:
                log.info("Refreshing materialized view 'large_trade_thresholds_mv'...")
                await connection.execute("REFRESH MATERIALIZED VIEW public.large_trade_thresholds_mv;")

            records = await connection.fetch(query)
            for record in records:
                if record['p99_volume'] is not None:
                    thresholds[record['stock_name']] = int(record['p99_volume'])
        log.info(f"Successfully loaded {len(thresholds)} LIVE thresholds from materialized view.")
        return thresholds
    except asyncpg.exceptions.UndefinedTableError:
        log.error("Materialized view 'large_trade_thresholds_mv' not found. Please create it first.")
        return {}
    except Exception as e:
        log.warning(f"Could not fetch live thresholds: {e}. Large trade detection may be disabled.")
        return {}


async def calculate_and_fetch_backtest_thresholds(db_pool: asyncpg.Pool, backtest_date_str: str) -> Dict[str, int]:
    """
    Dynamically calculates and fetches large trade thresholds for BACKTESTING.
    It uses the 7 days of data immediately prior to the backtest date.
    """
    thresholds = {}
    log.info(f"Calculating backtest thresholds for date: {backtest_date_str}...")

    # This query dynamically calculates the p99 volume from the 7 days before the backtest date.
    query = """
        WITH trade_volumes AS (
            SELECT
                lt.stock_name,
                lt.volume_traded - lag(lt.volume_traded, 1, 0::bigint) OVER (PARTITION BY lt.stock_name, (lt.timestamp::date) ORDER BY lt.timestamp) AS tick_volume
            FROM live_ticks lt
            WHERE lt.timestamp >= ($1::date - '7 days'::interval) AND lt.timestamp < $1::date
        )
        SELECT
            tv.stock_name,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY tv.tick_volume::double precision) AS p99_volume
        FROM trade_volumes tv
        WHERE tv.tick_volume > 0
        GROUP BY tv.stock_name;
    """
    try:
        backtest_date = datetime.strptime(backtest_date_str, '%Y-%m-%d').date()
        async with db_pool.acquire() as connection:
            records = await connection.fetch(query, backtest_date)
            for record in records:
                if record['p99_volume'] is not None:
                    thresholds[record['stock_name']] = int(record['p99_volume'])
        log.info(f"Successfully calculated and loaded {len(thresholds)} BACKTEST thresholds.")
        return thresholds
    except Exception as e:
        log.error(f"Failed to calculate backtest thresholds: {e}. Large trade detection will be disabled.")
        return {}
