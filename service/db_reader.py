# service/db_reader.py

import asyncpg
from typing import Dict
from service.logger import log

async def fetch_large_trade_thresholds(db_pool: asyncpg.Pool) -> Dict[str, int]:
    """
    Refreshes and fetches large trade thresholds from the large_trade_thresholds_mv materialized view.
    The P99 volume is used as the threshold.
    """
    thresholds = {}
    query = "SELECT stock_name, p995_volume FROM public.large_trade_thresholds_mv;"
    try:
        async with db_pool.acquire() as connection:
            # Refresh the materialized view to ensure data is up-to-date
            log.info("Refreshing materialized view 'large_trade_thresholds_mv'...")
            await connection.execute("REFRESH MATERIALIZED VIEW public.large_trade_thresholds_mv;")
            log.info("Materialized view refreshed successfully.")

            records = await connection.fetch(query)
            for record in records:
                # Ensure p995_volume is not None and is cast to int
                if record['p995_volume'] is not None:
                    thresholds[record['stock_name']] = int(record['p995_volume'])
        log.info(f"Successfully loaded {len(thresholds)} large trade thresholds (P99.5) from the materialized view.")
        return thresholds
    except asyncpg.exceptions.UndefinedTableError:
        log.error("Materialized view 'large_trade_thresholds_mv' not found. Please create it first.")
        return {}
    except Exception as e:
        # It's possible the view doesn't exist on a fresh run, so we don't crash
        log.warning(f"Could not fetch large trade thresholds from materialized view: {e}. Large trade detection may be disabled.")
        return {}
