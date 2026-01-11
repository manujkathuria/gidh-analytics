import json
from typing import List
import asyncpg

from common import config
from common.logger import log
from common.models import EnrichedTick, BarData


async def batch_insert_ticks(db_pool, ticks: List[EnrichedTick]):
    if config.SKIP_RAW_DB_WRITES:
        return

    if not ticks:
        return

    async with db_pool.acquire() as connection:
        try:
            await connection.executemany("""
                INSERT INTO public.live_ticks (
                    timestamp, stock_name, last_price, last_traded_quantity,
                    average_traded_price, volume_traded, total_buy_quantity,
                    total_sell_quantity, ohlc_open, ohlc_high, ohlc_low, ohlc_close,
                    change, instrument_token
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (timestamp, stock_name) DO NOTHING;
            """, [(
                t.timestamp, t.stock_name, t.last_price, t.last_traded_quantity,
                t.average_traded_price, t.volume_traded, t.total_buy_quantity,
                t.total_sell_quantity, t.ohlc_open, t.ohlc_high, t.ohlc_low,
                t.ohlc_close, t.change, t.instrument_token
            ) for t in ticks])
            log.info(
                f"Successfully inserted batch of {len(ticks)} ticks. Sample first tick: {ticks[0].stock_name} @ {ticks[0].timestamp}")
        except asyncpg.PostgresError as e:
            sample_keys = [(t.timestamp, t.stock_name) for t in ticks[:3]]
            log.error(f"Failed to batch insert ticks: {e}; sample keys: {sample_keys}", exc_info=True)
            raise  # so the writer can see it if you want to stop on failure
        except Exception as e:
            log.error(f"An unexpected error occurred during tick insertion: {e}", exc_info=True)
            raise


async def batch_insert_order_depths(db_pool, ticks_with_depth: List[EnrichedTick]):
    """
    Inserts a batch of OrderDepth data into the live_order_depth table.
    """
    if config.SKIP_RAW_DB_WRITES:
        return

    if not ticks_with_depth:
        return

    records_to_insert = []
    for tick in ticks_with_depth:
        if tick.depth:
            depth_update = tick.depth
            for i, level in enumerate(depth_update.buy):
                records_to_insert.append((
                    depth_update.timestamp, depth_update.stock_name, 'buy', i,
                    level.price, level.quantity, level.orders, depth_update.instrument_token
                ))
            for i, level in enumerate(depth_update.sell):
                records_to_insert.append((
                    depth_update.timestamp, depth_update.stock_name, 'sell', i,
                    level.price, level.quantity, level.orders, depth_update.instrument_token
                ))

    if not records_to_insert:
        return

    async with db_pool.acquire() as connection:
        try:
            await connection.executemany("""
                INSERT INTO public.live_order_depth (
                    timestamp, stock_name, side, level, price, quantity, orders, instrument_token
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (timestamp, stock_name, side, level) DO NOTHING;
            """, records_to_insert)
            log.info(f"Successfully inserted batch of {len(records_to_insert)} order depth levels.")
        except asyncpg.PostgresError as e:
            log.error(f"Failed to batch insert order depths: {e}")
        except Exception as e:
            log.error(f"An unexpected error occurred during order depth insertion: {e}")


async def batch_upsert_features(db_pool, bars: List[BarData]):
    """
    Inserts or updates a batch of bar data into the enriched_features table.
    Uses ON CONFLICT to perform an "UPSERT" for real-time updates.
    """
    if not bars:
        return

    records_to_upsert = [
        (
            b.timestamp, b.stock_name, b.interval, b.open, b.high, b.low, b.close,
            b.volume, b.bar_vwap, b.session_vwap, json.dumps(b.raw_scores), b.instrument_token
        ) for b in bars
    ]

    async with db_pool.acquire() as connection:
        try:
            await connection.executemany("""
                INSERT INTO public.enriched_features (
                    timestamp, stock_name, interval, open, high, low, close,
                    volume, bar_vwap, session_vwap, raw_scores, instrument_token
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (timestamp, stock_name, interval) DO UPDATE
                SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    bar_vwap = EXCLUDED.bar_vwap,
                    session_vwap = EXCLUDED.session_vwap,
                    raw_scores = EXCLUDED.raw_scores;
            """, records_to_upsert)
            log.info(f"Successfully upserted batch of {len(bars)} feature bars.")
        except asyncpg.PostgresError as e:
            log.error(f"Failed to batch upsert feature bars: {e}", exc_info=True)
            raise


async def insert_signal(pool, s):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO public.live_signals 
            (timestamp, stock_name, interval, side, entry_price, quantity, div_obv, div_clv, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, s['timestamp'], s['stock_name'], s.get('interval', '10m'), s['side'],
             s['entry_price'], s['quantity'], s['div_obv'], s['div_clv'], s['status'])

async def update_signal_exit(pool, e):
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE public.live_signals 
            SET exit_timestamp = $1, exit_price = $2, exit_reason = $3, 
                pnl_pct = $4, realized_pnl_cash = $5, status = $6
            WHERE stock_name = $7 AND timestamp = $8
        """, e['exit_timestamp'], e['exit_price'], e['exit_reason'],
             e['pnl_pct'], e['pnl_cash'], e['status'], e['stock_name'], e['entry_time'])