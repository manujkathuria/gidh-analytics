# service/db_schema.py

from service import config
from service.logger import log

async def setup_schema(db_pool):
    log.info("Checking and creating database tables and views if necessary...")
    async with db_pool.acquire() as connection:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        # --- Table and Hypertable creation ---
        # (No changes to the table creation logic)
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_ticks (
                timestamp TIMESTAMPTZ NOT NULL, stock_name TEXT NOT NULL,
                last_price DOUBLE PRECISION, last_traded_quantity INTEGER,
                average_traded_price DOUBLE PRECISION, volume_traded BIGINT,
                total_buy_quantity BIGINT, total_sell_quantity BIGINT,
                ohlc_open DOUBLE PRECISION, ohlc_high DOUBLE PRECISION,
                ohlc_low DOUBLE PRECISION, ohlc_close DOUBLE PRECISION,
                change DOUBLE PRECISION, instrument_token INTEGER,
                PRIMARY KEY (timestamp, stock_name)
            );
        """)
        await connection.execute("SELECT create_hypertable('live_ticks', 'timestamp', if_not_exists => TRUE);")

        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_order_depth (
                timestamp TIMESTAMPTZ NOT NULL, stock_name TEXT NOT NULL,
                side TEXT NOT NULL, level INTEGER NOT NULL, price DOUBLE PRECISION,
                quantity BIGINT, orders INTEGER, instrument_token INTEGER,
                PRIMARY KEY (timestamp, stock_name, side, level)
            );
        """)
        await connection.execute("SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);")

        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.enriched_features (
                timestamp TIMESTAMPTZ NOT NULL, stock_name TEXT NOT NULL,
                interval TEXT NOT NULL, open DOUBLE PRECISION, high DOUBLE PRECISION,
                low DOUBLE PRECISION, close DOUBLE PRECISION, volume BIGINT,
                bar_vwap DOUBLE PRECISION, session_vwap DOUBLE PRECISION,
                raw_scores JSONB, instrument_token INTEGER,
                PRIMARY KEY (timestamp, stock_name, interval)
            );
        """)
        await connection.execute("SELECT create_hypertable('enriched_features', 'timestamp', if_not_exists => TRUE);")


        log.info("Creating or replacing the Grafana features view...")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.grafana_features_view AS
            SELECT
                timestamp, stock_name, interval, open, high, low, close, volume,
                bar_vwap, session_vwap, instrument_token,
                COALESCE((raw_scores->>'bar_delta')::BIGINT, 0) AS bar_delta,
                COALESCE((raw_scores->>'large_buy_volume')::BIGINT, 0) AS large_buy_volume,
                COALESCE((raw_scores->>'large_sell_volume')::BIGINT, 0) AS large_sell_volume,
                COALESCE((raw_scores->>'passive_buy_volume')::BIGINT, 0) AS passive_buy_volume,
                COALESCE((raw_scores->>'passive_sell_volume')::BIGINT, 0) AS passive_sell_volume,
                COALESCE((raw_scores->>'cvd_30m')::BIGINT, 0) AS cvd_30m,
                COALESCE((raw_scores->>'rsi')::DOUBLE PRECISION, 50.0) AS rsi,
                COALESCE((raw_scores->>'mfi')::DOUBLE PRECISION, 50.0) AS mfi,
                COALESCE((raw_scores->>'obv')::BIGINT, 0) AS obv,
                COALESCE((raw_scores->>'lvc_delta')::BIGINT, 0) AS lvc_delta,
                COALESCE((raw_scores->>'clv')::DOUBLE PRECISION, 0.0) AS clv, -- Add CLV to the view

                -- Tier 1: Price vs. Features
                COALESCE((raw_scores->'divergence'->>'price_vs_lvc')::DOUBLE PRECISION, 0.0) AS div_price_lvc,
                COALESCE((raw_scores->'divergence'->>'price_vs_cvd')::DOUBLE PRECISION, 0.0) AS div_price_cvd,
                COALESCE((raw_scores->'divergence'->>'price_vs_obv')::DOUBLE PRECISION, 0.0) AS div_price_obv,
                COALESCE((raw_scores->'divergence'->>'price_vs_rsi')::DOUBLE PRECISION, 0.0) AS div_price_rsi,
                COALESCE((raw_scores->'divergence'->>'price_vs_mfi')::DOUBLE PRECISION, 0.0) AS div_price_mfi,

                -- Tier 2: LVC vs. Features
                COALESCE((raw_scores->'divergence'->>'lvc_vs_cvd')::DOUBLE PRECISION, 0.0) AS div_lvc_cvd,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_obv')::DOUBLE PRECISION, 0.0) AS div_lvc_obv,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_rsi')::DOUBLE PRECISION, 0.0) AS div_lvc_rsi,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_mfi')::DOUBLE PRECISION, 0.0) AS div_lvc_mfi

            FROM
                public.enriched_features;
        """)
        log.info("Grafana view 'grafana_features_view' is ready.")

    log.info("Database schema setup is complete.")


async def truncate_tables_if_needed(db_pool):
    if config.PIPELINE_MODE == 'backtesting' and config.TRUNCATE_TABLES_ON_BACKTEST:
        log.warning("Truncating 'live_ticks', 'live_order_depth', and 'enriched_features' tables as per configuration.")
        try:
            async with db_pool.acquire() as connection:
                await connection.execute("TRUNCATE TABLE public.live_ticks, public.live_order_depth, public.enriched_features RESTART IDENTITY;")
            log.info("Successfully truncated tables.")
        except Exception as e:
            log.error(f"Failed to truncate tables: {e}")
            raise
    else:
        log.info("Skipping table truncation based on current mode and configuration.")
