# core/db_schema.py

from datetime import datetime
from common import config
from common.logger import log

async def setup_schema(db_pool):
    """
    Sets up the required database tables, hypertables, and optimized views.
    """
    log.info("Checking and creating database tables and views if necessary...")
    async with db_pool.acquire() as connection:
        # Enable the TimescaleDB extension for time-series optimization
        await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        # Create the hypertable for raw tick data
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

        # Create the table for trade signals and performance reports
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_signals (
                id SERIAL PRIMARY KEY,
                event_time TIMESTAMPTZ NOT NULL,
                processed_at TIMESTAMPTZ DEFAULT now(),
                stock_name TEXT NOT NULL,
                interval TEXT NOT NULL,
                authority TEXT NOT NULL,
                event_type TEXT NOT NULL,
                side TEXT NOT NULL,
                price DOUBLE PRECISION,
                vwap DOUBLE PRECISION,
                cost_regime SMALLINT,
                path_regime SMALLINT,
                accept_regime SMALLINT,
                entry_price DOUBLE PRECISION,
                peak_price DOUBLE PRECISION,
                mfe_pct DOUBLE PRECISION,
                mae_pct DOUBLE PRECISION,
                pnl_pct DOUBLE PRECISION,
                indicators JSONB,
                reason TEXT
            );
        """)
        await connection.execute("CREATE INDEX IF NOT EXISTS idx_signals_stock_event ON live_signals (stock_name, event_time);")
        await connection.execute("CREATE INDEX IF NOT EXISTS idx_signals_authority ON live_signals (authority);")

        # Create the hypertable for order book depth (L2)
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_order_depth (
                timestamp TIMESTAMPTZ NOT NULL, stock_name TEXT NOT NULL,
                side TEXT NOT NULL, level INTEGER NOT NULL, price DOUBLE PRECISION,
                quantity BIGINT, orders INTEGER, instrument_token INTEGER,
                PRIMARY KEY (timestamp, stock_name, side, level)
            );
        """)
        await connection.execute("SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);")

        # Create the hypertable for aggregated bar features
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

        # Materialized View for calculating 'Large Trade' thresholds from the last 7 days
        ref_date = f"'{config.BACKTEST_DATE_STR}'::date" if config.PIPELINE_MODE == 'backtesting' else "now()"
        if config.PIPELINE_MODE == 'backtesting':
            await connection.execute("DROP MATERIALIZED VIEW IF EXISTS public.large_trade_thresholds_mv CASCADE;")
        await connection.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.large_trade_thresholds_mv AS
            WITH trade_volumes AS (
                SELECT stock_name, timestamp::date AS trade_day,
                    volume_traded - lag(volume_traded, 1, 0::bigint) OVER (PARTITION BY stock_name, (timestamp::date) ORDER BY timestamp) AS tick_volume
                FROM live_ticks
                WHERE timestamp >= (date_trunc('day', {ref_date}) - interval '7 days')
                  AND timestamp < date_trunc('day', {ref_date})
            ),
            daily_pxx AS (
                SELECT stock_name, trade_day,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY tick_volume) AS day_p95,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY tick_volume) AS day_p99
                FROM trade_volumes WHERE tick_volume > 0 GROUP BY 1, 2
            )
            SELECT stock_name,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p95) AS p95_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p99) AS p99_volume
            FROM daily_pxx GROUP BY stock_name;
        """)
        await connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS large_trade_thresholds_mv_pk ON public.large_trade_thresholds_mv (stock_name);")

        # Optimized view for Grafana panels including OHLC, Handshake Sensors, and Iceberg volumes
        log.info("Creating or replacing the simplified Grafana features view...")
        await connection.execute("DROP VIEW IF EXISTS public.grafana_features_view CASCADE;")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.grafana_features_view AS
            SELECT
                timestamp,
                stock_name,
                "interval",
                open,
                high,
                low,
                close,
                volume,
                session_vwap,
                -- Sensor handshakes
                COALESCE((raw_scores ->> 'structure_ratio')::double precision, 0.0) AS path_ratio,
                COALESCE(((raw_scores -> 'divergence') ->> 'price_vs_vwap')::double precision, 0.0) AS cost_vwap_ratio,
                COALESCE(((raw_scores -> 'divergence') ->> 'price_vs_obv')::double precision, 0.0) AS cost_obv_ratio,
                COALESCE((raw_scores ->> 'price_acceptance')::integer, 0) AS confirm_ratio,
                COALESCE(((raw_scores -> 'divergence') ->> 'price_vs_clv')::double precision, 0.0) AS pressure_ratio,
                -- Order flow volumes (Icebergs)
                COALESCE((raw_scores ->> 'large_buy_volume')::bigint, 0) AS large_buy_volume,
                COALESCE((raw_scores ->> 'large_sell_volume')::bigint, 0) AS large_sell_volume,
                COALESCE((raw_scores ->> 'passive_buy_volume')::bigint, 0) AS passive_buy_volume,
                COALESCE((raw_scores ->> 'passive_sell_volume')::bigint, 0) AS passive_sell_volume,
                -- Indicators
                COALESCE((raw_scores ->> 'rsi')::double precision, 50.0) AS rsi,
                instrument_token
            FROM public.enriched_features;
        """)

        # Remove legacy aggregation view
        await connection.execute("DROP VIEW IF EXISTS public.market_data_aggregated_view CASCADE;")

    log.info("Database schema setup is complete.")

async def truncate_tables_if_needed(db_pool):
    """
    Cleans up database tables before a backtesting run if configured.
    """
    if config.PIPELINE_MODE == 'backtesting' and config.TRUNCATE_TABLES_ON_BACKTEST:
        log.warning("Performing targeted cleanup for backtest run...")
        try:
            backtest_date = datetime.strptime(config.BACKTEST_DATE_STR, '%Y-%m-%d').date()
            async with db_pool.acquire() as connection:
                await connection.execute('DELETE FROM public.live_ticks WHERE "timestamp"::date = $1;', backtest_date)
                await connection.execute('DELETE FROM public.enriched_features WHERE "timestamp"::date = $1;', backtest_date)
                await connection.execute("TRUNCATE TABLE public.live_order_depth RESTART IDENTITY;")
            log.info(f"Successfully prepared database for backtest on {backtest_date}.")
        except Exception as e:
            log.error(f"Failed to clean up tables: {e}", exc_info=True)
            raise