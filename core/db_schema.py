from datetime import datetime
from common import config
from common.logger import log


async def setup_schema(db_pool):
    log.info("Checking and creating database tables and views if necessary...")
    async with db_pool.acquire() as connection:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        # --- Table and Hypertable creation (No changes here) ---
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
            CREATE TABLE IF NOT EXISTS public.live_signals (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,       -- Entry Time
                stock_name TEXT NOT NULL,
                interval TEXT NOT NULL,
                side TEXT NOT NULL,                  -- 'SHORT'
                entry_price DOUBLE PRECISION,
                quantity INTEGER,                    -- Number of shares based on cash risk
                stop_loss DOUBLE PRECISION,
                tp1 DOUBLE PRECISION,
                tp2 DOUBLE PRECISION,
                div_obv DOUBLE PRECISION,
                div_clv DOUBLE PRECISION,
                structure TEXT,
                
                -- Exit Columns
                exit_timestamp TIMESTAMPTZ,
                exit_price DOUBLE PRECISION,
                exit_reason TEXT,                    -- 'SIGNAL_INVALIDATED', 'TP2_FULL', etc.
                pnl_pct DOUBLE PRECISION,            -- Total percentage return
                realized_pnl_cash DOUBLE PRECISION,  -- Total revenue/loss in cash
                status TEXT DEFAULT 'OPEN',          -- 'OPEN' or 'CLOSED'
                is_alerted BOOLEAN DEFAULT FALSE
            );
        """)

        await connection.execute("""
            CREATE INDEX IF NOT EXISTS live_signals_stock_time_idx ON live_signals (stock_name, timestamp);
        """)

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

        # --- Create Materialized View for Large Trade Thresholds (with daily median fix) ---
        log.info("Creating materialized view 'large_trade_thresholds_mv' if it does not exist...")
        await connection.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.large_trade_thresholds_mv AS
            WITH trade_volumes AS (
                SELECT
                    lt.stock_name,
                    lt.volume_traded - lag(lt.volume_traded, 1, 0::bigint)
                        OVER (PARTITION BY lt.stock_name, (lt.timestamp::date)
                              ORDER BY lt.timestamp) AS tick_volume,
                    lt.timestamp::date AS trade_day
                FROM live_ticks lt
                WHERE lt.timestamp >= (date_trunc('day', now()) - interval '7 days')
            ),
            daily_pxx AS (
                SELECT
                    stock_name,
                    trade_day,
                    percentile_cont(0.95)  WITHIN GROUP (ORDER BY tick_volume) AS day_p95,
                    percentile_cont(0.99)  WITHIN GROUP (ORDER BY tick_volume) AS day_p99,
                    percentile_cont(0.995) WITHIN GROUP (ORDER BY tick_volume) AS day_p995,
                    percentile_cont(0.999) WITHIN GROUP (ORDER BY tick_volume) AS day_p999,
                    max(tick_volume) AS day_max,
                    count(*)        AS day_trades
                FROM trade_volumes
                WHERE tick_volume > 0
                GROUP BY stock_name, trade_day
            )
            SELECT
                stock_name,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p95)  AS p95_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p99)  AS p99_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p995) AS p995_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p999) AS p999_volume,
                max(day_max)    AS max_volume,
                sum(day_trades) AS total_trades
            FROM daily_pxx
            GROUP BY stock_name;
        """)
        log.info("Materialized view 'large_trade_thresholds_mv' (with median-of-daily-percentiles) is ready.")

        await connection.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS large_trade_thresholds_mv_pk
            ON public.large_trade_thresholds_mv (stock_name);
        """)


        # --- Create grafana_features_view ---
        log.info("Creating or replacing the Grafana features view...")
        await connection.execute("DROP VIEW IF EXISTS public.grafana_features_view CASCADE;")
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
                COALESCE((raw_scores->>'cvd_5m')::BIGINT, 0) AS cvd_5m,
                COALESCE((raw_scores->>'cvd_10m')::BIGINT, 0) AS cvd_10m,
                COALESCE((raw_scores->>'cvd_30m')::BIGINT, 0) AS cvd_30m,
                COALESCE((raw_scores->>'rsi')::DOUBLE PRECISION, 50.0) AS rsi,
                COALESCE((raw_scores->>'mfi')::DOUBLE PRECISION, 50.0) AS mfi,
                COALESCE((raw_scores->>'obv')::BIGINT, 0) AS obv,
                COALESCE((raw_scores->>'lvc_delta')::BIGINT, 0) AS institutional_flow_delta,
                COALESCE((raw_scores->>'clv')::DOUBLE PRECISION, 0.0) AS clv,
                COALESCE((raw_scores->>'clv_smoothed')::DOUBLE PRECISION, 0.0) AS clv_smoothed,
                COALESCE((raw_scores->>'cvd_5m_smoothed')::DOUBLE PRECISION, 0.0) AS cvd_5m_smoothed,
                COALESCE((raw_scores->>'rsi_smoothed')::DOUBLE PRECISION, 50.0) AS rsi_smoothed,
                COALESCE((raw_scores->>'mfi_smoothed')::DOUBLE PRECISION, 50.0) AS mfi_smoothed,
                COALESCE((raw_scores->>'inst_flow_delta_smoothed')::DOUBLE PRECISION, 0.0) AS inst_flow_delta_smoothed,
                COALESCE((raw_scores->>'HH')::BOOLEAN, FALSE) AS is_hh,
                COALESCE((raw_scores->>'HL')::BOOLEAN, FALSE) AS is_hl,
                COALESCE((raw_scores->>'LH')::BOOLEAN, FALSE) AS is_lh,
                COALESCE((raw_scores->>'LL')::BOOLEAN, FALSE) AS is_ll,
                COALESCE((raw_scores->>'inside')::BOOLEAN, FALSE) AS is_inside_bar,
                COALESCE((raw_scores->>'outside')::BOOLEAN, FALSE) AS is_outside_bar,
                COALESCE(raw_scores->>'structure', 'init') AS bar_structure,
                COALESCE((raw_scores->'divergence'->>'price_vs_lvc')::DOUBLE PRECISION, 0.0) AS div_price_lvc,
                COALESCE((raw_scores->'divergence'->>'price_vs_cvd')::DOUBLE PRECISION, 0.0) AS div_price_cvd,
                COALESCE((raw_scores->'divergence'->>'price_vs_obv')::DOUBLE PRECISION, 0.0) AS div_price_obv,
                COALESCE((raw_scores->'divergence'->>'price_vs_rsi')::DOUBLE PRECISION, 0.0) AS div_price_rsi,
                COALESCE((raw_scores->'divergence'->>'price_vs_mfi')::DOUBLE PRECISION, 0.0) AS div_price_mfi,
                COALESCE((raw_scores->'divergence'->>'price_vs_clv')::DOUBLE PRECISION, 0.0) AS div_price_clv,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_cvd')::DOUBLE PRECISION, 0.0) AS div_lvc_cvd,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_obv')::DOUBLE PRECISION, 0.0) AS div_lvc_obv,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_rsi')::DOUBLE PRECISION, 0.0) AS div_lvc_rsi,
                COALESCE((raw_scores->'divergence'->>'lvc_vs_mfi')::DOUBLE PRECISION, 0.0) AS div_lvc_mfi
            FROM
                public.enriched_features;
        """)
        log.info("Grafana view 'grafana_features_view' is ready.")

        # --- Create the market_data_aggregated_view with rolling sums ---
        log.info("Creating or replacing the market data aggregated view...")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.market_data_aggregated_view AS
            WITH base_data AS (
                -- Get all 1-minute data
                SELECT timestamp,
                       stock_name,
                       large_buy_volume - large_sell_volume     AS net_aggressive_volume,
                       passive_buy_volume - passive_sell_volume AS net_passive_volume,
                       "close"
                FROM public.grafana_features_view
                WHERE "interval" = '1m'),
                 aggregated_by_interval AS (
                     -- 15m aggregation
                     SELECT time_bucket('15m', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 15 minutes')) AS "timestamp",
                            stock_name,
                            '15m'                                                                             AS interval_agg,
                            SUM(net_aggressive_volume)                                                        AS "Net Inst",
                            SUM(net_passive_volume)                                                           AS "Net Iceberg",
                            last("close", timestamp)                                                          AS "Price"
                     FROM base_data
                     GROUP BY 1, 2
            
                     UNION ALL
            
                     -- 30m aggregation
                     SELECT time_bucket('30m', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
                            stock_name,
                            '30m'                                                                             AS interval_agg,
                            SUM(net_aggressive_volume)                                                        AS "Net Inst",
                            SUM(net_passive_volume)                                                           AS "Net Iceberg",
                            last("close", timestamp)                                                          AS "Price"
                     FROM base_data
                     GROUP BY 1, 2
            
                     UNION ALL
            
                     -- 1h aggregation
                     SELECT time_bucket('1h', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
                            stock_name,
                            '1h'                                                                              AS interval_agg,
                            SUM(net_aggressive_volume)                                                        AS "Net Inst",
                            SUM(net_passive_volume)                                                           AS "Net Iceberg",
                            last("close", timestamp)                                                          AS "Price"
                     FROM base_data
                     GROUP BY 1, 2)
            -- Calculate the final rolling sums with shorter aliases
            SELECT "timestamp",
                   "stock_name",
                   "interval_agg",
                   "Price",
                   "Net Inst",
                   "Net Iceberg",
                   -- Renamed for brevity
                   SUM("Net Inst")
                   OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Inst_Flow_60m",
                   -- Renamed for brevity
                   SUM("Net Iceberg")
                   OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Iceberg_Flow_60m"
            FROM aggregated_by_interval;
        """)
        log.info("Aggregated data view 'market_data_aggregated_view' is ready.")

    log.info("Database schema setup is complete.")


async def truncate_tables_if_needed(db_pool):
    if config.PIPELINE_MODE == 'backtesting' and config.TRUNCATE_TABLES_ON_BACKTEST:
        log.warning("Cleaning up tables for backtest run...")
        try:
            # Convert the date string from config to a date object
            backtest_date = datetime.strptime(config.BACKTEST_DATE_STR, '%Y-%m-%d').date()

            async with db_pool.acquire() as connection:
                # Delete any existing tick data for the specific backtest date
                log.info(f"Deleting existing ticks from live_ticks for date: {backtest_date}")
                await connection.execute(
                    'DELETE FROM public.live_ticks WHERE "timestamp"::date = $1;',
                    backtest_date  # Pass the date object here
                )

                # Truncate the other tables to ensure they are empty
                log.info("Truncating 'live_order_depth' and 'enriched_features' tables.")
                await connection.execute(
                    "TRUNCATE TABLE public.live_order_depth, public.enriched_features RESTART IDENTITY;")
            log.info("Successfully cleaned up tables for the backtest.")
        except Exception as e:
            log.error(f"Failed to clean up tables: {e}", exc_info=True)
            raise
    else:
        log.info("Skipping table cleanup based on current mode and configuration.")
