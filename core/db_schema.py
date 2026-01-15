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
        # FIXED: Materialized View lookback for backtesting
        ref_date = f"'{config.BACKTEST_DATE_STR}'::date" if config.PIPELINE_MODE == 'backtesting' else "now()"
        
        # We must drop the MV in backtesting to force a recalculation for the specific day
        if config.PIPELINE_MODE == 'backtesting':
            await connection.execute("DROP MATERIALIZED VIEW IF EXISTS public.large_trade_thresholds_mv CASCADE;")

        await connection.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.large_trade_thresholds_mv AS
            WITH trade_volumes AS (
                SELECT
                    lt.stock_name,
                    lt.volume_traded - lag(lt.volume_traded, 1, 0::bigint) OVER (PARTITION BY lt.stock_name, (lt.timestamp::date) ORDER BY lt.timestamp) AS tick_volume,
                    lt.timestamp::date AS trade_day
                FROM live_ticks lt
                WHERE lt.timestamp >= (date_trunc('day', {ref_date}) - interval '7 days')
                  AND lt.timestamp < date_trunc('day', {ref_date})
            ),
            daily_pxx AS (
                SELECT stock_name, trade_day,
                    percentile_cont(0.95)  WITHIN GROUP (ORDER BY tick_volume) AS day_p95,
                    percentile_cont(0.99)  WITHIN GROUP (ORDER BY tick_volume) AS day_p99,
                    percentile_cont(0.995) WITHIN GROUP (ORDER BY tick_volume) AS day_p995,
                    percentile_cont(0.999) WITHIN GROUP (ORDER BY tick_volume) AS day_p999,
                    max(tick_volume) AS day_max, count(*) AS day_trades
                FROM trade_volumes WHERE tick_volume > 0 GROUP BY 1, 2
            )
            SELECT stock_name,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p95)  AS p95_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p99)  AS p99_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p995) AS p995_volume,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p999) AS p999_volume,
                max(day_max) AS max_volume, sum(day_trades) AS total_trades
            FROM daily_pxx GROUP BY stock_name;
        """)

        # FIXED: Sync View Header (49 columns) with SELECT (49 columns)
        await connection.execute("DROP VIEW IF EXISTS public.grafana_features_view CASCADE;")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.grafana_features_view
            (timestamp, stock_name, interval, open, high, low, close, volume, bar_vwap, session_vwap, instrument_token,
             bar_delta, large_buy_volume, large_sell_volume, passive_buy_volume, passive_sell_volume, cvd_5m, cvd_10m,
             cvd_30m, rsi, mfi, obv, institutional_flow_delta, clv, clv_smoothed, cvd_5m_smoothed, rsi_smoothed,
             mfi_smoothed, inst_flow_delta_smoothed, 
             structure_delta, structure_ratio, -- FIXED: Column header sync
             is_hh, is_hl, is_lh, is_ll, is_inside_bar, is_outside_bar,
             bar_structure, div_price_lvc, div_price_cvd, div_price_obv, div_price_rsi, div_price_mfi, div_price_clv,
             div_price_vwap, div_lvc_cvd, div_lvc_obv, div_lvc_rsi, div_lvc_mfi)
            AS
            SELECT enriched_features."timestamp", ... -- (Rest of your SELECT query as provided previously)
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
    """
    Cleans up data for a specific backtesta date.
    Deletes ticks and features only for the target date to preserve history.
    Truncates the order book entirely as it is highly transient.
    """
    if config.PIPELINE_MODE == 'backtesting' and config.TRUNCATE_TABLES_ON_BACKTEST:
        log.warning("Performing targeted cleanup for backtesta run...")
        try:
            # Convert the date string from config to a date object
            backtest_date = datetime.strptime(config.BACKTEST_DATE_STR, '%Y-%m-%d').date()

            async with db_pool.acquire() as connection:
                # 1. Targeted Delete: Live Ticks
                log.info(f"Deleting existing ticks from live_ticks for date: {backtest_date}")
                await connection.execute(
                    'DELETE FROM public.live_ticks WHERE "timestamp"::date = $1;',
                    backtest_date
                )

                # 2. Targeted Delete: Enriched Features (Preserves other dates)
                log.info(f"Deleting existing features from enriched_features for date: {backtest_date}")
                await connection.execute(
                    'DELETE FROM public.enriched_features WHERE "timestamp"::date = $1;',
                    backtest_date
                )

                # 3. Full Truncate: Live Order Depth (No problem to truncate)
                log.info("Truncating 'live_order_depth' table to ensure a clean L2 snapshot.")
                await connection.execute(
                    "TRUNCATE TABLE public.live_order_depth RESTART IDENTITY;"
                )

            log.info(f"Successfully prepared database for backtesta on {backtest_date}.")
        except Exception as e:
            log.error(f"Failed to clean up tables: {e}", exc_info=True)
            raise
    else:
        log.info("Skipping table cleanup based on current mode and configuration.")
