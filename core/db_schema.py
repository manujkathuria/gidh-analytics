from datetime import datetime
from common import config
from common.logger import log


async def setup_schema(db_pool):
    log.info("Checking and creating database tables and views if necessary...")
    async with db_pool.acquire() as connection:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        await connection.execute("""CREATE TABLE IF NOT EXISTS public.stock_strategy_configs(
                  stock_name TEXT PRIMARY KEY,
                  reg_int TEXT NOT NULL,
                  tim_int TEXT NOT NULL,
                  r_val DOUBLE PRECISION NOT NULL,
                  c_val DOUBLE PRECISION NOT NULL,
                  t_val DOUBLE PRECISION NOT NULL,
                  stop_loss DOUBLE PRECISION NOT NULL,
                  updated_at TIMESTAMPTZ DEFAULT NOW());""")
        log.info("Table 'stock_strategy_configs' is ready.")

        # --- Table and Hypertable creation ---
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
                                 CREATE TABLE IF NOT EXISTS public.live_signals
                                 (
                                     id
                                     SERIAL
                                     PRIMARY
                                     KEY,
                                     event_time
                                     TIMESTAMPTZ
                                     NOT
                                     NULL,
                                     processed_at
                                     TIMESTAMPTZ
                                     DEFAULT
                                     now
                                 (
                                 ),
                                     stock_name TEXT NOT NULL,
                                     interval TEXT NOT NULL, -- '1m', '5m', etc.
                                     authority TEXT NOT NULL, -- 'micro', 'trade', 'structural'
                                     event_type TEXT NOT NULL, -- 'LONG_ENTRY', etc.
                                     side TEXT NOT NULL,
                                     price DOUBLE PRECISION,
                                     vwap DOUBLE PRECISION,
                                     cost_regime SMALLINT, -- +1, 0, -1
                                     path_regime SMALLINT, -- +1, 0, -1
                                     accept_regime SMALLINT, -- +1, 0, -1
                                     indicators JSONB,
                                     reason TEXT
                                     );
                                 """)

        await connection.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_signals_stock_event
                                     ON live_signals (stock_name, event_time);
                                 """)

        await connection.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_signals_authority
                                     ON live_signals (authority);
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
        ref_date = f"'{config.BACKTEST_DATE_STR}'::date" if config.PIPELINE_MODE == 'backtesting' else "now()"

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

        await connection.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS large_trade_thresholds_mv_pk
            ON public.large_trade_thresholds_mv (stock_name);
        """)

        # --- Create grafana_features_view ---
        log.info("Creating or replacing the Grafana features view...")
        await connection.execute("DROP VIEW IF EXISTS public.grafana_features_view CASCADE;")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.grafana_features_view
            (timestamp, stock_name, interval, open, high, low, close, volume, bar_vwap, session_vwap, instrument_token,
             bar_delta, large_buy_volume, large_sell_volume, passive_buy_volume, passive_sell_volume, cvd_5m, cvd_10m,
             cvd_30m, rsi, mfi, obv, institutional_flow_delta, clv, clv_smoothed, cvd_5m_smoothed, rsi_smoothed,
             mfi_smoothed, inst_flow_delta_smoothed, 
             structure_delta, structure_ratio, 
             is_hh, is_hl, is_lh, is_ll, is_inside_bar, is_outside_bar,
             bar_structure, div_price_lvc, div_price_cvd, div_price_obv, div_price_rsi, div_price_mfi, div_price_clv,
             div_price_vwap, div_lvc_cvd, div_lvc_obv, div_lvc_rsi, div_lvc_mfi)
            AS
            SELECT enriched_features."timestamp",
                   enriched_features.stock_name,
                   enriched_features."interval",
                   enriched_features.open,
                   enriched_features.high,
                   enriched_features.low,
                   enriched_features.close,
                   enriched_features.volume,
                   enriched_features.bar_vwap,
                   enriched_features.session_vwap,
                   enriched_features.instrument_token,
                   COALESCE((enriched_features.raw_scores ->> 'bar_delta'::text)::bigint, 0::bigint) AS bar_delta,
                   COALESCE((enriched_features.raw_scores ->> 'large_buy_volume'::text)::bigint, 0::bigint) AS large_buy_volume,
                   COALESCE((enriched_features.raw_scores ->> 'large_sell_volume'::text)::bigint, 0::bigint) AS large_sell_volume,
                   COALESCE((enriched_features.raw_scores ->> 'passive_buy_volume'::text)::bigint, 0::bigint) AS passive_buy_volume,
                   COALESCE((enriched_features.raw_scores ->> 'passive_sell_volume'::text)::bigint, 0::bigint) AS passive_sell_volume,
                   COALESCE((enriched_features.raw_scores ->> 'cvd_5m'::text)::bigint, 0::bigint) AS cvd_5m,
                   COALESCE((enriched_features.raw_scores ->> 'cvd_10m'::text)::bigint, 0::bigint) AS cvd_10m,
                   COALESCE((enriched_features.raw_scores ->> 'cvd_30m'::text)::bigint, 0::bigint) AS cvd_30m,
                   COALESCE((enriched_features.raw_scores ->> 'rsi'::text)::double precision, 50.0::double precision) AS rsi,
                   COALESCE((enriched_features.raw_scores ->> 'mfi'::text)::double precision, 50.0::double precision) AS mfi,
                   COALESCE((enriched_features.raw_scores ->> 'obv'::text)::bigint, 0::bigint) AS obv,
                   COALESCE((enriched_features.raw_scores ->> 'lvc_delta'::text)::bigint, 0::bigint) AS institutional_flow_delta,
                   COALESCE((enriched_features.raw_scores ->> 'clv'::text)::double precision, 0.0::double precision) AS clv,
                   COALESCE((enriched_features.raw_scores ->> 'clv_smoothed'::text)::double precision, 0.0::double precision) AS clv_smoothed,
                   COALESCE((enriched_features.raw_scores ->> 'cvd_5m_smoothed'::text)::double precision, 0.0::double precision) AS cvd_5m_smoothed,
                   COALESCE((enriched_features.raw_scores ->> 'rsi_smoothed'::text)::double precision, 50.0::double precision) AS rsi_smoothed,
                   COALESCE((enriched_features.raw_scores ->> 'mfi_smoothed'::text)::double precision, 50.0::double precision) AS mfi_smoothed,
                   COALESCE((enriched_features.raw_scores ->> 'inst_flow_delta_smoothed'::text)::double precision, 0.0::double precision) AS inst_flow_delta_smoothed,
                   COALESCE((enriched_features.raw_scores ->> 'structure_delta'::text)::integer, 0) AS structure_delta,
                   COALESCE((enriched_features.raw_scores ->> 'structure_ratio'::text)::double precision, 0.0::double precision) AS structure_ratio,                
                   COALESCE((enriched_features.raw_scores ->> 'HH'::text)::boolean, false) AS is_hh,
                   COALESCE((enriched_features.raw_scores ->> 'HL'::text)::boolean, false) AS is_hl,
                   COALESCE((enriched_features.raw_scores ->> 'LH'::text)::boolean, false) AS is_lh,
                   COALESCE((enriched_features.raw_scores ->> 'LL'::text)::boolean, false) AS is_ll,
                   COALESCE((enriched_features.raw_scores ->> 'inside'::text)::boolean, false) AS is_inside_bar,
                   COALESCE((enriched_features.raw_scores ->> 'outside'::text)::boolean, false) AS is_outside_bar,
                   COALESCE(enriched_features.raw_scores ->> 'structure'::text, 'init'::text) AS bar_structure,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_lvc'::text)::double precision, 0.0::double precision) AS div_price_lvc,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_cvd'::text)::double precision, 0.0::double precision) AS div_price_cvd,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_obv'::text)::double precision, 0.0::double precision) AS div_price_obv,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_rsi'::text)::double precision, 0.0::double precision) AS div_price_rsi,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_mfi'::text)::double precision, 0.0::double precision) AS div_price_mfi,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_clv'::text)::double precision, 0.0::double precision) AS div_price_clv,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_vwap'::text)::double precision, 0.0::double precision) AS div_price_vwap,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_cvd'::text)::double precision, 0.0::double precision) AS div_lvc_cvd,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_obv'::text)::double precision, 0.0::double precision) AS div_lvc_obv,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_rsi'::text)::double precision, 0.0::double precision) AS div_lvc_rsi,
                   COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_mfi'::text)::double precision, 0.0::double precision) AS div_lvc_mfi
            FROM enriched_features;
        """)
        log.info("Grafana view 'grafana_features_view' is ready.")

        # --- Create the market_data_aggregated_view with rolling sums ---
        log.info("Creating or replacing the market data aggregated view...")
        await connection.execute("""
            CREATE OR REPLACE VIEW public.market_data_aggregated_view AS
            WITH base_data AS (
                SELECT timestamp, stock_name,
                       large_buy_volume - large_sell_volume     AS net_aggressive_volume,
                       passive_buy_volume - passive_sell_volume AS net_passive_volume,
                       "close"
                FROM public.grafana_features_view
                WHERE "interval" = '1m'),
                 aggregated_by_interval AS (
                     SELECT time_bucket('15m', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 15 minutes')) AS "timestamp",
                            stock_name, '15m' AS interval_agg,
                            SUM(net_aggressive_volume) AS "Net Inst",
                            SUM(net_passive_volume) AS "Net Iceberg",
                            last("close", timestamp) AS "Price"
                     FROM base_data GROUP BY 1, 2
                     UNION ALL
                     SELECT time_bucket('30m', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
                            stock_name, '30m' AS interval_agg,
                            SUM(net_aggressive_volume) AS "Net Inst",
                            SUM(net_passive_volume) AS "Net Iceberg",
                            last("close", timestamp) AS "Price"
                     FROM base_data GROUP BY 1, 2
                     UNION ALL
                     SELECT time_bucket('1h', timestamp,
                                        (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
                            stock_name, '1h' AS interval_agg,
                            SUM(net_aggressive_volume) AS "Net Inst",
                            SUM(net_passive_volume) AS "Net Iceberg",
                            last("close", timestamp) AS "Price"
                     FROM base_data GROUP BY 1, 2)
            SELECT "timestamp", "stock_name", "interval_agg", "Price", "Net Inst", "Net Iceberg",
                   SUM("Net Inst") OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Inst_Flow_60m",
                   SUM("Net Iceberg") OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Iceberg_Flow_60m"
            FROM aggregated_by_interval;
        """)

    log.info("Database schema setup is complete.")


async def truncate_tables_if_needed(db_pool):
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