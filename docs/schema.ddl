-- Enable TimescaleDB extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =========================
-- live_ticks
-- =========================
CREATE TABLE public.live_ticks
(
    timestamp            timestamp with time zone NOT NULL DEFAULT now(),
    stock_name           text                     NOT NULL,
    last_price           double precision,
    last_traded_quantity integer,
    average_traded_price double precision,
    volume_traded        bigint,
    total_buy_quantity   bigint,
    total_sell_quantity  bigint,
    ohlc_open            double precision,
    ohlc_high            double precision,
    ohlc_low             double precision,
    ohlc_close           double precision,
    change               double precision,
    PRIMARY KEY (timestamp, stock_name)
);

SELECT create_hypertable('live_ticks', 'timestamp', if_not_exists => TRUE);

CREATE INDEX live_ticks_stock_name_timestamp_idx
    ON live_ticks (stock_name, timestamp);



-- =========================
-- live_order_depth
-- =========================
CREATE TABLE public.live_order_depth
(
    timestamp  timestamp with time zone NOT NULL,
    stock_name text                     NOT NULL,
    side       text                     NOT NULL,
    level      integer                  NOT NULL,
    price      double precision,
    quantity   bigint,
    orders     integer,
    PRIMARY KEY (timestamp, stock_name, side, level)
);

SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);

CREATE INDEX live_order_depth_stock_name_timestamp_idx
    ON live_order_depth (stock_name, timestamp);

CREATE INDEX live_order_depth_side_idx
    ON live_order_depth (side);

CREATE INDEX live_order_depth_level_idx
    ON live_order_depth (level);


-- =========================
-- enriched_features
-- =========================
create table public.enriched_features
(
    timestamp        timestamp with time zone not null,
    stock_name       text                     not null,
    interval         text                     not null,
    open             double precision,
    high             double precision,
    low              double precision,
    close            double precision,
    volume           bigint,
    bar_vwap         double precision,
    session_vwap     double precision,
    raw_scores       jsonb,
    instrument_token integer,
    primary key (timestamp, stock_name, interval)
);

SELECT create_hypertable('enriched_features', 'timestamp', if_not_exists => TRUE);


-- =========================
-- large_trade_thresholds_mv
-- =========================
create materialized view public.large_trade_thresholds_mv as
WITH trade_volumes AS (
    SELECT lt.stock_name,
           lt.volume_traded
             - lag(lt.volume_traded, 1, 0::bigint)
               OVER (PARTITION BY lt.stock_name, (lt."timestamp"::date)
                     ORDER BY lt."timestamp") AS tick_volume,
           lt."timestamp"::date as trade_day
    FROM live_ticks lt
    WHERE lt."timestamp" >= (date_trunc('day', now()) - interval '7 days')
),
daily_pxx AS (
    SELECT stock_name,
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
SELECT stock_name,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p95)  AS p95_volume,   -- median of daily p95
       percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p99)  AS p99_volume,   -- median of daily p99
       percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p995) AS p995_volume,  -- median of daily p995
       percentile_cont(0.5) WITHIN GROUP (ORDER BY day_p999) AS p999_volume,  -- median of daily p999
       max(day_max)      AS max_volume,         -- keep track of absolute max
       sum(day_trades)   AS total_trades        -- sum of trades across 7 days
FROM daily_pxx
GROUP BY stock_name;
create unique index large_trade_thresholds_mv_pk
    on public.large_trade_thresholds_mv (stock_name);



-- =========================
-- grafana_features_view
-- =========================
drop view if exists public.grafana_features_view cascade;
create view public.grafana_features_view
            (timestamp, stock_name, interval, open, high, low, close, volume, bar_vwap, session_vwap, instrument_token,
             bar_delta, large_buy_volume, large_sell_volume, passive_buy_volume, passive_sell_volume, cvd_5m, cvd_10m,
             cvd_30m, rsi, mfi, obv, institutional_flow_delta, clv, clv_smoothed, cvd_5m_smoothed, rsi_smoothed,
             mfi_smoothed, inst_flow_delta_smoothed, is_hh, is_hl, is_lh, is_ll, is_inside_bar, is_outside_bar,
             bar_structure, div_price_lvc, div_price_cvd, div_price_obv, div_price_rsi, div_price_mfi, div_price_clv,
             div_lvc_cvd, div_lvc_obv, div_lvc_rsi, div_lvc_mfi)
as
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
       COALESCE((enriched_features.raw_scores ->> 'bar_delta'::text)::bigint, 0::bigint)                             AS bar_delta,
       COALESCE((enriched_features.raw_scores ->> 'large_buy_volume'::text)::bigint,
                0::bigint)                                                                                           AS large_buy_volume,
       COALESCE((enriched_features.raw_scores ->> 'large_sell_volume'::text)::bigint,
                0::bigint)                                                                                           AS large_sell_volume,
       COALESCE((enriched_features.raw_scores ->> 'passive_buy_volume'::text)::bigint,
                0::bigint)                                                                                           AS passive_buy_volume,
       COALESCE((enriched_features.raw_scores ->> 'passive_sell_volume'::text)::bigint,
                0::bigint)                                                                                           AS passive_sell_volume,
       COALESCE((enriched_features.raw_scores ->> 'cvd_5m'::text)::bigint,
                0::bigint)                                                                                           AS cvd_5m,
       COALESCE((enriched_features.raw_scores ->> 'cvd_10m'::text)::bigint,
                0::bigint)                                                                                           AS cvd_10m,
       COALESCE((enriched_features.raw_scores ->> 'cvd_30m'::text)::bigint,
                0::bigint)                                                                                           AS cvd_30m,
       COALESCE((enriched_features.raw_scores ->> 'rsi'::text)::double precision,
                50.0::double precision)                                                                              AS rsi,
       COALESCE((enriched_features.raw_scores ->> 'mfi'::text)::double precision,
                50.0::double precision)                                                                              AS mfi,
       COALESCE((enriched_features.raw_scores ->> 'obv'::text)::bigint,
                0::bigint)                                                                                           AS obv,
       COALESCE((enriched_features.raw_scores ->> 'lvc_delta'::text)::bigint,
                0::bigint)                                                                                           AS institutional_flow_delta,
       COALESCE((enriched_features.raw_scores ->> 'clv'::text)::double precision,
                0.0::double precision)                                                                               AS clv,
       COALESCE((enriched_features.raw_scores ->> 'clv_smoothed'::text)::double precision,
                0.0::double precision)                                                                               AS clv_smoothed,
       COALESCE((enriched_features.raw_scores ->> 'cvd_5m_smoothed'::text)::double precision,
                0.0::double precision)                                                                               AS cvd_5m_smoothed,
       COALESCE((enriched_features.raw_scores ->> 'rsi_smoothed'::text)::double precision,
                50.0::double precision)                                                                              AS rsi_smoothed,
       COALESCE((enriched_features.raw_scores ->> 'mfi_smoothed'::text)::double precision,
                50.0::double precision)                                                                              AS mfi_smoothed,
       COALESCE((enriched_features.raw_scores ->> 'inst_flow_delta_smoothed'::text)::double precision,
                0.0::double precision)                                                                               AS inst_flow_delta_smoothed,
       COALESCE((enriched_features.raw_scores ->> 'HH'::text)::boolean,
                false)                                                                                               AS is_hh,
       COALESCE((enriched_features.raw_scores ->> 'HL'::text)::boolean,
                false)                                                                                               AS is_hl,
       COALESCE((enriched_features.raw_scores ->> 'LH'::text)::boolean,
                false)                                                                                               AS is_lh,
       COALESCE((enriched_features.raw_scores ->> 'LL'::text)::boolean,
                false)                                                                                               AS is_ll,
       COALESCE((enriched_features.raw_scores ->> 'inside'::text)::boolean,
                false)                                                                                               AS is_inside_bar,
       COALESCE((enriched_features.raw_scores ->> 'outside'::text)::boolean,
                false)                                                                                               AS is_outside_bar,
       COALESCE(enriched_features.raw_scores ->> 'structure'::text,
                'init'::text)                                                                                        AS bar_structure,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_lvc'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_lvc,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_cvd'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_cvd,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_obv'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_obv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_rsi'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_rsi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_mfi'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_mfi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_clv'::text)::double precision,
                0.0::double precision)                                                                               AS div_price_clv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_cvd'::text)::double precision,
                0.0::double precision)                                                                               AS div_lvc_cvd,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_obv'::text)::double precision,
                0.0::double precision)                                                                               AS div_lvc_obv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_rsi'::text)::double precision,
                0.0::double precision)                                                                               AS div_lvc_rsi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_mfi'::text)::double precision,
                0.0::double precision)                                                                               AS div_lvc_mfi
FROM enriched_features;


-- =========================
-- market_data_aggregated_view
-- =========================
drop view if exists public.market_data_aggregated_view cascade;
CREATE OR REPLACE VIEW public.market_data_aggregated_view AS

WITH base_data AS (
  -- Get all 1-minute data
  SELECT
    timestamp,
    stock_name,
    large_buy_volume - large_sell_volume AS net_aggressive_volume,
    passive_buy_volume - passive_sell_volume AS net_passive_volume,
    "close"
  FROM
    public.grafana_features_view
  WHERE
    "interval" = '1m'
),
aggregated_by_interval AS (
    -- 15m aggregation
    SELECT
        time_bucket('15m', timestamp, (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 15 minutes')) AS "timestamp",
        stock_name,
        '15m' AS interval_agg,
        SUM(net_aggressive_volume) AS "Net Inst",
        SUM(net_passive_volume) AS "Net Iceberg",
        last("close", timestamp) AS "Price"
    FROM base_data GROUP BY 1, 2

    UNION ALL

    -- 30m aggregation
    SELECT
        time_bucket('30m', timestamp, (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
        stock_name,
        '30m' AS interval_agg,
        SUM(net_aggressive_volume) AS "Net Inst",
        SUM(net_passive_volume) AS "Net Iceberg",
        last("close", timestamp) AS "Price"
    FROM base_data GROUP BY 1, 2

    UNION ALL

    -- 1h aggregation
    SELECT
        time_bucket('1h', timestamp, (timestamp::date AT TIME ZONE 'IST' + interval '9 hours 30 minutes')) AS "timestamp",
        stock_name,
        '1h' AS interval_agg,
        SUM(net_aggressive_volume) AS "Net Inst",
        SUM(net_passive_volume) AS "Net Iceberg",
        last("close", timestamp) AS "Price"
    FROM base_data GROUP BY 1, 2
)
-- Calculate the final rolling sums with shorter aliases
SELECT
  "timestamp",
  "stock_name",
  "interval_agg",
  "Price",
  "Net Inst",
  "Net Iceberg",
  -- Renamed for brevity
  SUM("Net Inst") OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Inst_Flow_60m",
  -- Renamed for brevity
  SUM("Net Iceberg") OVER (PARTITION BY stock_name, interval_agg ORDER BY "timestamp" RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW) AS "Iceberg_Flow_60m"
FROM
  aggregated_by_interval;


CREATE TABLE public.stock_selections (
    symbol TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    last_updated TIMESTAMPTZ DEFAULT now()
);