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
CREATE TABLE public.enriched_features
(
    timestamp    timestamp with time zone NOT NULL,
    stock_name   text                     NOT NULL,
    interval     text                     NOT NULL,
    open         double precision,
    high         double precision,
    low          double precision,
    close        double precision,
    volume       bigint,
    bar_vwap     double precision,
    session_vwap double precision,
    raw_scores   jsonb
);


create view public.grafana_features_view
            (timestamp, stock_name, interval, open, high, low, close, volume, bar_vwap, session_vwap, instrument_token,
             bar_delta, large_buy_volume, large_sell_volume, passive_buy_volume, passive_sell_volume, cvd_30m, rsi, mfi,
             obv, lvc_delta, clv, div_price_lvc, div_price_cvd, div_price_obv, div_price_rsi, div_price_mfi,
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
       COALESCE((enriched_features.raw_scores ->> 'bar_delta'::text)::bigint, 0::bigint)                  AS bar_delta,
       COALESCE((enriched_features.raw_scores ->> 'large_buy_volume'::text)::bigint,
                0::bigint)                                                                                AS large_buy_volume,
       COALESCE((enriched_features.raw_scores ->> 'large_sell_volume'::text)::bigint,
                0::bigint)                                                                                AS large_sell_volume,
       COALESCE((enriched_features.raw_scores ->> 'passive_buy_volume'::text)::bigint,
                0::bigint)                                                                                AS passive_buy_volume,
       COALESCE((enriched_features.raw_scores ->> 'passive_sell_volume'::text)::bigint,
                0::bigint)                                                                                AS passive_sell_volume,
       COALESCE((enriched_features.raw_scores ->> 'cvd_30m'::text)::bigint, 0::bigint)                    AS cvd_30m,
       COALESCE((enriched_features.raw_scores ->> 'rsi'::text)::double precision, 50.0::double precision) AS rsi,
       COALESCE((enriched_features.raw_scores ->> 'mfi'::text)::double precision, 50.0::double precision) AS mfi,
       COALESCE((enriched_features.raw_scores ->> 'obv'::text)::bigint, 0::bigint)                        AS obv,
       COALESCE((enriched_features.raw_scores ->> 'lvc_delta'::text)::bigint, 0::bigint)                  AS lvc_delta,
       COALESCE((enriched_features.raw_scores ->> 'clv'::text)::double precision, 0.0::double precision)  AS clv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_lvc'::text)::double precision,
                0.0::double precision)                                                                    AS div_price_lvc,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_cvd'::text)::double precision,
                0.0::double precision)                                                                    AS div_price_cvd,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_obv'::text)::double precision,
                0.0::double precision)                                                                    AS div_price_obv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_rsi'::text)::double precision,
                0.0::double precision)                                                                    AS div_price_rsi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'price_vs_mfi'::text)::double precision,
                0.0::double precision)                                                                    AS div_price_mfi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_cvd'::text)::double precision,
                0.0::double precision)                                                                    AS div_lvc_cvd,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_obv'::text)::double precision,
                0.0::double precision)                                                                    AS div_lvc_obv,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_rsi'::text)::double precision,
                0.0::double precision)                                                                    AS div_lvc_rsi,
       COALESCE(((enriched_features.raw_scores -> 'divergence'::text) ->> 'lvc_vs_mfi'::text)::double precision,
                0.0::double precision)                                                                    AS div_lvc_mfi
FROM enriched_features;


create materialized view public.large_trade_thresholds_mv as
WITH trade_volumes AS (SELECT lt.stock_name,
                              lt.volume_traded - lag(lt.volume_traded, 1, 0::bigint)
                                                 OVER (PARTITION BY lt.stock_name, (lt."timestamp"::date) ORDER BY lt."timestamp") AS tick_volume
                       FROM live_ticks lt
                       WHERE lt."timestamp" >= (date_trunc('day'::text, now()) - '7 days'::interval))
SELECT tv.stock_name,
       percentile_cont(0.95::double precision) WITHIN GROUP (ORDER BY (tv.tick_volume::double precision))  AS p95_volume,
       percentile_cont(0.99::double precision)
       WITHIN GROUP (ORDER BY (tv.tick_volume::double precision))                                          AS p99_volume,
       percentile_cont(0.995::double precision)
       WITHIN GROUP (ORDER BY (tv.tick_volume::double precision))                                          AS p995_volume,
       percentile_cont(0.999::double precision)
       WITHIN GROUP (ORDER BY (tv.tick_volume::double precision))                                          AS p999_volume,
       max(tv.tick_volume)                                                                                 AS max_volume,
       count(*)                                                                                            AS total_trades
FROM trade_volumes tv
WHERE tv.tick_volume > 0
GROUP BY tv.stock_name;


