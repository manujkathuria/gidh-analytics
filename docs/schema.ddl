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