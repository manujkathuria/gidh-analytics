from service.logger import log

async def setup_schema(db_pool):
    """Creates the necessary tables and hypertables in TimescaleDB if they don't exist."""
    log.info("Checking and creating database tables if necessary...")
    async with db_pool.acquire() as connection:
        # Enable TimescaleDB extension
        await connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        # Create live_ticks table
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_ticks (
                timestamp            TIMESTAMPTZ NOT NULL,
                stock_name           TEXT NOT NULL,
                last_price           DOUBLE PRECISION,
                last_traded_quantity INTEGER,
                average_traded_price DOUBLE PRECISION,
                volume_traded        BIGINT,
                total_buy_quantity   BIGINT,
                total_sell_quantity  BIGINT,
                ohlc_open            DOUBLE PRECISION,
                ohlc_high            DOUBLE PRECISION,
                ohlc_low             DOUBLE PRECISION,
                ohlc_close           DOUBLE PRECISION,
                change               DOUBLE PRECISION,
                instrument_token     INTEGER,
                PRIMARY KEY (timestamp, stock_name)
            );
        """)
        await connection.execute("SELECT create_hypertable('live_ticks', 'timestamp', if_not_exists => TRUE);")
        await connection.execute("""
            CREATE INDEX IF NOT EXISTS live_ticks_stock_name_timestamp_idx
            ON live_ticks (stock_name, timestamp DESC);
        """)

        # Create live_order_depth table
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.live_order_depth (
                timestamp  TIMESTAMPTZ NOT NULL,
                stock_name TEXT NOT NULL,
                side       TEXT NOT NULL,
                level      INTEGER NOT NULL,
                price      DOUBLE PRECISION,
                quantity   BIGINT,
                orders     INTEGER,
                instrument_token INTEGER,
                PRIMARY KEY (timestamp, stock_name, side, level)
            );
        """)
        await connection.execute("SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);")
        await connection.execute("""
            CREATE INDEX IF NOT EXISTS live_order_depth_stock_name_timestamp_idx
            ON live_order_depth (stock_name, timestamp DESC);
        """)
        await connection.execute("""
            CREATE INDEX IF NOT EXISTS live_order_depth_side_idx ON live_order_depth (side);
        """)
        await connection.execute("""
            CREATE INDEX IF NOT EXISTS live_order_depth_level_idx ON live_order_depth (level);
        """)

        # Create enriched_features table
        await connection.execute("""
            CREATE TABLE IF NOT EXISTS public.enriched_features (
                timestamp    TIMESTAMPTZ NOT NULL,
                stock_name   TEXT NOT NULL,
                interval     TEXT NOT NULL,
                open         DOUBLE PRECISION,
                high         DOUBLE PRECISION,
                low          DOUBLE PRECISION,
                close        DOUBLE PRECISION,
                volume       BIGINT,
                bar_vwap     DOUBLE PRECISION,
                session_vwap DOUBLE PRECISION,
                raw_scores   JSONB,
                instrument_token INTEGER
            );
        """)
        # This table might also benefit from being a hypertable if it grows large
        await connection.execute("SELECT create_hypertable('enriched_features', 'timestamp', if_not_exists => TRUE);")
        await connection.execute("""
            CREATE INDEX IF NOT EXISTS enriched_features_stock_interval_timestamp_idx
            ON enriched_features (stock_name, interval, timestamp DESC);
        """)

    log.info("Database schema setup is complete.")
