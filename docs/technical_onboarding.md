## GIDH Analytics: Technical Onboarding Guide

### 1. High-Level System Architecture

GIDH Analytics is an asynchronous, event-driven data pipeline written in Python. Its primary purpose is to ingest raw, tick-level financial market data, enrich it with sophisticated order-flow analytics, and store the results in a time-series database for real-time visualization and analysis.

The system is designed to operate in two distinct modes:
* **Real-time Mode**: Connects to a live data feed via WebSocket to process market data as it happens.
* **Backtesting Mode**: Reads historical data from local CSV files to process past events and validate strategies.

The core of the application is the `DataPipeline` class, which orchestrates the flow of data through a series of specialized components using `asyncio` queues.

### 2. Data Flow

The data processing sequence is designed for high throughput and low latency.

1.  **Ingestion**:
    * In `realtime` mode, the `WebSocketClient` connects to the KiteTicker API, subscribes to a predefined list of instruments, and places incoming tick data into the `raw_tick_queue`.
    * In `backtesting` mode, the `FileReader` reads historical tick and order depth data from CSVs, merges them chronologically, and streams them into the `raw_tick_queue` to simulate a live feed.

2.  **Enrichment**:
    * The `processor_and_writer_coroutine` in `DataPipeline` consumes raw ticks from the queue.
    * Each raw tick is passed to the `FeatureEnricher`, which calculates crucial micro-level features like **Trade Sign** (classifying if a trade was a buy or sell), **Large Trade Detection**, and **Absorption/Iceberg Order Detection**. This creates an `EnrichedTick` object.

3.  **Aggregation**:
    * The `EnrichedTick` is then sent to the `BarAggregatorProcessor`.
    * This component maintains separate `BarAggregator` instances for different time intervals (1m, 3m, 5m, etc.). It updates the currently building bar for each interval (Open, High, Low, Close, Volume, VWAP).
    * Crucially, it also calculates complex, bar-level indicators like **RSI, MFI, OBV, Cumulative Volume Delta (CVD)**, and **Price-Indicator Divergences**.

4.  **Storage**:
    * The pipeline collects the processed data into batches.
    * The `db_writer` module handles efficient bulk insertion of raw ticks (`live_ticks`) and order depth (`live_order_depth`) and performs "upserts" for the aggregated bar data (`enriched_features`) to ensure the latest calculations are always reflected.

### 3. Core Logic and Modules

* **`service/pipeline.py`**: The central coordinator. It initializes all components, manages the main processing loop, and handles graceful shutdown.
* **`service/feature_enricher.py`**: This stateful class is vital for order flow analysis. It maintains the last known state of the order book for each instrument to detect absorption (a key sign of hidden orders) and determines the trade initiator (buy or sell).
* **`service/bar_aggregator.py`**: Contains the logic for time-based data aggregation. It calculates a variety of standard indicators (RSI, MFI) and custom ones like Cumulative Volume Delta. It also tracks market structure (Higher Highs, Lower Lows, etc.).
* **`service/divergence.py`**: This module implements the logic for detecting divergences, a powerful leading indicator. It calculates a normalized score by comparing the rate of change in price against the rate of change in key indicators like CVD and LVC (Large Volume Count).
* **`service/db_reader.py`**: Handles mode-aware data fetching from the database. It contains separate logic to fetch large trade thresholds for live trading (from a materialized view) versus backtesting (calculated dynamically to prevent lookahead bias).
* **`service/config.py`**: Centralizes all configuration management. It loads settings from environment variables (`.env` file) and validates them on startup, ensuring the application has all necessary parameters to run.

### 4. Database Schema

The system uses a TimescaleDB (a PostgreSQL extension) database for storing time-series data efficiently.

* **`live_ticks`**: Stores the raw tick-by-tick data.
* **`live_order_depth`**: Stores snapshots of the order book depth.
* **`enriched_features`**: This is the most important table. It stores the aggregated bar data, including OHLCV, VWAP, and a `JSONB` column named `raw_scores` that contains all the calculated indicators (CVD, RSI, divergence scores, etc.).
* **`grafana_features_view`**: A database VIEW that unnests the `JSONB` data from `enriched_features` into individual columns. This view is the primary data source for the Grafana dashboard, making it easy to query and visualize the calculated features.
* **`large_trade_thresholds_mv`**: A MATERIALIZED VIEW that pre-calculates the 99th percentile of trade volume over the last 7 days. This is used in live mode for efficient large trade detection.

### 5. Getting Started

1.  **Environment Setup**:
    * Copy the `.env.example` file to `.env` and fill in your database credentials and API keys.
    * Create a Python virtual environment and install the required packages using `pip install -r requirements.txt`.

2.  **Running the Application**:
    * The primary entry point is `main.py`.
    * Use the `scripts/master_script.sh` for lifecycle management:
        * `./master_script.sh start`: Starts the application in the background.
        * `./master_script.sh stop`: Gracefully stops the application.
        * `./master_script.sh login`: Interactively runs a script to generate a new Kite access token.

3.  **Configuring the Mode**:
    * Set the `PIPELINE_MODE` in the `.env` file to either `realtime` or `backtesting`.
    * For `backtesting`, ensure `BACKTEST_DATA_DIRECTORY` and `BACKTEST_DATE` are set correctly.

This document provides a foundational understanding of the GIDH Analytics system. For deeper insights, developers should review the source code of the core modules mentioned above.