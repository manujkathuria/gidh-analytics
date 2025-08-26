# Gidh Analytics: Business Analysis Document

## 1. Executive Summary

Gidh Analytics is a real-time market data analysis platform engineered to provide a significant competitive advantage to financial traders and portfolio managers. The system moves beyond conventional, lagging indicators by ingesting granular, tick-level market data to uncover the trading patterns of large institutional players.

The core business value lies in delivering **live, actionable intelligence** on market dynamics. By identifying hidden buy and sell absorption points, tracking cumulative volume delta, and flagging statistically significant trades, the platform offers an evidence-based view of where "smart money" is moving. This empowers users to make more informed, timely decisions by revealing critical moments of market support and resistance as they unfold.

The platform is architected as a data pipeline that can operate in two modes: **real-time** for live market analysis and **backtesting** for historical data processing and strategy validation. The processed insights are made available through a purpose-built database view, designed for visualization in a real-time dashboard like Grafana.

---

## 2. Business Problem & Objectives

### 2.1. Business Problem

In the highly competitive financial markets, retail and smaller institutional traders often struggle to understand the actions of larger, market-moving players. Traditional technical analysis relies on lagging indicators derived from price and volume, which often fail to capture the subtle, real-time dynamics of order flow. This information asymmetry puts them at a disadvantage, as they cannot easily detect:

* **Hidden Intentions:** Large institutions often mask their buy or sell programs using "iceberg orders" to avoid causing significant price shifts.
* **True Momentum:** It is difficult to distinguish between market "noise" and genuine, conviction-driven buying or selling pressure.
* **Incipient Price Moves:** Key support and resistance levels are often established by institutional activity before they become apparent on a price chart.

### 2.2. Business Objectives

* **To Level the Playing Field:** Provide traders with a tool that exposes the hidden activities of institutional investors.
* **To Enhance Decision Making:** Deliver clear, actionable insights that help traders identify high-probability entry and exit points.
* **To Provide a Forward-Looking Edge:** Shift from reactive, lagging indicators to proactive, real-time analysis of order flow.
* **To Enable Strategy Validation:** Offer a robust backtesting engine to process historical data and validate trading strategies based on the platform's proprietary indicators.

---

## 3. Scope

### 3.1. In-Scope Features

* Real-time and historical data processing pipeline for tick and order depth data.
* Calculation of proprietary and standard technical indicators.
* Data storage in a TimescaleDB database for efficient time-series analysis.
* A dedicated database view (`grafana_features_view`) to serve data to visualization platforms.
* Configuration for both `realtime` and `backtesting` operational modes.

### 3.2. Out-of-Scope Features

* **User Interface:** The project delivers the data and a JSON model for a Grafana dashboard but does not include a web application or a standalone UI.
* **Trade Execution:** The platform is purely for analysis and does not connect to any brokerage for executing trades.
* **Data Vending:** The system processes data from a user-provided source (Kite Connect API or local files); it does not source or sell market data.

---

## 4. Functional Requirements (User & System Perspective)

This section details the core features of the Gidh Analytics platform.

### 4.1. Large Trade Detection

* **Description:** The system identifies trades that are significantly larger than the recent average for a given stock, flagging potential market-moving orders.
* **Business Value:** Separates meaningful institutional activity from background noise, allowing traders to focus on impactful trades.
* **Implementation:**
    * In **live mode**, it uses a materialized view that calculates the 99th percentile of trade volumes over the last 7 days.
    * In **backtesting mode**, it dynamically calculates the 99th percentile of trade volume for the 7 days *prior* to the backtest date to avoid lookahead bias.
    * If a pre-calculated threshold is not available, it uses a dynamic fallback based on a rolling window of the last 1000 trades.

### 4.2. Iceberg Order & Absorption Detection

* **Description:** The system analyzes the order book to detect "iceberg orders"—large hidden orders that are partially revealed. It does this by monitoring for rapid refills of the best bid or ask quantity after a trade.
* **Business Value:** Pinpoints exact price levels where large institutions are actively accumulating (buy absorption) or distributing (sell absorption) shares. These levels act as strong indicators of support and resistance.
* **Implementation:**
    * The `FeatureEnricher` module maintains the state of the top of the order book.
    * It tracks a `refill_count` for the best bid and ask. If the quantity at a price level is replenished multiple times (specifically, more than twice) immediately after being traded against, it flags this as absorption (`is_buy_absorption` or `is_sell_absorption`).

### 4.3. Cumulative Volume Delta (CVD)

* **Description:** CVD is a running total of the volume traded on the bid versus the volume traded on the ask. It provides a real-time measure of buying and selling pressure.
* **Business Value:** Offers an immediate, clear view of market momentum. A rising CVD indicates aggressive buying, while a falling CVD signals aggressive selling.
* **Implementation:**
    * The system first determines the "trade sign" (+1 for a buy, -1 for a sell) for each incoming trade.
    * The `BarAggregator` calculates the net volume delta for each bar.
    * It then maintains a running sum of this delta over different lookback periods (5, 10, and 30 minutes).

### 4.4. Price Divergence Detection

* **Description:** The system identifies divergences between price movement and key technical indicators (like RSI, MFI, OBV, and CVD). For example, if the price is making a new high but the CVD is not, it signals that the upward move lacks conviction.
* **Business Value:** Acts as a powerful leading indicator for potential trend reversals or continuations, allowing traders to anticipate price moves.
* **Implementation:**
    * The `PatternDetector` class in `divergence.py` calculates a normalized divergence score.
    * It compares the percentage change in price over a lookback window (5 to 30 minutes) with the normalized change in various indicators.
    * Scores are generated for "Price vs. Feature" (e.g., `div_price_cvd`) and "LVC vs. Feature" (e.g., `div_lvc_obv`) divergences.

### 4.5. Data Pipeline & Processing

* **Description:** The core of the system is an asynchronous data pipeline that ingests, enriches, aggregates, and stores market data.
* **Business Value:** Ensures high-throughput, low-latency processing, which is critical for real-time market analysis.
* **Implementation:**
    * **Data Ingestion:** In `realtime` mode, the `WebSocketClient` connects to the Kite API. In `backtesting` mode, the `FileReader` reads from local CSV files.
    * **Enrichment:** Raw ticks are passed to the `FeatureEnricher` to add trade sign, large trade flags, and absorption flags.
    * **Aggregation:** Enriched ticks are processed by the `BarAggregatorProcessor`, which builds time-based bars (1m, 3m, 5m, etc.) and calculates all features (CVD, RSI, OBV, divergences, etc.).
    * **Storage:** The `db_writer` module performs batch inserts and upserts of the processed data into the `live_ticks`, `live_order_depth`, and `enriched_features` tables in the database.

---

## 5. Data Dictionary

| Term | Field Name | Description | Source Table/View |
| --- | --- | --- | --- |
| **Large Trade** | `is_large_trade` | A boolean flag indicating if a trade's volume exceeds the 99th percentile of recent trades. | `enriched_features` |
| **Buy Absorption** | `is_buy_absorption` | A boolean flag indicating that a hidden buy order is absorbing sell-side volume at a specific price. | `enriched_features` |
| **Sell Absorption** | `is_sell_absorption` | A boolean flag indicating that a hidden sell order is absorbing buy-side volume at a specific price. | `enriched_features` |
| **Cumulative Volume Delta** | `cvd_5m`, `cvd_30m` | The net total of buying vs. selling volume over the last 5 or 30 minutes. | `grafana_features_view` |
| **Institutional Flow Delta** | `institutional_flow_delta` | A cumulative sum of large buy volumes minus large sell volumes, representing net institutional pressure. | `grafana_features_view` |
| **Price vs. CVD Divergence**| `div_price_cvd` | A score from -1 to 1 indicating the degree of divergence between price movement and CVD. | `grafana_features_view` |

---

## 6. Visualization & User Interface

The primary interface for consuming the platform's insights is a **Grafana dashboard**. A pre-configured dashboard JSON file (`gidh_analytics_dashboard.json`) is provided, which includes visualizations for:

* **Price and VWAP:** A candlestick chart showing price action alongside the session's Volume Weighted Average Price.
* **Institutional Volume:** A bar chart displaying the volume of large buy and sell trades.
* **Iceberg Detection:** A time-series chart showing the volume associated with buy and sell absorption events.
* **Aggregated Order Flow:** A table view that summarizes net institutional and iceberg activity over longer intervals (e.g., 15m, 30m, 1h).
* **Price Divergence:** A heatmap that visualizes the divergence scores between price and various indicators, making it easy to spot potential reversals.


---

## 7. Stakeholders

* **Retail Traders:** Individual traders who will use the platform to gain an edge in their daily trading.
* **Portfolio Managers:** Professionals managing larger funds who can use the insights to time their entries and exits more effectively.
* **Quantitative Analysts ("Quants"):** Analysts who can use the backtesting engine and the rich feature set to develop and validate new trading models.
* **Technical Team:** The developers and data engineers responsible for maintaining and extending the platform.