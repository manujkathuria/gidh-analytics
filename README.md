# Gidh Analytics: Real-Time Market Intelligence Platform

## Business Overview

In today's fast-paced financial markets, gaining a competitive edge requires seeing beyond simple price movements. **Gidh Analytics** is an advanced, real-time data analysis platform designed to uncover the hidden intentions of institutional traders. By processing market data at a granular level, our system identifies subtle patterns of buying and selling pressure that are invisible to the average observer.

This platform empowers traders and portfolio managers to make more informed decisions by providing a clear, evidence-based view of market dynamics. We move beyond lagging indicators to deliver live, actionable intelligence on where the "smart money" is moving, revealing critical moments of market support and resistance as they happen.

---

## Key Features & Business Value

Our platform enriches raw market data with a suite of proprietary indicators designed to reveal institutional trading activity:

- **📈 Large Trade Detection (P90 Volume):** We instantly flag trades that are in the top 10% of recent activity for a specific stock. This allows you to see significant market-moving orders in real-time, separating meaningful trades from market noise.
    
- **🧊 Absorption & Iceberg Order Detection:** Our system detects hidden buy and sell orders ("iceberg orders") by analyzing order book refills. Identifying these areas of **buy and sell absorption** reveals strong levels of support and resistance where large institutions are actively accumulating or distributing shares, often signaling a potential price turn.
    
- **📊 Cumulative Volume Delta (CVD):** We provide a running tally of buying vs. selling pressure over multiple timeframes (5, 10, and 30 minutes). A rising CVD indicates aggressive buying, while a falling CVD shows aggressive selling, offering a clear, immediate view of market momentum.
    
- **🚀 Advanced Momentum Indicators (RSI, MFI, OBV):** The platform calculates standard momentum indicators (RSI, MFI) and volume-based indicators (OBV) on clean, aggregated data bars, providing a reliable, traditional context to our more advanced features.
    

---

## How It Works: A High-Level View

Our system operates as a sophisticated, automated data pipeline that transforms raw information into high-value intelligence.

1. **Data Ingestion:** The platform consumes real-time tick-by-tick market data, including trades and order book depth.
    
2. **Feature Enrichment:** In this core step, every tick is analyzed to calculate our proprietary features, such as identifying large trades and detecting absorption events.
    
3. **Bar Aggregation:** The enriched ticks are then instantly aggregated into time-based bars (e.g., 1-minute, 5-minute, 15-minute). During this stage, indicators like CVD and RSI are calculated.
    
4. **Real-Time Visualization:** The final, feature-rich bar data is streamed directly into a database and visualized in a real-time Grafana dashboard, providing an intuitive and interactive user experience.
    

---

## The Business Value

Gidh Analytics provides a significant competitive advantage by:

- **Increasing Conviction:** Make trading decisions with higher confidence by seeing where institutional players are active.
    
- **Improving Entry and Exit Timing:** Identify precise levels of support and resistance to optimize trade entries and exits.
    
- **Reducing Risk:** Gain early warnings of potential trend reversals when you see large players absorbing buying or selling pressure.
    
- **Saving Time:** The platform fully automates the complex analysis of order flow, freeing up traders to focus on strategy and execution.
    

---

## Getting Started

Accessing the intelligence from the Gidh Analytics platform is simple:

1. Open the pre-configured **Grafana Dashboard**.
    
2. Select the stock and time interval you wish to analyze from the dashboard's dropdown menus.
    
3. The dashboard will display all the calculated features in real-time, with charts visualizing key metrics like Cumulative Volume Delta, Large Trade Volume, and RSI.
    

There is no complex setup required. The dashboard is designed to be the primary interface for consuming this high-value data.


## How to Backtest

The platform includes a powerful backtesting engine that allows you to process historical data and generate features as if it were happening in real-time.

### Step 1: Configure Your Environment

1.  **Create a `.env` file:** In the project's root directory, make a copy of `.env.example` and rename it to `.env`.

2.  **Edit the `.env` file:**
    * Set `PIPELINE_MODE=backtesting`.
    * Set `TRUNCATE_TABLES_ON_BACKTEST=true` to ensure each run starts with a clean database.
    * Provide the correct connection details for your `DB_*` variables.
    * Set `BACKTEST_DATA_DIRECTORY` to the absolute path of your historical data folder.
    * Set `BACKTEST_DATE` to the specific date you want to process (e.g., `2024-01-25`).

### Step 2: Structure Your Data Directory

The backtesting engine expects your historical data to be organized in a specific way. Inside your `BACKTEST_DATA_DIRECTORY`, create a folder for each day of data (e.g., a folder named `2024-01-25`).

Inside each daily folder, you must have two sub-folders:
* `live_ticks`
* `live_order_depth`

The data files must be placed inside these folders with the following naming convention:
* `.../2024-01-25/live_ticks/live_ticks_DIXON.csv`
* `.../2024-01-25/live_order_depth/live_order_depth_DIXON.csv`

The system will automatically discover and process the data for the instruments defined in the configuration.

### Step 3: Run the Backtest

Once your `.env` file is configured and your data is structured correctly, simply run the main application from your terminal:

```bash
python main.py