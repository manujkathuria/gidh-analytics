# Gidh Analytics: Real-Time Market Intelligence Platform

## 1\. Business Overview

In today's fast-paced financial markets, gaining a competitive edge requires seeing beyond simple price movements. **Gidh Analytics** is an advanced, real-time data analysis platform designed to uncover the hidden intentions of institutional traders. By processing market data at a granular level, our system identifies subtle patterns of buying and selling pressure that are invisible to the average observer.

This platform empowers traders and portfolio managers to make more informed decisions by providing a clear, evidence-based view of market dynamics. We move beyond lagging indicators to deliver live, actionable intelligence on where the "smart money" is moving, revealing critical moments of market support and resistance as they happen.

-----

## 2\. Key Features & Business Value

Our platform enriches raw market data with a suite of proprietary indicators designed to reveal institutional trading activity:

  * **📈 Large Trade Detection (P90 Volume):** We instantly flag trades that are in the top 10% of recent activity for a specific stock. This allows you to see significant market-moving orders in real-time, separating meaningful trades from market noise.
  * **🧊 Absorption & Iceberg Order Detection:** Our system detects hidden buy and sell orders ("iceberg orders") by analyzing order book refills. Identifying these areas of **buy and sell absorption** reveals strong levels of support and resistance where large institutions are actively accumulating or distributing shares, often signaling a potential price turn.
  * **📊 Cumulative Volume Delta (CVD):** We provide a running tally of buying vs. selling pressure over multiple timeframes (5, 10, and 30 minutes). A rising CVD indicates aggressive buying, while a falling CVD shows aggressive selling, offering a clear, immediate view of market momentum.
  * **🚀 Advanced Momentum Indicators (RSI, MFI, OBV):** The platform calculates standard momentum indicators (RSI, MFI) and volume-based indicators (OBV) on clean, aggregated data bars, providing a reliable, traditional context to our more advanced features.

-----

## 3\. Setup and Installation

Follow these steps to set up and run the Gidh Analytics platform on a new system.

### Step 1: Create and Activate a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.

```bash
# Navigate to your project directory
cd /path/to/gidh-analytics

# Create a virtual environment named '.venv'
python3 -m venv .venv

# Activate the virtual environment
# On macOS and Linux:
source .venv/bin/activate
# On Windows:
# .\.venv\Scripts\activate
```

Your terminal prompt should now show `(.venv)` at the beginning, indicating that the virtual environment is active.

### Step 2: Install Required Packages

Use the `pip` package manager to install all the dependencies listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```

### Step 3: Configure Your Environment

1.  **Create a `.env` file:** In the project's root directory, make a copy of `.env.example` and rename it to `.env`.

2.  **Edit the `.env` file:**

      * Set the `PIPELINE_MODE` (`backtesting` or `realtime`).
      * Provide the correct connection details for your `DB_*` variables.
      * If backtesting, configure the `BACKTEST_*` variables.

### Step 4: Run the Application

With your environment set up and dependencies installed, you can now run the main application:

```bash
python main.py
```

-----

## 4\. How to Backtest

The platform includes a powerful backtesting engine that allows you to process historical data and generate features as if it were happening in real-time.

### Step 1: Configure Your Environment for Backtesting

In your `.env` file, ensure the following are set correctly:

  * `PIPELINE_MODE=backtesting`
  * `TRUNCATE_TABLES_ON_BACKTEST=true` (Recommended to ensure each run starts with a clean database)
  * `BACKTEST_DATA_DIRECTORY=/path/to/your/data`
  * `BACKTEST_DATE=YYYY-MM-DD` (The specific date you want to process)

### Step 2: Structure Your Data Directory

The backtesting engine expects your historical data to be organized in a specific way. Inside your `BACKTEST_DATA_DIRECTORY`, create a folder for each day of data (e.g., a folder named `2024-01-25`).

Inside each daily folder, you must have two sub-folders:

  * `live_ticks`
  * `live_order_depth`

The data files must be placed inside these folders with the following naming convention:

  * `.../2024-01-25/live_ticks/live_ticks_DIXON.csv`
  * `.../2024-01-25/live_order_depth/live_order_depth_DIXON.csv`

The system will automatically discover and process the data for the instruments defined in `service/parameters.py`.

### Step 3: Run the Backtest

Once your `.env` file is configured and your data is structured correctly, simply run the main application from your terminal:

```bash
python main.py
```

The application will read the configuration, connect to the database, find your historical data, and begin processing. You can monitor its progress through the console logs and see the results populate the `enriched_features` table and the `grafana_features_view` in your database.

-----

## 5\. Real-Time Dashboard

The primary way to consume the intelligence generated by this platform is through a real-time dashboard (e.g., in Grafana). The database view `public.grafana_features_view` is specifically designed for this purpose, providing clean, queryable columns for all calculated features.