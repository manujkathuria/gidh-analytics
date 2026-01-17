# 🦅 Gidh Analytics: Institutional Order-Flow Engine

**Gidh Analytics** is a high-frequency asynchronous data pipeline and strategy engine designed to identify institutional footprints in the Indian stock market (NSE). By analyzing tick-level data and order-book depth, the system identifies "Smart Money" activity through price structure, institutional cost basis, and tape pressure.

The system is currently operating as an **Alert & Signal Logging Engine**, generating high-probability entry/exit logs based on stock-specific optimized parameters.

---

## 🚀 Core Features

### 1. The 3-Sensor Market Model

The engine converts complex market data into three normalized signals ([-1, +1]):

* **PATH (Structure):** Measures the walking direction of price (Higher Highs/Lower Lows) over a lookback window.
* **COST (Institutional):** Tracks if price is trading above/below the institutional cost basis (VWAP) and volume accumulation (OBV).
* **PRESSURE (Tape):** Analyzes where candles close relative to their range (CLV) to detect active "hitting of the tape".

### 2. Stock-Specific Optimization

Unlike traditional systems with hardcoded rules, Gidh uses a **5D Brute-Force Optimizer** (`scripts/optimize_parameters.py`) to find the best configuration for every individual stock. These parameters are stored in the database and injected into the engine at runtime:

* **Regime & Timing Intervals:** Different stocks may trend better on 15m vs 10m.
* **R (Regime Threshold):** Sensitivity for trend confirmation.
* **C (Chop Threshold):** Tolerance for sideways "noise" before exiting.
* **T (Timing Threshold):** Depth of the pullback required before entry.

### 3. Micro-Flow Detection

* **Iceberg/Absorption:** Detects hidden orders by monitoring rapid refills of the best bid/ask quantity.
* **Large Trade Detection:** Flags trades exceeding the 99th percentile of volume to isolate institutional moves.
* **Divergence Engine:** Identifies when price moves are not supported by volume or internal pressure.

---

## 🛠 Technical Stack

* **Language:** Python 3.10+ (Asynchronous/Event-Driven).
* **Database:** TimescaleDB (PostgreSQL) for time-series optimization.
* **Data Source:** Kite Connect API (WebSocket for Real-time, CSV for Backtesting).
* **Visualization:** Grafana (Custom Heatmaps and Candle charts).

---

## 📂 Project Structure

| Directory | Purpose |
| --- | --- |
| `core/` | The engine room: `strategy_engine.py`, `bar_aggregator.py`, and `feature_enricher.py`. |
| `analytics/` | Pre-market selection tools and macro trend classifiers. |
| `common/` | Global configurations, models, and shared instrument maps. |
| `scripts/` | Optimization tools, database backups, and maintenance tasks. |
| `docs/` | Technical onboarding and the 3-Sensor Model documentation. |

---

## 🚦 Current Operational Status

The system is currently in **Phase 1: Signal & Alert Logging**.

* **Automated Entry/Exit Logs:** Signals are generated in real-time and persisted to the `live_signals` table.
* **Performance Tracking:** Every exit log includes calculated PnL% based on the optimized parameters.
* **No Execution:** The system provides the intelligence; trade execution (Algo Trading) is a future module.

---

## 🔧 Getting Started

### 1. Setup Environment

```bash
cp .env.example .env
# Fill in KITE_API_KEY, KITE_ACCESS_TOKEN, and DB credentials
pip install -r requirements.txt

```

### 2. Run Optimization

Before running the live engine, use historical data to find the best parameters for your watchlist:

```bash
python scripts/optimize_parameters.py

```

*This script will populate the `stock_strategy_configs` table in your database.*

### 3. Launch the Pipeline

```bash
./scripts/master_script.sh start

```

*This starts the WebSocket ingestion, enrichment, and the Strategy Engine in the background.*

---

## 📊 Monitoring

View the live "3-Light" signals (PATH, COST, PRESSURE) by importing `docs/gidh_analytics_dashboard.json` into your Grafana instance.

