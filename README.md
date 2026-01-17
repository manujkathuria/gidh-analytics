# 🦅 Gidh Analytics: Institutional Order-Flow Engine

**Gidh Analytics** is a high-frequency asynchronous data pipeline designed to identify institutional footprints in the Indian stock market (NSE). By analyzing tick-level data and order-book depth, the system identifies "Smart Money" activity through price structure and institutional cost basis.

The system currently operates as a **Signal & Alert Logging Engine**, generating high-probability entry/exit logs based on a standardized conviction model.

---

## 🚦 The 2-Sensor Market Model

The engine converts complex market data into two primary normalized signals (ranging from -1 to +1), which are visualized as heatmaps in Grafana to provide immediate market context:

| Sensor | Speed | Focus | Question Answered |
| --- | --- | --- | --- |
| **PATH** | Slow | Price Structure | Where is the price actually walking? (HH/HL/LH/LL) |
| **COST** | Medium | Institutional Intent | Is big money positioned with or against the price? (VWAP/OBV) |

---

## 🛠 Core Features

### 1. The Alert Engine (3-Bar Handshake)

The system uses a state-machine logic to generate conviction-based signals. It avoids "chasing" noise by requiring a **3-bar handshake** for the core sensors:

* **Persistence:** **COST** (Intent) and **PATH** (Structure) must hold their direction for 3 consecutive bars to establish a regime.
* **Entry:** Fired when a 3-bar regime alignment meets a 1-bar **ACCEPTANCE** (Price breaking the recent 5-bar range).
* **Exit:** Triggered immediately if institutional intent fades or the price structure flips, ensuring capital protection.

### 2. Divergence Engine

Detects "hidden" moves where price and volume metrics are disconnected to identify potential reversals or trend strength:

* **Price vs. OBV:** Identifies accumulation or distribution through volume delta.
* **Price vs. VWAP:** Determines if the price is deviating significantly from the institutional cost basis.

### 3. Asynchronous Data Pipeline

* **Real-time & Backtest Modes:** Built with `asyncio` to handle live WebSocket feeds or high-speed historical CSV replays through the exact same logic.
* **Micro-Flow Enrichment:** Processes every tick to identify **Trade Sign** and **Large Trade Detection** (identifying trades exceeding the 99th percentile of volume).

---

## 📊 Technical Stack

* **Language:** Python 3.10+ (Asynchronous/Event-Driven).
* **Database:** TimescaleDB (PostgreSQL) for optimized time-series storage and signal persistence.
* **Data Source:** Kite Connect API (WebSockets for live ingestion).
* **Visualization:** **Grafana** (Custom Heatmaps, Price & VWAP charts, and automated Alert Annotations).

---

## 📈 Operational Workflow

1. **Ingestion:** Real-time tick data is captured via WebSocket and queued for processing.
2. **Enrichment:** Ticks are enriched with trade sign and institutional flags.
3. **Aggregation:** Data is binned into intervals (1m to 15m), calculating indicators like RSI, MFI, and CVD.
4. **Alerting:** The Alert Engine monitors for 3-sensor alignment and logs entry/exit signals to the `live_signals` table.
5. **Visualization:** Signals are displayed in Grafana with vertical annotations directly on the Price & VWAP charts.

---

## 🚀 Getting Started

### 1. Environment Setup

```bash
cp .env.example .env
# Configure DB credentials and KITE API keys
pip install -r requirements.txt

```

### 2. Testing & Development

Install the test dependencies to run the suite of unit and integration tests:

```bash
pip install -r requirements-test.txt
pytest

```

*The test suite uses `pytest` and `pytest-asyncio` to validate core components like the `BarAggregator` and `FeatureEnricher`.*

### 3. Launch Pipeline

```bash
./scripts/master_script.sh start

```

---

## 🦅 Monitoring

Import `docs/gidh_analytics_dashboard.json` into your Grafana instance to view the live **"2-Light"** system. All signals are logged to the `live_signals` table for real-time monitoring and post-trade analysis.