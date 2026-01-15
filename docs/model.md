# 📄 **The 3-Sensor Market Model**

This system converts all market data into three independent, real-time signals:

| Sensor       | Question it answers                                                |
| ------------ | ------------------------------------------------------------------ |
| **PATH**     | Where is price actually going?                                     |
| **COST**     | Are institutions and large money positioned with or against price? |
| **PRESSURE** | Are traders actively pushing price right now?                      |

Each sensor outputs a **normalized ratio in [-1, +1]** and is displayed as a heatmap in Grafana.

---

# 🧭 1) PATH — Price Direction

**Source:** `structure_ratio`

PATH is based purely on **price structure**:

* Higher highs
* Higher lows
* Lower highs
* Lower lows

over the last ~12 bars.

It measures the **walking direction of price**.

### Interpretation

| structure_ratio | Meaning          |
| --------------- | ---------------- |
| > +0.35         | Strong uptrend   |
| +0.15 to +0.35  | Weak uptrend     |
| −0.15 to +0.15  | Sideways / chop  |
| −0.35 to −0.15  | Weak downtrend   |
| < −0.35         | Strong downtrend |

PATH never uses volume, VWAP, CLV, or indicators.
It only uses **price movement**.

---

# 💰 2) COST — Institutional Positioning

**Source:**

```
cost_ratio = average(price_vs_vwap, price_vs_obv)
```

These two divergence scores are already normalized to [-1, +1].

### What they mean

| Component       | Measures                                          |
| --------------- | ------------------------------------------------- |
| `price_vs_vwap` | Is price above or below institutional cost basis? |
| `price_vs_obv`  | Is volume accumulating or distributing?           |

Together they answer:

> “Is big money winning or losing?”

### Interpretation

| cost_ratio   | Meaning                         |
| ------------ | ------------------------------- |
| > +0.3       | Institutions positioned bullish |
| −0.3 to +0.3 | Neutral                         |
| < −0.3       | Institutions positioned bearish |

If COST is red, rallies tend to fail.
If COST is green, dips tend to get bought.

---

# 💥 3) PRESSURE — Active Trading Force

**Source:**

```
pressure_ratio = price_vs_clv
```

CLV divergence measures:

> “Are candles closing near highs or near lows relative to price movement?”

This is the cleanest signal of **who is hitting the tape right now**.

### Interpretation

| pressure_ratio | Meaning                  |
| -------------- | ------------------------ |
| > +0.3         | Buyers in control        |
| −0.3 to +0.3   | No pressure / exhaustion |
| < −0.3         | Sellers in control       |

This is a **fast** signal.

---

# How the system is used

Traders do not read indicators.
They read these three lights:

| PATH | COST | PRESSURE | Action                          |
| ---- | ---- | -------- | ------------------------------- |
| 🟥   | 🟥   | 🟥       | Strong short                    |
| 🟥   | 🟥   | 🟨       | Wait for bounce to short        |
| 🟥   | 🟨   | 🟩       | Short covering (do NOT go long) |
| 🟩   | 🟩   | 🟩       | Strong long                     |
| 🟩   | 🟩   | 🟥       | Distribution → wait             |
| 🟨   | any  | any      | No trade                        |

---

# Why this works

Each sensor is independent:

| Sensor   | Speed  | What it sees              |
| -------- | ------ | ------------------------- |
| PATH     | Slow   | Price path                |
| COST     | Medium | Institutional positioning |
| PRESSURE | Fast   | Order-flow control        |

This prevents:

* Buying inside downtrends
* Shorting inside uptrends
* Chasing fake moves

---

# Implementation Status

| Sensor   | Field                                                                                        |
| -------- | -------------------------------------------------------------------------------------------- |
| PATH     | `raw_scores['structure_ratio']`                                                              |
| COST     | `(raw_scores['divergence']['price_vs_vwap'] + raw_scores['divergence']['price_vs_obv']) / 2` |
| PRESSURE | `raw_scores['divergence']['price_vs_clv']`                                                   |

These already exist in Postgres and can be directly consumed by Grafana.

