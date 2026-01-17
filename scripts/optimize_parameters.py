import asyncio
import asyncpg
import pandas as pd
from datetime import time
from common import config
from common.parameters import INSTRUMENT_MAP
from common.logger import log

# ========== 1. 5-D OPTIMIZATION GRID ==========
REGIME_INTERVALS = ["5m", "10m", "15m"]
TIMING_INTERVALS = ["3m", "5m", "10m"]
REGIME_RANGE = [0.20, 0.25, 0.30]
CHOP_RANGE = [0.05, 0.10, 0.15]
TIMING_RANGE = [0.30, 0.40, 0.50]
STOP_LOSS = 0.005  # 0.5% Hard Stop

START_TIME = time(10, 0, 0)  # IST
END_TIME = time(14, 0, 0)  # IST


# ========== 2. DUAL-INTERVAL DATA LOADER ==========
async def load_dual_data(conn, stock, r_int, t_int):
    # Using 'timestamp' explicitly to match your DB schema
    qR = f"""
    SELECT timestamp, structure_ratio AS path, (div_price_vwap + div_price_obv)/2 AS cost
    FROM grafana_features_view
    WHERE stock_name=$1 AND interval='{r_int}'
      AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time BETWEEN $2 AND $3
    ORDER BY timestamp
    """
    qT = f"""
    SELECT timestamp, close AS price, structure_ratio AS path, (div_price_vwap + div_price_obv)/2 AS cost,
           div_price_clv AS clv, div_price_obv AS obv, div_price_vwap AS vwap
    FROM grafana_features_view
    WHERE stock_name=$1 AND interval='{t_int}'
      AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time BETWEEN $2 AND $3
    ORDER BY timestamp
    """

    rowsR = await conn.fetch(qR, stock, START_TIME, END_TIME)
    rowsT = await conn.fetch(qT, stock, START_TIME, END_TIME)

    dfR = pd.DataFrame(rowsR, columns=["timestamp", "path", "cost"])
    dfT = pd.DataFrame(rowsT, columns=["timestamp", "price", "path", "cost", "clv", "obv", "vwap"])

    return dfR, dfT


# ========== 3. ENGINE STATE-MACHINE SIMULATOR ==========
def simulate(dfR, dfT, R, C, T):
    regime = "NO"
    pos, entry, stop, scaled = None, 0, 0, False
    pnl, wins, trades, r_idx = 0, 0, 0, 0

    # Convert to list of dictionaries for 100% reliable attribute-free access
    data_T = dfT.to_dict('records')
    data_R = dfR.to_dict('records')

    for row in data_T:
        ts = row['timestamp']  # Using 'timestamp' as per your DB View

        # Sync Regime (Slow) with Timing (Fast) tape
        while r_idx + 1 < len(data_R) and data_R[r_idx + 1]['timestamp'] <= ts:
            r = data_R[r_idx]
            if r['path'] > R and r['cost'] > R:
                regime = "BULL"
            elif r['path'] < -R and r['cost'] < -R:
                regime = "BEAR"
            else:
                regime = "NO"
            r_idx += 1

        price = row['price']

        # ---- Entry Logic ----
        if pos is None:
            if regime == "BULL" and row['clv'] < -T:
                pos, entry, stop, scaled = "LONG", price, price * (1 - STOP_LOSS), False
            elif regime == "BEAR" and row['clv'] > T:
                pos, entry, stop, scaled = "SHORT", price, price * (1 + STOP_LOSS), False
            continue

        # ---- Stop with Structural Veto ----
        if (pos == "LONG" and price <= stop) or (pos == "SHORT" and price >= stop):
            if (pos == "LONG" and row['path'] > C) or (pos == "SHORT" and row['path'] < -C):
                pass  # Vetoed by Institutional Structure
            else:
                trade = (price - entry) / entry if pos == "LONG" else (entry - price) / entry
                pnl += trade * (0.5 if scaled else 1.0)
                wins += trade > 0
                trades += 1
                pos = None
                continue

        # ---- Partial Scaling ----
        if not scaled:
            if (pos == "LONG" and row['clv'] > 0 and (row['obv'] < 0 or row['vwap'] < 0)) or \
                    (pos == "SHORT" and row['clv'] < 0 and (row['obv'] > 0 or row['vwap'] > 0)):
                pnl += 0.5 * ((price - entry) / entry if pos == "LONG" else (entry - price) / entry)
                scaled = True

        # ---- Full Exit ----
        if (pos == "LONG" and (row['cost'] < 0 or abs(row['path']) < C)) or \
                (pos == "SHORT" and (row['cost'] > 0 or abs(row['path']) < C)):
            trade = (price - entry) / entry if pos == "LONG" else (entry - price) / entry
            pnl += trade * (0.5 if scaled else 1.0)
            wins += trade > 0
            trades += 1
            pos = None

    return (pnl * 100, 100 * wins / trades, trades) if trades > 0 else None

async def save_optimized_configs(conn, summary):
    """Upserts the optimization results into the database."""
    log.info(f"Saving {len(summary)} optimized configurations to database...")
    query = """
        INSERT INTO public.stock_strategy_configs 
        (stock_name, reg_int, tim_int, r_val, c_val, t_val, stop_loss)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (stock_name) DO UPDATE SET
            reg_int = EXCLUDED.reg_int,
            tim_int = EXCLUDED.tim_int,
            r_val = EXCLUDED.r_val,
            c_val = EXCLUDED.c_val,
            t_val = EXCLUDED.t_val,
            stop_loss = EXCLUDED.stop_loss,
            updated_at = NOW();
    """
    records = [
        (s['Stock'], s['Reg_Int'], s['Tim_Int'], s['R'], s['C'], s['T'], STOP_LOSS)
        for s in summary
    ]
    await conn.executemany(query, records)
    log.info("✅ Optimization results saved successfully.")


# ========== 4. GLOBAL OPTIMIZATION RUNNER ==========
async def main():
    conn = await asyncpg.connect(
        user=config.DB_USER, password=config.DB_PASSWORD,
        host=config.DB_HOST, port=config.DB_PORT, database=config.DB_NAME
    )
    summary = []
    log.info("🚀 Running 5-D Brute Force (Dictionary Access Fix - UTC/IST Handled)")

    for stock in INSTRUMENT_MAP.keys():
        best = None
        for r_int in REGIME_INTERVALS:
            for t_int in TIMING_INTERVALS:
                if int(r_int[:-1]) < int(t_int[:-1]): continue

                dfR, dfT = await load_dual_data(conn, stock, r_int, t_int)
                if dfR.empty or dfT.empty: continue

                for R in REGIME_RANGE:
                    for C in CHOP_RANGE:
                        for T in TIMING_RANGE:
                            res = simulate(dfR, dfT, R, C, T)
                            if res and (not best or res[0] > best["PnL%"]):
                                best = {"Stock": stock, "Reg_Int": r_int, "Tim_Int": t_int,
                                        "R": R, "C": C, "T": T, "PnL%": round(res[0], 2),
                                        "Win%": round(res[1], 1), "Trades": res[2]}
        if best: summary.append(best)

    print("\n" + "=" * 105)
    print("GLOBAL 5-D OPTIMIZATION REPORT: Dictionary-Safe Simulation")
    print("=" * 105)
    print(pd.DataFrame(summary).sort_values("PnL%", ascending=False).to_string(index=False))

    if summary:
        await save_optimized_configs(conn, summary)
        print("\n" + "=" * 105)
        print("GLOBAL 5-D OPTIMIZATION REPORT: Results Saved to DB")
        print("=" * 105)
        print(pd.DataFrame(summary).sort_values("PnL%", ascending=False).to_string(index=False))

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())