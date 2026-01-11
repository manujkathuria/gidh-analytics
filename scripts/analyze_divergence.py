import asyncio
import pandas as pd
import asyncpg
from common import config
from common.logger import log

# Same Instrument Map as your parameters.py
INSTRUMENT_MAP = {
    "APOLLOHOSP": 40193, "BAJAJ-AUTO": 4267265, "BRITANNIA": 140033,
    "DIXON": 5552641, "EICHERMOT": 232961, "HAL": 589569,
    "HEROMOTOCO": 345089, "MARUTI": 2815745, "POWERINDIA": 4724993,
    "SOLARINDS": 3412993, "SRF": 837889, "TATAELXSI": 873217,
    "TCS": 2953217, "THERMAX": 889601, "TITAN": 897537,
    "TORNTPHARM": 900609, "TRENT": 502785,
}

async def fetch_intensity_analysis():
    try:
        selected_stocks = list(INSTRUMENT_MAP.keys())
        conn = await asyncpg.connect(
            user=config.DB_USER, password=config.DB_PASSWORD,
            host=config.DB_HOST, port=config.DB_PORT, database=config.DB_NAME
        )

        # PRODUCTION TUNING
        TP = 0.0025          # 0.25% Profit Target
        SL = 0.0030          # 0.30% Stop Loss
        BE_TRIGGER = 0.0012  # Move to Break-even at +0.12%
        THRESHOLD = -0.5     # "Intensity" Filter

        query = f"""
        WITH raw_data AS (
            SELECT 
                timestamp, stock_name, "interval" AS tf, close, high, low,
                -- Intensity Logic
                (div_price_obv < {THRESHOLD} AND div_price_clv < {THRESHOLD}) AS is_signal,
                -- Fallback for VWAP: Use Typical Price (H+L+C)/3
                (high + low + close) / 3 as typical_price
            FROM public.grafana_features_view
            WHERE stock_name = ANY($1)
        ),
        signal_starts AS (
            SELECT timestamp, stock_name, tf, close AS entry_price
            FROM (
                SELECT *, LAG(is_signal) OVER (PARTITION BY stock_name, tf ORDER BY timestamp) AS prev_signal
                FROM raw_data
            ) sub
            WHERE is_signal AND (prev_signal IS FALSE OR prev_signal IS NULL)
              -- Traps only: Short when price is 'expensive' relative to current bar midpoint
              AND close > typical_price 
        ),
        path_results AS (
            SELECT 
                s.tf, s.timestamp, s.entry_price,
                ARRAY_AGG(f.high ORDER BY f.timestamp) as high_path,
                ARRAY_AGG(f.low ORDER BY f.timestamp) as low_path
            FROM signal_starts s
            JOIN raw_data f ON f.stock_name = s.stock_name AND f.tf = s.tf
                AND f.timestamp > s.timestamp 
                AND f.timestamp <= s.timestamp + INTERVAL '2 hours'
            GROUP BY s.tf, s.timestamp, s.entry_price
        ),
        final_trades AS (
            SELECT 
                tf,
                CASE 
                    WHEN tp_idx < sl_idx AND tp_idx < 99 THEN {TP}      -- Hit TP First
                    WHEN be_idx < sl_idx AND sl_idx < tp_idx THEN 0.0   -- Hit BE, then hit SL (Saved!)
                    WHEN sl_idx < tp_idx AND sl_idx < be_idx THEN -{SL} -- Hit SL immediately
                    ELSE 0.0                                            -- Flat/Timeout
                END as trade_return
            FROM (
                SELECT 
                    tf, entry_price,
                    (SELECT COALESCE(MIN(i), 99) FROM generate_subscripts(high_path, 1) i 
                     WHERE high_path[i] >= entry_price * (1 + {SL})) as sl_idx,
                    (SELECT COALESCE(MIN(i), 99) FROM generate_subscripts(low_path, 1) i 
                     WHERE low_path[i] <= entry_price * (1 - {TP})) as tp_idx,
                    (SELECT COALESCE(MIN(i), 99) FROM generate_subscripts(low_path, 1) i 
                     WHERE low_path[i] <= entry_price * (1 - {BE_TRIGGER})) as be_idx
                FROM path_results
            ) sub2
        )
        SELECT 
            tf as interval,
            COUNT(*) as trades,
            ROUND(AVG(trade_return * 100)::numeric, 4) as avg_return_pct,
            ROUND((SUM(CASE WHEN trade_return > 0 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2) as win_rate_pct,
            ROUND((SUM(CASE WHEN trade_return = 0 THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100), 2) as be_rate_pct
        FROM final_trades
        GROUP BY tf
        ORDER BY avg_return_pct DESC;
        """

        log.info(f"Running High-Intensity Research (Threshold: {THRESHOLD}, BE at +0.12%)")
        records = await conn.fetch(query, selected_stocks)
        await conn.close()
        return pd.DataFrame(records, columns=['interval', 'trades', 'avg_return_pct', 'win_rate_pct', 'be_rate_pct'])

    except Exception as e:
        log.error(f"Intensity Analysis failed: {e}")
        return None

async def main():
    df = await fetch_intensity_analysis()
    if df is not None:
        print("\n📊 INTENSITY RESULTS: Div < -0.5 + BE Management")
        print(df.to_string(index=False))

if __name__ == "__main__":
    asyncio.run(main())