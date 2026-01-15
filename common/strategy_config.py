# common/strategy_config.py

# ==============================================================================
# GIDH ANALYTICS: OPTIMIZED INSTITUTIONAL CONFIG (10 AM - 2 PM IST)
# ==============================================================================

# --- 1. REGIME SENSORS (Directional Bias - 10m) ---
# Set to 0.25 based on the balanced performance of Dixon/Solarinds/Trent.
PATH_REGIME_THRESHOLD = 0.25
COST_REGIME_THRESHOLD = 0.25

# --- 2. EXIT & CHOP FILTERS (Trend Validation - 10m) ---
# WIDENED: Set to 0.10. Maruti and Solarinds thrived with this 'C' value.
# It allows for sideways movement without panicking.
PATH_CHOP_THRESHOLD = 0.10
COST_EXIT_THRESHOLD = 0.0

# --- 3. TIMING & TAPE SENSOR (Execution - 5m) ---
# INCREASED: Set to 0.40 (The 'T' parameter).
# This forces the engine to wait for deeper pullbacks, which was
# the key to Maruti and Dixon's high win rates.
PRESSURE_ENTRY_THRESHOLD = 0.40
PRESSURE_EXIT_THRESHOLD = 0.90

# --- 4. RISK MANAGEMENT (Safety) ---
# 0.50% is the institutional sweet spot for Dixon/Solar volatility.
STOP_LOSS_PCT = 0.0050

# --- 5. OPERATIONAL INTERVALS ---
REGIME_INTERVAL = "10m"
TIMING_INTERVAL = "5m"