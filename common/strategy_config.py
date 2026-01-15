# common/strategy_config.py

# ==============================================================================
# GIDH ANALYTICS: 3-SENSOR MARKET MODEL CONFIGURATION
# ==============================================================================

# --- 1. REGIME SENSORS (Directional Bias - 5m) ---
# Defines the strength required to establish a valid BULL or BEAR environment.
# PATH: Measures price structure (Higher Highs / Lower Lows).
# COST: Measures institutional value (Average of VWAP and OBV).
PATH_REGIME_THRESHOLD = 0.25  # Strength of structure_ratio
COST_REGIME_THRESHOLD = 0.25  # Alignment of institutional positioning

# --- 2. EXIT & CHOP FILTERS (Trend Validation - 5m) ---
# Defines when a trend has lost conviction or institutions have flipped.
PATH_CHOP_THRESHOLD = 0.15    # Exit if PATH enters sideways range [-0.15, 0.15]
COST_EXIT_THRESHOLD = 0.0     # Exit if institutions move against the trade

# --- 3. TIMING & TAPE SENSOR (Execution - 3m) ---
# Defines the sensitivity of the tape pullback for entries and exhaustion for exits.
# PRESSURE: Based on Candle Location Value (CLV) divergence.
PRESSURE_ENTRY_THRESHOLD = 0.35   # Min pullback intensity to trigger entry
PRESSURE_EXIT_THRESHOLD = 0.75    # Panic trigger for extreme tape moves (e.g. news/shocks)

# --- 4. RISK MANAGEMENT (Safety) ---
# The primary price-based circuit breaker for every trade.
STOP_LOSS_PCT = 0.0030           # 0.30% Hard Stop Loss

# --- 5. OPERATIONAL INTERVALS ---
# Determines which finalized bars trigger specific parts of the logic engine.
REGIME_INTERVAL = "10m"           # Timeframe for Trend and Institutional Analysis
TIMING_INTERVAL = "5m"           # Timeframe for Tape Execution and Pullbacks

# ==============================================================================
# NOTES:
# 1. EFFORT (LVC) is excluded to avoid noise from large-trade detection.
# 2. Entries require REGIME agreement (5m) and a PRESSURE pullback (3m).
# 3. Exits can be triggered by 5m (Structural/Value change) or 3m (Risk/Panic).
# ==============================================================================