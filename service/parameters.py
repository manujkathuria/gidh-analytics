# In service/parameters.py

from service import config # Import the config module

# --- Define Instrument Sets ---

# Instruments for live, real-time trading
REALTIME_INSTRUMENTS = {
    "BOSCHLTD":   558337,
    "POWERINDIA": 4724993,
    "SOLARINDS":  3412993,
    "DIXON":      5552641,
    "FORCEMOT":   2962689,
    "BAJAJHLDNG": 78081,
    "MARUTI":     2815745,
    "ULTRACEMCO": 2952193,
    "NEULANDLAB": 615937,
    "ORACLE":     2748929,
    "MCX":        7982337,
    "BAJAJ-AUTO": 4267265,
}

# A smaller, consistent set of instruments for backtesting
BACKTEST_INSTRUMENTS = {
    "BOSCHLTD":   558337,
    "POWERINDIA": 4724993,
    "SOLARINDS":  3412993,
    "DIXON":      5552641,
    "FORCEMOT":   2962689,
    "BAJAJHLDNG": 78081,
    "MARUTI":     2815745,
    "ULTRACEMCO": 2952193,
    "NEULANDLAB": 615937,
    "ORACLE":     2748929,
    "MCX":        7982337,
    "BAJAJ-AUTO": 4267265,
}

# --- Dynamically Select the Instrument Map ---

# This logic will choose the correct map based on the PIPELINE_MODE.
# Other files will continue to import 'INSTRUMENT_MAP' and will not need to be changed.
if config.PIPELINE_MODE == 'realtime':
    INSTRUMENT_MAP = REALTIME_INSTRUMENTS
else:
    # Default to backtesting instruments for safety
    INSTRUMENT_MAP = BACKTEST_INSTRUMENTS