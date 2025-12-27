# In service/parameters.py

from common import config

# --- Define Instrument Sets ---

REALTIME_INSTRUMENTS = {
    "AMBER": 303361,
    "APOLLOHOSP": 40193,
    "BRITANNIA": 140033,
    "BAJAJ-AUTO": 4267265,
    "DIXON": 5552641,
    "EICHERMOT": 232961,
    "HEROMOTOCO": 345089,
    "MARUTI": 2815745,
    "MCX": 7982337,
    "POLYCAB": 2455041,
    "POWERINDIA": 4724993,
    "SOLARINDS": 3412993,
    "ULTRACEMCO": 2952193,
}

# Instruments for live, real-time trading

# A smaller, consistent set of instruments for backtesting
BACKTEST_INSTRUMENTS = {
    "DIXON": 5552641,
    "MARUTI": 2815745,
    "POWERINDIA": 4724993,
    "BOSCHLTD": 558337,

}

# --- Dynamically Select the Instrument Map ---

# This logic will choose the correct map based on the PIPELINE_MODE.
# Other files will continue to import 'INSTRUMENT_MAP' and will not need to be changed.
if config.PIPELINE_MODE == 'realtime':
    INSTRUMENT_MAP = REALTIME_INSTRUMENTS
else:
    # Default to backtesting instruments for safety
    INSTRUMENT_MAP = BACKTEST_INSTRUMENTS
