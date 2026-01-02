# In service/parameters.py

from common import config

# --- Define Instrument Sets ---

REALTIME_INSTRUMENTS = {
    "AKZOINDIA": 375553,
    "AMBER": 303361,
    "APOLLOHOSP": 40193,
    "BAJAJ-AUTO": 4267265,
    "BRITANNIA": 140033,
    "CEATLTD": 3905025,
    "CUMMINSIND": 486657,
    "DIVISLAB": 2800641,
    "DIXON": 5552641,
    "EICHERMOT": 232961,
    "GVT&D": 4296449,
    "HAL": 589569,
    "HEROMOTOCO": 345089,
    "KAYNES": 3095553,
    "KEI": 3407361,
    "LTIM": 4561409,
    "MARUTI": 2815745,
    "NAVINFLUOR": 3756033,
    "PERSISTENT": 4701441,
    "PIIND": 6191105,
    "POLYCAB": 2455041,
    "POWERINDIA": 4724993,
    "RADICO": 2813441,
    "SOLARINDS": 3412993,
    "SRF": 837889,
    "SUPREMEIND": 860929,
    "TATAELXSI": 873217,
    "TCS": 2953217,
    "THERMAX": 889601,
    "TITAN": 897537,
    "TORNTPHARM": 900609,
    "TRENT": 502785,
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
