import os
from dotenv import load_dotenv
from service.logger import log
from datetime import datetime

# Load environment variables from a .env file into the system's environment
load_dotenv()

# --- Retrieve Configuration from Environment Variables ---
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "realtime")

# --- Backtesting Configuration ---
BACKTEST_DATA_DIRECTORY = os.getenv("BACKTEST_DATA_DIRECTORY")
SAVE_RAW_TICKS_IN_BACKTEST = os.getenv("BACKTEST_SAVE_RAW_TICKS", "false").lower() == "true"
BACKTEST_DATE_STR = os.getenv("BACKTEST_DATE")
BACKTEST_SLEEP_DURATION = float(os.getenv("BACKTEST_SLEEP_DURATION", 0.001))
SKIP_RAW_DB_WRITES = PIPELINE_MODE == 'backtesting' and not SAVE_RAW_TICKS_IN_BACKTEST

# --- New Data Window and Truncation Settings ---
DATA_WINDOW_MINUTES = int(os.getenv("DATA_WINDOW_MINUTES", 60))
# Convert "true" string to boolean
TRUNCATE_TABLES_ON_BACKTEST = os.getenv("TRUNCATE_TABLES_ON_BACKTEST", "false").lower() == "true"


def validate_config():
    """
    Validates that all necessary configuration variables have been loaded.
    If any required variable is missing, it logs an error and raises an exception.
    """
    required_vars = {
        "KITE_API_KEY": KITE_API_KEY,
        "KITE_API_SECRET": KITE_API_SECRET,
        "KITE_ACCESS_TOKEN": KITE_ACCESS_TOKEN,
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_NAME": DB_NAME,
    }

    if PIPELINE_MODE == 'backtesting':
        required_vars["BACKTEST_DATA_DIRECTORY"] = BACKTEST_DATA_DIRECTORY
        required_vars["BACKTEST_DATE"] = BACKTEST_DATE_STR

    missing_vars = [key for key, value in required_vars.items() if value is None]

    if missing_vars:
        error_message = f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file."
        log.error(error_message)
        raise ValueError(error_message)

    if PIPELINE_MODE not in ['realtime', 'backtesting']:
        error_message = f"Invalid PIPELINE_MODE: '{PIPELINE_MODE}'. Must be 'realtime' or 'backtesting'."
        log.error(error_message)
        raise ValueError(error_message)

    # Validate date format if in backtesting mode
    if PIPELINE_MODE == 'backtesting':
        try:
            datetime.strptime(BACKTEST_DATE_STR, '%Y-%m-%d')
        except ValueError:
            error_message = f"Invalid BACKTEST_DATE format: '{BACKTEST_DATE_STR}'. Must be 'YYYY-MM-DD'."
            log.error(error_message)
            raise ValueError(error_message)

    log.info("Configuration loaded and validated successfully.")
    log.info(f"Pipeline mode is set to: '{PIPELINE_MODE}'")
    log.info(f"Data window is set to: {DATA_WINDOW_MINUTES} minutes")
    log.info(f"Truncate tables on backtest: {TRUNCATE_TABLES_ON_BACKTEST}")


# --- Run Validation on Import ---
validate_config()
