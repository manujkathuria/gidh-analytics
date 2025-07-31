import os
from dotenv import load_dotenv
from service.logger import log

# Load environment variables from a .env file into the system's environment
load_dotenv()

# --- Retrieve Configuration from Environment Variables ---
# These lines fetch the necessary configuration values. If a variable is not set, its value will be None.
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "realtime") # Default to 'realtime' if not set

def validate_config():
    """
    Validates that all necessary configuration variables have been loaded.
    If any required variable is missing, it logs an error and raises an exception.
    """
    # Note: PIPELINE_MODE is not in required_vars as it has a default value.
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

    # Identify any variables that are missing (i.e., are None)
    missing_vars = [key for key, value in required_vars.items() if value is None]

    if missing_vars:
        # If there are missing variables, construct an error message and terminate the application.
        error_message = f"Missing required environment variables: {', '.join(missing_vars)}. Please check your .env file."
        log.error(error_message)
        raise ValueError(error_message)

    if PIPELINE_MODE not in ['realtime', 'backtesting']:
        error_message = f"Invalid PIPELINE_MODE: '{PIPELINE_MODE}'. Must be 'realtime' or 'backtesting'."
        log.error(error_message)
        raise ValueError(error_message)


    log.info("Configuration loaded and validated successfully.")
    log.info(f"Pipeline mode is set to: '{PIPELINE_MODE}'")


# --- Run Validation on Import ---
# This ensures that the application will not start without the required configuration.
validate_config()
