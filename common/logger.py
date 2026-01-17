import logging
import os
import sys


def setup_logger():
    """
    Sets up a globally accessible logger using configuration from environment variables.
    """
    # Silence external library noise
    logging.getLogger("kiteconnect").setLevel(logging.CRITICAL)

    logger = logging.getLogger("DataPipelineLogger")

    # 1. REMOVE BACKTESTING CHECK AND LOAD FROM .ENV
    # Fetch log level from .env, default to INFO if not found
    env_log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Map string level to logging constants
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }

    # Set the level based on .env or default to INFO
    logger.setLevel(level_map.get(env_log_level, logging.INFO))

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


log = setup_logger()