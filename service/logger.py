import logging
import sys

def setup_logger():
    """
    Sets up a globally accessible logger.

    This function configures a logger that can be imported and used
    across different modules of the application, ensuring that all
    log messages have a consistent format and destination.
    """
    # Get a logger instance with a specific name
    logger = logging.getLogger("DataPipelineLogger")
    logger.setLevel(logging.INFO) # Set the minimum level of messages to handle

    # Prevent the logger from having duplicate handlers if this function is called multiple times.
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a handler to output log messages to the console (standard output)
    handler = logging.StreamHandler(sys.stdout)

    # Define the format for the log messages
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the configured handler to the logger
    logger.addHandler(handler)

    return logger

# Create a single, module-level logger instance that can be imported by other parts of the application.
log = setup_logger()
