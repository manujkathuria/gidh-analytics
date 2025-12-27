import asyncio
from core.pipeline import DataPipeline
from common.config import validate_config
from common.logger import log

async def main():
    """
    Main function to initialize and run the data pipeline.
    This is the primary entry point of the application.
    """

    # 1. Validate config before starting the engine
    validate_config()

    # 2. Start the Backend Engine
    pipeline = DataPipeline()
    try:
        await pipeline.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Application interrupted by user. Shutting down...")
    except Exception as e:
        # Catch any other unexpected errors to ensure graceful shutdown.
        log.error(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        # This block ensures that all running asyncio tasks are properly cancelled
        # when the application exits, preventing them from being left in a running state.
        log.info("Cleaning up tasks...")
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Application has been shut down.")


if __name__ == "__main__":
    try:
        # Run the main asynchronous function
        asyncio.run(main())
    except KeyboardInterrupt:
        # This handles the case where Ctrl+C is pressed before the asyncio event loop starts.
        log.info("Program terminated by user during startup.")

