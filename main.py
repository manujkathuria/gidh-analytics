import asyncio
from service.pipeline import DataPipeline
from service.logger import log

async def main():
    """
    Main function to initialize and run the data pipeline.
    This is the primary entry point of the application.
    """
    # The pipeline now configures itself from the service/config.py
    # and service/parameters.py files.
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

