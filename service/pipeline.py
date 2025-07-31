import asyncio
import asyncpg
from collections import deque
from datetime import timedelta

from service.db_schema import setup_schema, truncate_tables_if_needed
from service.logger import log
import service.config as config
from service.parameters import INSTRUMENT_MAP
from service.file_reader import FileReader
import service.db_writer as db_writer
from service.feature_enricher import FeatureEnricher  # Import the new enricher
from service.models import EnrichedTick  # Import the new model


class DataPipeline:
    """
    The main class for handling the data pipeline.
    """

    def __init__(self):
        """
        Initializes the DataPipeline using settings from config and parameters files.
        """
        self.mode = config.PIPELINE_MODE
        self.instruments = list(INSTRUMENT_MAP.values())
        self.instrument_map = INSTRUMENT_MAP
        self.db_pool = None

        # --- Pipeline Queues ---
        self.raw_tick_queue = asyncio.Queue()
        self.enriched_tick_queue = asyncio.Queue()

        # --- In-Memory Data Window for Enriched Ticks ---
        self.data_window = deque()
        self.data_window_seconds = config.DATA_WINDOW_MINUTES * 60

        # --- Pipeline Components ---
        self.feature_enricher = FeatureEnricher()

        # Batching configuration
        self.batch_size = 1000
        self.batch_interval = 5  # seconds
        log.info(f"DataPipeline initialized in '{self.mode}' mode for {len(self.instruments)} instruments.")

    async def initialize_db(self):
        """Creates an asyncpg connection pool, truncates tables if needed, and ensures schema is set up."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME
            )
            log.info(f"Successfully connected to the database '{config.DB_NAME}'.")
            await truncate_tables_if_needed(self.db_pool)
            await setup_schema(self.db_pool)
        except Exception as e:
            log.error(f"Failed to connect to or initialize the database: {e}")
            raise

    async def enricher_coroutine(self):
        """
        Coroutine that consumes raw ticks, enriches them, and passes them on.
        This is now wrapped in a try/except block to prevent a single bad tick
        from crashing the entire pipeline.
        """
        log.info("Enricher coroutine started.")
        while True:
            raw_tick = await self.raw_tick_queue.get()

            try:
                # Enrich the raw tick using our new component
                enriched_tick = self.feature_enricher.enrich_tick(raw_tick, self.data_window)

                # --- Manage the in-memory data window ---
                current_timestamp = enriched_tick.timestamp
                self.data_window.append((current_timestamp, enriched_tick))
                while self.data_window and (
                        current_timestamp - self.data_window[0][0]).total_seconds() > self.data_window_seconds:
                    self.data_window.popleft()

                # Put the enriched tick onto the next queue for the DB writer
                await self.enriched_tick_queue.put(enriched_tick)

            except Exception as e:
                log.error(f"Failed to enrich tick for {raw_tick.stock_name} at {raw_tick.timestamp}: {e}",
                          exc_info=True)

            finally:
                # CRITICAL: Ensure task_done() is always called, even if an error occurs.
                # This prevents the pipeline from deadlocking on queue.join().
                self.raw_tick_queue.task_done()

    async def db_writer_coroutine(self):
        """
        Coroutine that consumes enriched ticks and writes them to the DB in batches.
        """
        log.info("DB writer coroutine started.")
        ticks_batch = []
        last_write_time = asyncio.get_event_loop().time()

        try:
            while True:
                try:
                    # Calculate timeout for waiting on the queue
                    timeout = max(0, self.batch_interval - (asyncio.get_event_loop().time() - last_write_time))
                    enriched_tick = await asyncio.wait_for(self.enriched_tick_queue.get(), timeout=timeout)
                    ticks_batch.append(enriched_tick)
                    self.enriched_tick_queue.task_done()

                except asyncio.TimeoutError:
                    # This is not an error, just a signal to check if we should write the batch
                    pass

                # Check if it's time to write the batch
                time_since_last_write = asyncio.get_event_loop().time() - last_write_time
                if len(ticks_batch) >= self.batch_size or (
                        time_since_last_write >= self.batch_interval and ticks_batch):
                    log.info(f"Writing batch of {len(ticks_batch)} ticks to DB...")
                    ticks_with_depth = [t for t in ticks_batch if t.depth]

                    await db_writer.batch_insert_ticks(self.db_pool, ticks_batch)
                    if ticks_with_depth:
                        await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)

                    ticks_batch.clear()
                    last_write_time = asyncio.get_event_loop().time()

        except asyncio.CancelledError:
            # This is expected during shutdown.
            log.info("DB writer coroutine is being cancelled.")

        finally:
            # --- CRITICAL: Write any remaining items in the batch before exiting ---
            if ticks_batch:
                log.warning(f"Writing final batch of {len(ticks_batch)} ticks before shutdown.")
                ticks_with_depth = [t for t in ticks_batch if t.depth]

                await db_writer.batch_insert_ticks(self.db_pool, ticks_batch)
                if ticks_with_depth:
                    await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)

                ticks_batch.clear()
            log.info("DB writer coroutine finished cleanup.")

    async def start_data_source(self):
        """Starts the data source based on the selected mode."""
        log.info(f"Attempting to start data source in '{self.mode}' mode.")
        if self.mode == 'realtime':
            # await self.start_websocket() # Placeholder
            pass
        elif self.mode == 'backtesting':
            await self.start_file_reader()
        else:
            log.error(f"Invalid mode specified: {self.mode}")
            raise ValueError(f"Invalid mode: {self.mode}")

    async def start_file_reader(self):
        """Initializes and runs the file reader for backtesting."""
        log.info("Starting file reader for backtesting.")
        file_reader = FileReader()
        # The file reader now puts data into the raw_tick_queue
        await file_reader.stream_ticks(self.raw_tick_queue)

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        # Create tasks for each pipeline stage
        enricher_task = asyncio.create_task(self.enricher_coroutine())
        writer_task = asyncio.create_task(self.db_writer_coroutine())
        all_tasks = [enricher_task, writer_task]

        try:
            await self.start_data_source()
            # Wait for queues to be fully processed
            await self.raw_tick_queue.join()
            await self.enriched_tick_queue.join()

        except Exception as e:
            log.error(f"An unhandled exception occurred in the main run loop: {e}", exc_info=True)

        finally:
            log.info("Shutting down data pipeline...")
            log.info(f"Final in-memory data window size: {len(self.data_window)} ticks.")

            # Gracefully cancel all background tasks
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)

            if self.db_pool:
                await self.db_pool.close()
                log.info("Database connection pool closed.")
            log.info("Data pipeline shut down gracefully.")
