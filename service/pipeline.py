import asyncio
import asyncpg
from collections import deque
from datetime import timedelta

from service.bar_aggregator import BarAggregatorProcessor
from service.db_schema import setup_schema, truncate_tables_if_needed
from service.logger import log
import service.config as config
from service.parameters import INSTRUMENT_MAP
from service.file_reader import FileReader
import service.db_writer as db_writer
from service.feature_enricher import FeatureEnricher
from service.models import EnrichedTick


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
        self.bar_aggregator_processor = BarAggregatorProcessor() # Add this

        # Batching configuration
        self.tick_batch_size = 1000
        self.bar_batch_size = 100
        self.batch_interval = 2  # seconds

        # Counter for printing enriched ticks
        self.enriched_tick_print_counter = 0
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
            await setup_schema(self.db_pool)
            await truncate_tables_if_needed(self.db_pool)
        except Exception as e:
            log.error(f"Failed to connect to or initialize the database: {e}")
            raise

    async def processor_and_writer_coroutine(self):
        """
        A single, powerful coroutine that consumes raw ticks and handles:
        1. Feature Enrichment
        2. Writing enriched ticks & depth to DB
        3. Bar Aggregation
        4. Writing feature bars to DB
        """
        log.info("Primary processor and writer coroutine started.")

        tick_batch = []
        bar_batch = []
        last_write_time = asyncio.get_event_loop().time()

        while True:
            try:
                # 1. Get raw tick from the queue
                raw_tick_message = await self.raw_tick_queue.get()
                raw_tick = raw_tick_message.get('data')

                # 2. Enrich the tick
                enriched_tick = self.feature_enricher.enrich_tick(raw_tick, self.data_window)
                tick_batch.append(enriched_tick)

                # 3. Manage the global data window
                current_timestamp = enriched_tick.timestamp
                self.data_window.append((current_timestamp, enriched_tick))
                while self.data_window and (
                        current_timestamp - self.data_window[0][0]).total_seconds() > self.data_window_seconds:
                    self.data_window.popleft()

                # 4. Process with the bar aggregator
                updated_bars = self.bar_aggregator_processor.process_tick(enriched_tick)
                if updated_bars:
                    bar_batch.extend(updated_bars)

                # 5. Check if it's time to write batches to DB
                time_since_last_write = asyncio.get_event_loop().time() - last_write_time
                if (len(tick_batch) >= self.tick_batch_size or
                        len(bar_batch) >= self.bar_batch_size or
                        time_since_last_write >= self.batch_interval):

                    # Write ticks and depth
                    if tick_batch:
                        log.info(f"Writing batch of {len(tick_batch)} ticks to DB...")
                        ticks_with_depth = [t for t in tick_batch if t.depth]
                        await db_writer.batch_insert_ticks(self.db_pool, tick_batch)
                        if ticks_with_depth:
                            await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)
                        tick_batch.clear()

                    # Write feature bars
                    if bar_batch:
                        log.info(f"Writing batch of {len(bar_batch)} feature bars to DB...")
                        await db_writer.batch_upsert_features(self.db_pool, bar_batch)
                        bar_batch.clear()

                    last_write_time = asyncio.get_event_loop().time()

            except Exception as e:
                log.error(f"Error in processor coroutine: {e}", exc_info=True)
            finally:
                self.raw_tick_queue.task_done()

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
        await file_reader.stream_ticks(self.raw_tick_queue)

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        # Create tasks for each pipeline stage
        processor_task = asyncio.create_task(self.processor_and_writer_coroutine())
        attach_task_monitor(processor_task, "Processor and Writer")

        # Start the data source in a separate task
        data_source_task = asyncio.create_task(self.start_data_source())

        try:
            # Wait for the data source to finish
            await data_source_task
            log.info("Data source finished. Waiting for queue to empty...")
            await self.raw_tick_queue.join()
            log.info("Queue is empty.")

        except Exception as e:
            log.error(f"An unhandled exception occurred in the main run loop: {e}", exc_info=True)

        finally:
            log.info("Shutting down data pipeline...")
            # Gracefully cancel the processor task
            processor_task.cancel()
            await asyncio.gather(processor_task, return_exceptions=True)

            if self.db_pool:
                await self.db_pool.close()
                log.info("Database connection pool closed.")
            log.info("Data pipeline shut down gracefully.")


def attach_task_monitor(task, name):
    def cb(t):
        if t.cancelled():
            log.warning(f"{name} task was cancelled.")
        elif t.exception():
            log.error(f"{name} task crashed: {t.exception()}", exc_info=True)

    task.add_done_callback(cb)
