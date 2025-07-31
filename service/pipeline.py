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

        # Batching configuration
        self.batch_size = 1000
        self.batch_interval = 5  # seconds

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
            await truncate_tables_if_needed(self.db_pool)
            await setup_schema(self.db_pool)
        except Exception as e:
            log.error(f"Failed to connect to or initialize the database: {e}")
            raise

    async def enricher_coroutine(self):
        log.info("Enricher coroutine started.")
        while True:
            raw_tick_message = await self.raw_tick_queue.get()
            try:
                log.debug(f"[Enricher] Received raw tick message: {raw_tick_message}")
                raw_tick = raw_tick_message.get('data')
                if raw_tick is None:
                    raise ValueError(f"'data' missing in message: {raw_tick_message}")

                try:
                    enriched_tick = self.feature_enricher.enrich_tick(raw_tick, self.data_window)
                    log.debug(
                        f"[Enricher] Enriched tick: {enriched_tick.stock_name} @ {enriched_tick.timestamp}, tick_volume={getattr(enriched_tick, 'tick_volume', None)}")
                except Exception as e:
                    log.error(f"[Enricher] enrich_tick failed for {raw_tick}. Using raw tick as fallback. Error: {e}",
                              exc_info=True)
                    enriched_tick = raw_tick  # type: ignore

                # Manage data window
                current_timestamp = enriched_tick.timestamp
                self.data_window.append((current_timestamp, enriched_tick))
                while self.data_window and (
                        current_timestamp - self.data_window[0][0]).total_seconds() > self.data_window_seconds:
                    self.data_window.popleft()

                await self.enriched_tick_queue.put(enriched_tick)
                log.debug(
                    f"[Enricher] Put enriched tick into queue. Enriched queue size: {self.enriched_tick_queue.qsize()}")

            except Exception as e:
                log.error(f"Failed to process raw tick message: {raw_tick_message}. Error: {e}", exc_info=True)
            finally:
                self.raw_tick_queue.task_done()

    async def db_writer_coroutine(self):
        log.info("DB writer coroutine started.")
        ticks_batch = []
        last_write_time = asyncio.get_event_loop().time()

        try:
            while True:
                # Wait up to batch_interval for the first tick
                try:
                    enriched_tick = await asyncio.wait_for(self.enriched_tick_queue.get(), timeout=self.batch_interval)
                    log.debug(
                        f"[DB Writer] Received enriched tick: {enriched_tick.stock_name} @ {enriched_tick.timestamp}")
                    ticks_batch.append(enriched_tick)
                except asyncio.TimeoutError:
                    log.debug(
                        f"[DB Writer] No tick received in {self.batch_interval}s. Current batch size: {len(ticks_batch)}; queue size: {self.enriched_tick_queue.qsize()}")
                except Exception as e:
                    log.error(f"[DB Writer] Unexpected error while getting tick: {e}", exc_info=True)
                    # continue so we don't kill the loop

                # Drain any additional available ticks without waiting
                while len(ticks_batch) < self.batch_size:
                    try:
                        extra_tick = self.enriched_tick_queue.get_nowait()
                        log.debug(
                            f"[DB Writer] Drained extra enriched tick: {extra_tick.stock_name} @ {extra_tick.timestamp}")
                        ticks_batch.append(extra_tick)
                    except asyncio.QueueEmpty:
                        break

                time_since_last_write = asyncio.get_event_loop().time() - last_write_time
                if ticks_batch and (
                        len(ticks_batch) >= self.batch_size or time_since_last_write >= self.batch_interval):
                    log.info(f"Writing batch of {len(ticks_batch)} ticks to DB...")
                    ticks_with_depth = [t for t in ticks_batch if t.depth]

                    try:
                        await db_writer.batch_insert_ticks(self.db_pool, ticks_batch)
                        if ticks_with_depth:
                            await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)
                    except Exception as e:
                        log.error(f"[DB Writer] Failed to write batch to DB: {e}", exc_info=True)
                        # Decide: you might retry or drop; for now, continue so it doesn't deadlock

                    # Acknowledge all pulled ticks
                    for _ in ticks_batch:
                        try:
                            self.enriched_tick_queue.task_done()
                        except Exception:
                            # protection in case of mismatch
                            log.warning("Task done mismatch when acknowledging enriched ticks.")
                    ticks_batch.clear()
                    last_write_time = asyncio.get_event_loop().time()

        except asyncio.CancelledError:
            log.info("DB writer coroutine is being cancelled.")
        except Exception as e:
            log.error(f"[DB Writer] Coroutine crashed unexpectedly: {e}", exc_info=True)
        finally:
            if ticks_batch:
                log.warning(f"Writing final batch of {len(ticks_batch)} ticks before shutdown.")
                ticks_with_depth = [t for t in ticks_batch if t.depth]
                try:
                    await db_writer.batch_insert_ticks(self.db_pool, ticks_batch)
                    if ticks_with_depth:
                        await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)
                except Exception as e:
                    log.error(f"[DB Writer] Final flush failed: {e}", exc_info=True)
                for _ in ticks_batch:
                    try:
                        self.enriched_tick_queue.task_done()
                    except Exception:
                        pass
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
        await file_reader.stream_ticks(self.raw_tick_queue)

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        # Create tasks for each pipeline stage
        enricher_task = asyncio.create_task(self.enricher_coroutine())
        writer_task = asyncio.create_task(self.db_writer_coroutine())
        attach_task_monitor(writer_task, "DB writer")

        # Start the data source in a separate task
        data_source_task = asyncio.create_task(self.start_data_source())

        try:
            # Wait for the data source to finish
            await data_source_task

            # Wait for both queues to be fully processed
            log.info("Data source finished. Waiting for queues to empty...")
            await self.raw_tick_queue.join()
            await self.enriched_tick_queue.join()
            log.info("Queues are empty.")


        except Exception as e:
            log.error(f"An unhandled exception occurred in the main run loop: {e}", exc_info=True)

        finally:
            log.info("Shutting down data pipeline...")
            log.info(f"Final in-memory data window size: {len(self.data_window)} ticks.")

            # Gracefully cancel all background tasks
            enricher_task.cancel()
            writer_task.cancel()
            await asyncio.gather(enricher_task, writer_task, return_exceptions=True)

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
