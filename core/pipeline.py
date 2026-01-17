# service/pipeline.py
import asyncio
import asyncpg
from collections import deque

from core.bar_aggregator import BarAggregatorProcessor
from core.db_schema import setup_schema, truncate_tables_if_needed
from common.logger import log
import common.config as config
from common.parameters import INSTRUMENT_MAP
from core.file_reader import FileReader
from core.alert_engine import StrategyEngine
from core.websocket_client import WebSocketClient
import core.db_writer as db_writer
from core.feature_enricher import FeatureEnricher
# Import the new mode-aware functions
from core.db_reader import fetch_live_thresholds, calculate_and_fetch_backtest_thresholds


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

        # --- Pipeline Queues & Events ---
        self.raw_tick_queue = asyncio.Queue()
        # Event to signal shutdown
        self._shutdown_event = asyncio.Event()

        # --- In-Memory Data Window for Enriched Ticks ---
        self.data_window = deque()
        self.data_window_seconds = config.DATA_WINDOW_MINUTES * 60

        # --- Pipeline Components ---
        self.websocket_client = None
        if self.mode == 'realtime':
            loop = asyncio.get_event_loop()
            self.websocket_client = WebSocketClient(self.raw_tick_queue, self.instrument_map, loop)

        # The enricher is now initialized without thresholds
        self.feature_enricher = FeatureEnricher()
        self.bar_aggregator_processor = BarAggregatorProcessor()
        self.strategy_engine = None
        # Batching configuration
        self.tick_batch_size = 1000
        self.bar_batch_size = 100
        self.batch_interval = 2  # seconds
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
        Primary coroutine that consumes raw ticks, enriches them,
        aggregates bars, and triggers the strategy engine on finalized bars.
        """
        log.info("Primary processor and writer coroutine started.")

        tick_batch = []
        bar_batch = []
        last_write_time = asyncio.get_event_loop().time()

        while not self._shutdown_event.is_set():
            try:
                # Wait for an item with a timeout, allowing the loop to check for shutdown
                raw_tick_message = await asyncio.wait_for(self.raw_tick_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue  # No item received, loop again to check shutdown event

            try:
                raw_tick = raw_tick_message.get('data')
                if not raw_tick: continue

                # 1. Enrichment: Calculate trade sign, large trade flags, and absorption
                enriched_tick = self.feature_enricher.enrich_tick(raw_tick, self.data_window)
                tick_batch.append(enriched_tick)

                # Manage the global data window for internal feature calculations
                current_timestamp = enriched_tick.timestamp
                self.data_window.append((current_timestamp, enriched_tick))
                while self.data_window and \
                        (current_timestamp - self.data_window[0][0]).total_seconds() > self.data_window_seconds:
                    self.data_window.popleft()

                # 2. Aggregation & Strategy Logic
                # We iterate through intervals to identify exactly when a bar is finalized.
                from core.bar_aggregator import BAR_INTERVALS

                for interval in BAR_INTERVALS:
                    agg_key = f"{enriched_tick.stock_name}-{int(interval.total_seconds())}"

                    # Ensure the aggregator exists for this instrument/interval
                    if agg_key not in self.bar_aggregator_processor.aggregators:
                        from core.bar_aggregator import BarAggregator
                        self.bar_aggregator_processor.aggregators[agg_key] = BarAggregator(
                            enriched_tick.stock_name, enriched_tick.instrument_token, interval
                        )

                    agg = self.bar_aggregator_processor.aggregators[agg_key]

                    # add_tick returns a BarData object ONLY when the previous bar is completed
                    finalized_bar = agg.add_tick(enriched_tick)

                    if finalized_bar:
                        # --- CRITICAL: Trigger Strategy ONLY on Finalized Bars ---
                        if self.strategy_engine:
                            await self.strategy_engine.run_logic(finalized_bar)
                        bar_batch.append(finalized_bar)

                    # Always add the currently building bar to the batch for live updates in DB/Grafana
                    if agg.building_bar:
                        bar_batch.append(agg.building_bar)

                # 3. Batch DB Writing
                time_since_last_write = asyncio.get_event_loop().time() - last_write_time
                if (len(tick_batch) >= self.tick_batch_size or
                        len(bar_batch) >= self.bar_batch_size or
                        time_since_last_write >= self.batch_interval):

                    if tick_batch:
                        await db_writer.batch_insert_ticks(self.db_pool, tick_batch)
                        ticks_with_depth = [t for t in tick_batch if t.depth]
                        if ticks_with_depth:
                            await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)
                        tick_batch.clear()

                    if bar_batch:
                        # Use upsert to handle real-time updates of the 'building' bar
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
            self.start_websocket()
        elif self.mode == 'backtesting':
            await self.start_file_reader()
        else:
            log.error(f"Invalid mode specified: {self.mode}")
            raise ValueError(f"Invalid mode: {self.mode}")

    def start_websocket(self):
        """Initializes and connects the WebSocket client."""
        if self.websocket_client:
            self.websocket_client.connect()
        else:
            log.error("WebSocket client not initialized. Check pipeline mode.")

    async def start_file_reader(self):
        """Initializes and runs the file reader for backtesting."""
        log.info("Starting file reader for backtesting.")
        file_reader = FileReader()
        await file_reader.stream_ticks(self.raw_tick_queue)

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        # --- Threshold Loading Logic ---
        large_trade_thresholds = {}
        if config.PIPELINE_MODE == 'backtesting':
            log.info(f"Connecting to Production DB ({config.DB_NAME_PRODUCTION}) for thresholds...")
            # Create a temporary pool to read from the production DB
            temp_live_pool = await asyncpg.create_pool(
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME_PRODUCTION
            )

            # CHANGE: Use fetch_live_thresholds instead of dynamic calculation
            large_trade_thresholds = await fetch_live_thresholds(temp_live_pool)

            # Close the temporary connection
            await temp_live_pool.close()
            log.info("Production thresholds loaded. Results will be stored in " + config.DB_NAME)

        token_to_name_map = {v: k for k, v in self.instrument_map.items()}
        self.feature_enricher.load_thresholds(large_trade_thresholds, token_to_name_map)

        processor_task = asyncio.create_task(self.processor_and_writer_coroutine())
        attach_task_monitor(processor_task, "Processor and Writer")

        data_source_task = asyncio.create_task(self.start_data_source())
        self.strategy_engine = StrategyEngine(self.db_pool)
        try:
            if self.mode == 'backtesting':
                await data_source_task
                log.info("Data source finished. Waiting for queue to empty...")
                await self.raw_tick_queue.join()
                log.info("Queue is empty.")
            else:  # Realtime mode
                log.info("Running in real-time mode. Press Ctrl+C to exit.")
                await self._shutdown_event.wait()  # Wait here indefinitely until event is set

        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("Application interrupted by user.")

        finally:
            log.info("Shutting down data pipeline...")
            self._shutdown_event.set()  # Signal all coroutines to stop

            if self.websocket_client:
                self.websocket_client.close()

            # Wait for the processor to finish handling any remaining items
            await asyncio.sleep(1)  # Give it a moment to finish up
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
            exc = t.exception()
            # Avoid logging the "task_done" error as it's an expected part of shutdown now
            if not isinstance(exc, asyncio.CancelledError):
                log.error(f"{name} task crashed: {exc}", exc_info=True)

    task.add_done_callback(cb)
