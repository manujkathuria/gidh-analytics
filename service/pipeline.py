# wealth-wave-ventures/gidh-analytics/wealth-wave-ventures-gidh-analytics-5ad4a7c6bd53291eeab53ca042856af359c2ca1f/service/pipeline.py
import asyncio
import asyncpg
from collections import deque

from service.db_schema import setup_schema
# Import from our new service modules
from service.logger import log
import service.config as config
from service.parameters import INSTRUMENT_MAP
from service.file_reader import FileReader  # Import the new FileReader
import service.db_writer as db_writer  # Import the new db_writer


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
        self.kws = None
        self.data_window = deque(maxlen=3600)
        self.db_queue = asyncio.Queue()
        # Batching configuration
        self.batch_size = 1000
        self.batch_interval = 5  # seconds
        log.info(f"DataPipeline initialized in '{self.mode}' mode for {len(self.instruments)} instruments.")

    async def initialize_db(self):
        """Creates an asyncpg connection pool and ensures tables are set up."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                host=config.DB_HOST,
                port=config.DB_PORT,
                database=config.DB_NAME
            )
            log.info(f"Successfully connected to the database '{config.DB_NAME}'.")
            # Ensure the necessary tables and hypertables exist.
            await setup_schema(self.db_pool)
        except Exception as e:
            log.error(f"Failed to connect to or initialize the database: {e}")
            raise

    async def db_writer_coroutine(self):
        """
        Coroutine that collects data from the queue and writes to the DB in batches.
        """
        log.info("DB writer coroutine started.")
        ticks_batch = []
        last_write_time = asyncio.get_event_loop().time()

        while True:
            try:
                # Wait for an item from the queue, but with a timeout
                timeout = max(0, self.batch_interval - (asyncio.get_event_loop().time() - last_write_time))
                item = await asyncio.wait_for(self.db_queue.get(), timeout=timeout)

                # Add item to the appropriate batch
                if item['type'] == 'tick':
                    ticks_batch.append(item['data'])

                self.db_queue.task_done()

            except asyncio.TimeoutError:
                # Timeout reached, process whatever is in the batch
                pass

            # Write to DB if a batch is full or the time interval has passed
            time_since_last_write = asyncio.get_event_loop().time() - last_write_time
            if (len(ticks_batch) >= self.batch_size or
                    (time_since_last_write >= self.batch_interval and ticks_batch)):

                if ticks_batch:
                    # Create a list of ticks that have depth data
                    ticks_with_depth = [t for t in ticks_batch if t.depth]

                    # Insert all ticks
                    await db_writer.batch_insert_ticks(self.db_pool, ticks_batch)

                    # Insert order depth data for ticks that have it
                    if ticks_with_depth:
                        await db_writer.batch_insert_order_depths(self.db_pool, ticks_with_depth)

                    ticks_batch.clear()

                last_write_time = asyncio.get_event_loop().time()

    def setup_websocket(self):
        """Placeholder for setting up the Kite Ticker WebSocket client."""
        log.info("Placeholder for: `setup_websocket`")
        pass

    async def start_data_source(self):
        """Starts the data source based on the selected mode."""
        log.info(f"Attempting to start data source in '{self.mode}' mode.")
        if self.mode == 'realtime':
            await self.start_websocket()
        elif self.mode == 'backtesting':
            await self.start_file_reader()
        else:
            log.error(f"Invalid mode specified: {self.mode}")
            raise ValueError(f"Invalid mode: {self.mode}")

    async def start_websocket(self):
        """Placeholder for starting the Kite Ticker WebSocket connection."""
        log.info("Placeholder for: `start_websocket`")
        pass

    async def start_file_reader(self):
        """Initializes and runs the file reader for backtesting."""
        log.info("Starting file reader for backtesting.")
        file_reader = FileReader()
        # The stream_ticks method will run and put data into our queue
        await file_reader.stream_ticks(self.db_queue)

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        writer_task = asyncio.create_task(self.db_writer_coroutine())

        try:
            await self.start_data_source()
            # Wait for the queue to be fully processed after the source is done
            await self.db_queue.join()
        finally:
            log.info("Shutting down data pipeline...")
            writer_task.cancel()
            await asyncio.gather(writer_task, return_exceptions=True)
            if self.db_pool:
                await self.db_pool.close()
                log.info("Database connection pool closed.")
            log.info("Data pipeline shut down gracefully.")