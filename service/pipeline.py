import asyncio
import asyncpg
from collections import deque

from service.db_schema import setup_schema
# Import from our new service modules
from service.logger import log
import service.config as config
from service.parameters import INSTRUMENT_MAP

class DataPipeline:
    """
    The main class for handling the data pipeline.
    This class is responsible for:
    - Connecting to the database.
    - Managing the WebSocket connection for real-time data.
    - Processing and storing incoming data.
    - Handling different data sources (real-time vs. backtesting).
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


    async def db_writer(self):
        """Placeholder for the coroutine that writes data to the database."""
        log.info("Placeholder for: `db_writer`")
        pass

    async def insert_live_ticks(self, ticks):
        """Placeholder for inserting a batch of ticks."""
        log.info("Placeholder for: `insert_live_ticks`")
        pass

    async def insert_order_depths(self, depths):
        """Placeholder for inserting a batch of order depth updates."""
        log.info("Placeholder for: `insert_order_depths`")
        pass

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
        """Placeholder for reading data from a file for backtesting."""
        log.info("Placeholder for: `start_file_reader`")
        pass

    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        await self.initialize_db()

        # The following lines are commented out as their targets are placeholders.
        # We will uncomment and implement them in the subsequent steps.
        # writer_task = asyncio.create_task(self.db_writer())

        try:
            await self.start_data_source()
        finally:
            log.info("Shutting down data pipeline...")
            if self.db_pool:
                await self.db_pool.close()
                log.info("Database connection pool closed.")
            # if writer_task:
            #     writer_task.cancel()
            #     await asyncio.gather(writer_task, return_exceptions=True)
            log.info("Data pipeline shut down gracefully.")
