import asyncio
import asyncpg
from collections import deque

from service.db_schema import setup_schema
# Import from our new service modules
from service.logger import log
import service.config as config
from service.parameters import INSTRUMENT_MAP
from service.file_reader import FileReader # Import the new FileReader
# from service.db_schema import setup_schema # Assuming you created this file

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
        while True:
            # A simple loop to consume items from the queue for now
            item = await self.db_queue.get()
            log.info(f"DB Writer received: {item['data'].stock_name} @ {item['data'].last_price}")
            self.db_queue.task_done()


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
        """Initializes and runs the file reader for backtesting."""
        log.info("Starting file reader for backtesting.")
        file_reader = FileReader()
        # The stream_ticks method will run and put data into our queue
        await file_reader.stream_ticks(self.db_queue)


    async def run(self):
        """The main entry point for the data pipeline."""
        log.info("Starting pipeline run...")
        # await self.initialize_db() # Uncomment when ready

        writer_task = asyncio.create_task(self.db_writer())

        try:
            await self.start_data_source()
            # Wait for the queue to be fully processed after the source is done
            await self.db_queue.join()
        finally:
            log.info("Shutting down data pipeline...")
            writer_task.cancel()
            await asyncio.gather(writer_task, return_exceptions=True)
            log.info("Data pipeline shut down gracefully.")
