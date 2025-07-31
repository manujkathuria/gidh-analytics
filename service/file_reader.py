import asyncio
import csv
import heapq
from dateutil.parser import isoparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from service.logger import log
import service.config as config
from service.models import TickData, OrderDepth, DepthLevel
from service.parameters import INSTRUMENT_MAP

class FileReader:
    """
    Reads historical tick and depth data from CSV files for backtesting.
    """
    def __init__(self):
        self.data_directory = Path(config.BACKTEST_DATA_DIRECTORY)
        self.date_str = config.BACKTEST_DATE_STR
        self.sleep_duration = config.BACKTEST_SLEEP_DURATION
        self.instrument_map = INSTRUMENT_MAP
        self.base_path = self.data_directory / self.date_str
        self.ticks_path = self.base_path / "live_ticks"
        self.depth_path = self.base_path / "live_order_depth"

    def _parse_timestamp(self, ts_str: str) -> datetime:
        ts_str = ts_str.strip().replace(' ', 'T', 1)
        return isoparse(ts_str)

    def _parse_tick_row(self, row: Dict, instrument_token: int) -> TickData:
        """Safely parses a dictionary row from a CSV into a TickData object."""
        # Helper to safely convert to float
        def to_float(val):
            return float(val) if val and val.strip() else None
        # Helper to safely convert to int
        def to_int(val):
            return int(float(val)) if val and val.strip() else None

        return TickData(
            timestamp=self._parse_timestamp(row['timestamp']),
            stock_name=row['stock_name'],
            instrument_token=instrument_token,
            last_price=to_float(row.get('last_price')),
            last_traded_quantity=to_int(row.get('last_traded_quantity')),
            average_traded_price=to_float(row.get('average_traded_price')),
            volume_traded=to_int(row.get('volume_traded')),
            total_buy_quantity=to_int(row.get('total_buy_quantity')),
            total_sell_quantity=to_int(row.get('total_sell_quantity')),
            ohlc_open=to_float(row.get('ohlc_open')),
            ohlc_high=to_float(row.get('ohlc_high')),
            ohlc_low=to_float(row.get('ohlc_low')),
            ohlc_close=to_float(row.get('ohlc_close')),
            change=to_float(row.get('change')),
        )

    async def _load_ticks_for_stock(self, stock_name: str, instrument_token: int) -> List[TickData]:
        """Loads all tick data for a single stock from its CSV file."""
        file_path = self.ticks_path / f"live_ticks_{stock_name}.csv"
        if not file_path.exists():
            log.warning(f"Tick file not found for {stock_name}: {file_path}")
            return []

        ticks = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ticks.append(self._parse_tick_row(row, instrument_token))
            # Sort by timestamp just in case the CSV is not ordered
            ticks.sort(key=lambda t: t.timestamp)
            return ticks
        except Exception as e:
            log.error(f"Failed to load ticks for {stock_name} from {file_path}: {e}")
            return []

    async def _load_all_data(self) -> List[List[TickData]]:
        """Concurrently loads all tick data for all instruments in the map."""
        log.info(f"Loading backtest data from: {self.base_path}")
        tasks = []
        for stock_name, token in self.instrument_map.items():
            tasks.append(self._load_ticks_for_stock(stock_name, token))

        # Run all loading tasks in parallel
        all_stocks_data = await asyncio.gather(*tasks)
        # Filter out any empty lists from failed loads
        return [stock_data for stock_data in all_stocks_data if stock_data]

    async def stream_ticks(self, queue: asyncio.Queue):
        """
        Loads, merges, and streams tick data chronologically to a queue.
        This method replicates the Go code's logic of using a min-heap to merge
        multiple sorted lists of ticks.
        """
        all_stocks_data = await self._load_all_data()
        if not all_stocks_data:
            log.error("No tick data loaded. Backtesting cannot proceed.")
            return

        # Initialize the min-heap. Each item is a tuple:
        # (timestamp, list_index, item_index_in_list, tick_object)
        min_heap = []
        for i, stock_ticks in enumerate(all_stocks_data):
            if stock_ticks:
                first_tick = stock_ticks[0]
                heapq.heappush(min_heap, (first_tick.timestamp, i, 0, first_tick))

        log.info("Starting to stream historical ticks...")
        ticks_sent = 0
        while min_heap:
            # Pop the tick with the earliest timestamp
            _, list_idx, item_idx, tick = heapq.heappop(min_heap)

            # Put the tick onto the pipeline's queue
            await queue.put({'type': 'tick', 'data': tick})
            ticks_sent += 1

            # Push the next tick from the same stock list onto the heap
            next_item_idx = item_idx + 1
            if next_item_idx < len(all_stocks_data[list_idx]):
                next_tick = all_stocks_data[list_idx][next_item_idx]
                heapq.heappush(min_heap, (next_tick.timestamp, list_idx, next_item_idx, next_tick))

            # Sleep to simulate real-time tick flow
            await asyncio.sleep(self.sleep_duration)

        log.info(f"Finished streaming historical data. Total ticks sent: {ticks_sent}")
