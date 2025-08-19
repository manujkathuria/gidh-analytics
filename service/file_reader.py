# wealth-wave-ventures/gidh-analytics/wealth-wave-ventures-gidh-analytics-5ad4a7c6bd53291eeab53ca042856af359c2ca1f/service/file_reader.py
import asyncio
import csv
import heapq
from dateutil.parser import isoparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

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
        """
        Parses a timestamp string and normalizes it by removing microseconds
        to ensure ticks and depth data can be matched despite nanosecond differences.
        """
        ts_str = ts_str.strip().replace(' ', 'T', 1)
        dt = isoparse(ts_str)
        # FIX: Truncate microseconds to handle nanosecond-level mismatches
        return dt.replace(microsecond=0)

    def _parse_tick_row(self, row: Dict, instrument_token: int, depth_data: Dict) -> TickData:
        """Safely parses a dictionary row from a CSV into a TickData object."""

        # Helper to safely convert to float
        def to_float(val):
            return float(val) if val and val.strip() else None

        # Helper to safely convert to int
        def to_int(val):
            return int(float(val)) if val and val.strip() else None

        timestamp = self._parse_timestamp(row['timestamp'])
        stock_name = row['stock_name']

        tick = TickData(
            timestamp=timestamp,
            stock_name=stock_name,
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

        # Attach depth data if it exists for this timestamp and stock
        if (timestamp, stock_name) in depth_data:
            tick.depth = depth_data[(timestamp, stock_name)]

        return tick

    async def _load_ticks_for_stock(self, stock_name: str, instrument_token: int, depth_data: Dict) -> List[TickData]:
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
                    ticks.append(self._parse_tick_row(row, instrument_token, depth_data))
            # Sort by timestamp just in case the CSV is not ordered
            ticks.sort(key=lambda t: t.timestamp)
            return ticks
        except Exception as e:
            log.error(f"Failed to load ticks for {stock_name} from {file_path}: {e}")
            return []

    async def _load_all_depth_data(self) -> Dict:
        """Loads all order depth data from all files into a dictionary."""
        depth_data = {}
        for stock_name, token in self.instrument_map.items():
            file_path = self.depth_path / f"live_order_depth_{stock_name}.csv"
            if not file_path.exists():
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ts = self._parse_timestamp(row['timestamp'])
                    if (ts, stock_name) not in depth_data:
                        depth_data[(ts, stock_name)] = OrderDepth(
                            timestamp=ts,
                            stock_name=stock_name,
                            instrument_token=token,
                            buy=[],
                            sell=[]
                        )

                    level = DepthLevel(
                        price=float(row['price']),
                        quantity=int(row['quantity']),
                        orders=int(row.get('orders', 0))  # handle missing orders
                    )

                    if row['side'] == 'buy':
                        depth_data[(ts, stock_name)].buy.append(level)
                    elif row['side'] == 'sell':
                        depth_data[(ts, stock_name)].sell.append(level)
        return depth_data

    async def _load_all_data(self) -> List[List[TickData]]:
        """Concurrently loads all tick data for all instruments."""
        log.info(f"Loading backtest data from: {self.base_path}")

        depth_data = await self._load_all_depth_data()

        tasks = []
        for stock_name, token in self.instrument_map.items():
            tasks.append(self._load_ticks_for_stock(stock_name, token, depth_data))

        # Run all loading tasks in parallel
        all_stocks_data = await asyncio.gather(*tasks)
        # Filter out any empty lists from failed loads
        return [stock_data for stock_data in all_stocks_data if stock_data]

    async def stream_ticks(self, queue: asyncio.Queue):
        """
        Loads, merges, and streams tick and depth data chronologically to a queue.
        """
        all_stocks_data = await self._load_all_data()
        if not all_stocks_data:
            log.error("No data loaded. Backtesting cannot proceed.")
            return

        min_heap = []
        for i, stock_data in enumerate(all_stocks_data):
            if stock_data:
                first_item = stock_data[0]
                heapq.heappush(min_heap, (first_item.timestamp, i, 0, first_item))

        log.info("Starting to stream historical data...")
        items_sent = 0
        while min_heap:
            _, list_idx, item_idx, item = heapq.heappop(min_heap)

            await queue.put({'type': 'tick', 'data': item})
            items_sent += 1

            next_item_idx = item_idx + 1
            if next_item_idx < len(all_stocks_data[list_idx]):
                next_item = all_stocks_data[list_idx][next_item_idx]
                heapq.heappush(min_heap, (next_item.timestamp, list_idx, next_item_idx, next_item))

            await asyncio.sleep(self.sleep_duration)

        log.info(f"Finished streaming historical data. Total items sent: {items_sent}")
