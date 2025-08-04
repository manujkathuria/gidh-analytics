# service/websocket_client.py
import asyncio
from datetime import datetime, time, timezone
from typing import Dict, List
import pytz

from kiteconnect import KiteTicker

from service.logger import log
import service.config as config
from service.models import TickData, OrderDepth, DepthLevel


class WebSocketClient:
    """
    Handles the connection and data reception from the KiteTicker WebSocket.
    """

    def __init__(self, queue: asyncio.Queue, instrument_map: Dict[str, int], loop: asyncio.AbstractEventLoop):
        """
        Initializes the WebSocket client.

        Args:
            queue: The asyncio.Queue to put the received tick data into.
            instrument_map: A dictionary mapping stock names to their instrument tokens.
            loop: The main asyncio event loop.
        """
        self.kws = KiteTicker(config.KITE_API_KEY, config.KITE_ACCESS_TOKEN)
        self.queue = queue
        self.loop = loop
        self.tokens = list(instrument_map.values())
        self.instrument_token_to_name = {v: k for k, v in instrument_map.items()}

        # --- Timezone and Trading Hours Configuration ---
        self.ist_tz = pytz.timezone('Asia/Kolkata')
        self.trading_start_time = time(9, 15)
        self.trading_end_time = time(15, 30)

        # Assign all the callbacks
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect

    def _parse_tick(self, tick_dict: Dict) -> TickData:
        """
        Parses a raw tick dictionary from Kite into a TickData object,
        ensuring the timestamp is valid and converted to UTC.
        """
        token = tick_dict.get('instrument_token')
        stock_name = self.instrument_token_to_name.get(token)

        if not stock_name:
            log.warning(f"Received tick for unknown instrument token: {token}")
            return None


        return TickData(
            timestamp=datetime.now(timezone.utc),
            instrument_token=token,
            stock_name=stock_name,
            last_price=tick_dict.get('last_price'),
            last_traded_quantity=tick_dict.get('last_quantity'),
            average_traded_price=tick_dict.get('average_price'),
            volume_traded=tick_dict.get('volume'),
            total_buy_quantity=tick_dict.get('total_buy_quantity'),
            total_sell_quantity=tick_dict.get('total_sell_quantity'),
            ohlc_open=tick_dict.get('ohlc', {}).get('open'),
            ohlc_high=tick_dict.get('ohlc', {}).get('high'),
            ohlc_low=tick_dict.get('ohlc', {}).get('low'),
            ohlc_close=tick_dict.get('ohlc', {}).get('close'),
            change=tick_dict.get('change'),
            depth=None
        )

    def on_ticks(self, ws, ticks: List[Dict]):
        """Callback function to receive ticks."""
        now_ist = datetime.now(self.ist_tz).time()
        if not self.trading_start_time <= now_ist <= self.trading_end_time:
            return  # Silently drop ticks outside of trading hours

        log.debug(f"Received a batch of {len(ticks)} ticks.")
        if not self.loop.is_running():
            log.warning("Event loop is not running. Cannot queue ticks.")
            return

        for tick_dict in ticks:
            parsed_tick = self._parse_tick(tick_dict)
            if parsed_tick:
                message = {'type': 'tick', 'data': parsed_tick}
                self.loop.call_soon_threadsafe(self.queue.put_nowait, message)

    def on_connect(self, ws, response):
        """Callback on successful connection."""
        log.info("WebSocket connected. Subscribing to instruments...")
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)
        log.info(f"Subscribed to {len(self.tokens)} instruments in MODE_FULL.")

    def on_close(self, ws, code, reason):
        """Callback on connection close."""
        # --- IGNORE 1006 ON MANUAL SHUTDOWN ---
        if code == 1006:
            log.info(f"WebSocket connection closed uncleanly (Code: {code}). This is expected during shutdown.")
        else:
            log.warning(f"WebSocket connection closed. Code: {code}, Reason: {reason}")

    def on_error(self, ws, code, reason):
        """Callback on connection error."""
        # --- IGNORE 1006 ON MANUAL SHUTDOWN ---
        if code == 1006:
            log.warning(f"WebSocket handshake timeout (Code: {code}). This is expected during shutdown.")
        else:
            log.error(f"WebSocket error. Code: {code}, Reason: {reason}")


    def on_reconnect(self, ws, attempts_count):
        """Callback when reconnecting."""
        log.info(f"Reconnecting WebSocket: attempt {attempts_count}")

    def on_noreconnect(self, ws):
        """Callback when reconnection fails."""
        log.error("WebSocket reconnect failed after maximum attempts.")

    def connect(self):
        """Starts the WebSocket connection in a separate thread."""
        log.info("Starting KiteTicker WebSocket in threaded mode.")
        self.kws.connect(threaded=True)

    def close(self):
        """Closes the WebSocket connection gracefully."""
        if self.kws and self.kws.is_connected():
            log.info("Closing WebSocket connection.")
            self.kws.close(code=1000, reason="Normal closure")