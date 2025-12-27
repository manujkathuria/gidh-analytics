from datetime import datetime, timedelta

from kiteconnect import KiteConnect
from common import config
from common.models import Candle


class KiteHistoricalAdapter:
    def __init__(self):
        self.kite = KiteConnect(api_key=config.KITE_API_KEY)
        self.kite.set_access_token(config.KITE_ACCESS_TOKEN)

    def fetch_daily_candles(self, instrument_token: int, days: int = 60) -> list[Candle]:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)

        raw_data = self.kite.historical_data(instrument_token, from_date, to_date, "day")
        return [
            Candle(
                timestamp=d['date'],
                open=d['open'],
                high=d['high'],
                low=d['low'],
                close=d['close']
            ) for d in raw_data
        ]