from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import os

class KiteAdapter:
    def __init__(self):
        self.kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
        self.kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

    def fetch_daily_candles(self, token, days=60):
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)
        try:
            return self.kite.historical_data(token, from_date, to_date, "day")
        except Exception as e:
            print(f"Kite Fetch Error for {token}: {e}")
            return []