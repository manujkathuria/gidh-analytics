from kiteconnect import KiteConnect
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("KITE_API_KEY")
api_secret = os.getenv("KITE_API_SECRET")

kite = KiteConnect(api_key=api_key)

print("Login URL:", kite.login_url())
request_token = input("Please enter your request token: ").strip()

try:
    data = kite.generate_session(request_token, api_secret=api_secret)
    print("✅ Access Token:", data["access_token"])
except Exception as e:
    print("❌ Error:", e)
