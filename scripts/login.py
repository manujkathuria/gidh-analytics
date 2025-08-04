import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# Load environment variables from .env files
load_dotenv("../.env")
load_dotenv(".env")

api_key = os.getenv("KITE_API_KEY")
api_secret = os.getenv("KITE_API_SECRET")

# Create a KiteConnect instance
kite = KiteConnect(api_key=api_key)

# Print login URL to get request token
print("Login URL:", kite.login_url())

# Simulate getting request token from user input
request_token = input("Please enter your request token: ").strip()

try:
    # Generate session and get access token
    data = kite.generate_session(request_token, api_secret=api_secret)
    print("Access Token:", data["access_token"])
except Exception as e:
    print("Error:", e)
