import os
import json
import redis
import time
import yfinance as yf
from datetime import datetime

# Config
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
TICKERS = ['AAPL', 'NVDA', 'META']

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

print("Fundamental data service started")

while True:
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            data = {
                'ticker': ticker,
                'fundamentals': info,
                'last_updated': datetime.now().isoformat()
            }

            r.set(f'fundamental:{ticker}', json.dumps(data))

        except Exception as e:
            print(f"Error: {ticker}")
    
    time.sleep(300)
