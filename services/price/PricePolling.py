import os
import json
import time
import redis
import yfinance as yf
from datetime import datetime

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
TICKERS = ['AAPL', 'NVDA', 'META']

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

print("Price service started")

while True:
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period='1d', interval='5m')
            
            data = {
                'ticker': ticker,
                'prices': df.to_dict('records'),
                'last_updated': datetime.now().isoformat()
            }
            
            r.set(f'price:{ticker}', json.dumps(data))
            
        except Exception as e:
            print(f"Error: {ticker}")
    
    time.sleep(300)
