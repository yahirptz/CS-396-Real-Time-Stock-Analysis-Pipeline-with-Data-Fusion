import os
import json
import time
import redis
import requests
from datetime import datetime

# Configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
TICKERS = ['AAPL', 'NVDA', 'META']

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

print("Price service started (using Alpha Vantage)")
print(f"API Key configured: {API_KEY is not None}")

def fetch_intraday_data(ticker):
    """Fetch 5-minute intraday data from Alpha Vantage"""
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': ticker,
        'interval': '5min',
        'apikey': API_KEY,
        'outputsize': 'compact'
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if 'Time Series (5min)' in data:
            time_series = data['Time Series (5min)']
            prices = []
            
            for timestamp, values in time_series.items():
                prices.append({
                    'Datetime': timestamp,
                    'Open': float(values['1. open']),
                    'High': float(values['2. high']),
                    'Low': float(values['3. low']),
                    'Close': float(values['4. close']),
                    'Volume': int(values['5. volume'])
                })
            
            return prices
        else:
            print(f"Error fetching {ticker}: {data.get('Note', data.get('Error Message', 'Unknown error'))}")
            return None
            
    except Exception as e:
        print(f"Exception fetching {ticker}: {e}")
        return None

while True:
    for ticker in TICKERS:
        try:
            prices = fetch_intraday_data(ticker)
            
            if prices:
                data = {
                    'ticker': ticker,
                    'prices': prices,
                    'last_updated': datetime.now().isoformat()
                }
                
                r.set(f'price:{ticker}', json.dumps(data))
                print(f"Updated price data for {ticker} - {len(prices)} data points")
            else:
                print(f"No data retrieved for {ticker}")
                
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
    
    time.sleep(60) 