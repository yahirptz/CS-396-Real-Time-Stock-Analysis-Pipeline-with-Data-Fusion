import os
import json
import redis
import time
import requests
from datetime import datetime

# Config
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
TICKERS = ['AAPL', 'NVDA', 'META']

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

print("Fundamental data service started")

def fetch_company_overview(ticker):
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'OVERVIEW',
        'symbol': ticker,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if 'Symbol' in data:
            return {
                'marketCap': int(data.get('MarketCapitalization', 0)) if data.get('MarketCapitalization') else None,
                'trailingPE': float(data.get('PERatio', 0)) if data.get('PERatio') != 'None' else None,
                'fiftyTwoWeekHigh': float(data.get('52WeekHigh', 0)) if data.get('52WeekHigh') else None,
                'fiftyTwoWeekLow': float(data.get('52WeekLow', 0)) if data.get('52WeekLow') else None
            }
        else:
            return None
            
    except Exception as e:
        return None

while True:
    for ticker in TICKERS:
        try:
            info = fetch_company_overview(ticker)

            if info:
                data = {
                    'ticker': ticker,
                    'fundamentals': info,
                    'last_updated': datetime.now().isoformat()
                }

                r.set(f'fundamental:{ticker}', json.dumps(data))

        except Exception as e:
            print(f"Error: {ticker}")
        
        time.sleep(15)
    
    time.sleep(300)