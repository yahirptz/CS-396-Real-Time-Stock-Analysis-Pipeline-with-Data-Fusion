import os
import json
import redis
from flask import Flask, render_template, jsonify
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Config
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
TICKERS = ['AAPL', 'NVDA', 'META']

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def calculate_sma(prices, period=20):
    """Calculate Simple Moving Average"""
    if len(prices) < period:
        return None
    return sum(prices[-period:]) / period

def get_stock_data(ticker):
    """Fetch and fuse price and fundamental data for a ticker"""
    try:
        # Get price data
        price_data = r.get(f'price:{ticker}')
        fundamental_data = r.get(f'fundamental:{ticker}')
        
        if not price_data or not fundamental_data:
            return None
        
        price_info = json.loads(price_data)
        fundamental_info = json.loads(fundamental_data)
        
        # Extract prices for technical analysis
        prices = [record.get('Close', 0) for record in price_info['prices'] if 'Close' in record]
        
        # Calculate SMA
        sma_20 = calculate_sma(prices, 20) if len(prices) >= 20 else None
        
        # Get current price
        current_price = prices[-1] if prices else None
        
        fundamentals = fundamental_info.get('fundamentals', {})
        
        # Fused data
        fused_data = {
            'ticker': ticker,
            'current_price': current_price,
            'sma_20': sma_20,
            'prices': prices,
            'market_cap': fundamentals.get('marketCap'),
            'pe_ratio': fundamentals.get('trailingPE'),
            'week_52_high': fundamentals.get('fiftyTwoWeekHigh'),
            'week_52_low': fundamentals.get('fiftyTwoWeekLow'),
            'price_last_updated': price_info.get('last_updated'),
            'fundamental_last_updated': fundamental_info.get('last_updated')
        }
        
        return fused_data
        
    except Exception as e:
        print(f"Error getting data for {ticker}: {e}")
        return None

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html', tickers=TICKERS)

@app.route('/api/stock/<ticker>')
def get_stock(ticker):
    """API endpoint to get stock data"""
    if ticker not in TICKERS:
        return jsonify({'error': 'Invalid ticker'}), 400
    
    data = get_stock_data(ticker)
    if data is None:
        return jsonify({'error': 'Data not available'}), 404
    
    return jsonify(data)

@app.route('/api/all')
def get_all_stocks():
    
    all_data = {}
    for ticker in TICKERS:
        data = get_stock_data(ticker)
        if data:
            all_data[ticker] = data
    
    return jsonify(all_data)

if __name__ == '__main__':
    print("Analysis and Visualization Service started")
    app.run(host='0.0.0.0', port=5000, debug=True)