#!/usr/bin/env python3
"""
Short Volume Collector
=======================
Pulls daily short volume data from Fintel API.
Stores in SQLite database for backtesting and cross-signal detection.

Run daily after market close (e.g., 23:30 UTC).
"""

import sqlite3
import requests
import time
import os
from datetime import datetime, timezone

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_PATH, 'short_volume.db')

# API config
FINTEL_API_KEY = os.environ.get('FINTEL_API_KEY', '')
FINTEL_BASE_URL = 'https://api.fintel.io/web/v/0.0/ss/us'

# Rate limiting - be nice to the API
REQUESTS_PER_SECOND = 2
DELAY_BETWEEN_REQUESTS = 1.0 / REQUESTS_PER_SECOND

# Get tickers from options scanner if available, otherwise use default list
def get_tickers():
    """Load tickers from options scanner's ticker list."""
    tickers_file = os.path.join(BASE_PATH, 'options_scanner', 'tickers.py')
    
    if os.path.exists(tickers_file):
        try:
            import sys
            sys.path.insert(0, os.path.join(BASE_PATH, 'options_scanner'))
            from tickers import RUSSELL_1000_TICKERS as TICKERS
            print(f"Loaded {len(TICKERS)} tickers from options scanner")
            return TICKERS
        except Exception as e:
            print(f"Error loading tickers: {e}")
    
    # Fallback to a core list
    return [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'AMD', 'INTC',
        'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'V', 'MA', 'PYPL',
        'XOM', 'CVX', 'COP', 'SLB', 'OXY',
        'JNJ', 'PFE', 'UNH', 'MRK', 'ABBV', 'LLY',
        'DIS', 'NFLX', 'CMCSA', 'T', 'VZ',
        'HD', 'LOW', 'TGT', 'WMT', 'COST',
        'BA', 'CAT', 'DE', 'GE', 'MMM',
        'SPY', 'QQQ', 'IWM', 'DIA', 'XLF', 'XLE', 'XLK'
    ]


# =============================================================================
# DATABASE
# =============================================================================

def init_database():
    """Create database tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS short_volume (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            market_date TEXT NOT NULL,
            short_volume INTEGER,
            total_volume INTEGER,
            short_volume_ratio REAL,
            collected_at TEXT NOT NULL,
            UNIQUE(ticker, market_date)
        )
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_short_volume_ticker 
        ON short_volume(ticker)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_short_volume_date 
        ON short_volume(market_date)
    ''')
    
    # Track API calls for rate limit awareness
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            collection_date TEXT NOT NULL,
            tickers_requested INTEGER,
            tickers_success INTEGER,
            tickers_failed INTEGER,
            new_records INTEGER,
            duration_seconds REAL
        )
    ''')
    
    conn.commit()
    conn.close()


def save_short_volume(ticker, data_points):
    """Save short volume data to database. Returns count of new records."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    new_count = 0
    collected_at = datetime.now(timezone.utc).isoformat()
    
    for point in data_points:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO short_volume 
                (ticker, market_date, short_volume, total_volume, short_volume_ratio, collected_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                ticker,
                point.get('marketDate'),
                int(point.get('shortVolume', 0)),
                int(point.get('totalVolume', 0)),
                point.get('shortVolumeRatio'),
                collected_at
            ))
            if cursor.rowcount > 0:
                new_count += 1
        except Exception as e:
            print(f"    Error saving {ticker} {point.get('marketDate')}: {e}")
    
    conn.commit()
    conn.close()
    return new_count


def log_collection(tickers_requested, tickers_success, tickers_failed, new_records, duration):
    """Log collection run stats."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO collection_log 
        (collection_date, tickers_requested, tickers_success, tickers_failed, new_records, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        tickers_requested,
        tickers_success,
        tickers_failed,
        new_records,
        duration
    ))
    
    conn.commit()
    conn.close()


# =============================================================================
# API
# =============================================================================

def fetch_short_volume(ticker):
    """Fetch short volume data for a single ticker."""
    if not FINTEL_API_KEY:
        raise ValueError("FINTEL_API_KEY not set")
    
    url = f"{FINTEL_BASE_URL}/{ticker}"
    headers = {
        'X-API-KEY': FINTEL_API_KEY,
        'Accept': 'application/json'
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None  # Ticker not found
    else:
        response.raise_for_status()


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Skip weekends
    if datetime.now(timezone.utc).weekday() >= 5:
        print("Weekend - skipping")
        return
    
    print("=" * 60)
    print("SHORT VOLUME COLLECTOR")
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    if not FINTEL_API_KEY:
        print("ERROR: FINTEL_API_KEY environment variable not set")
        print("Set it with: export FINTEL_API_KEY='your_key_here'")
        return
    
    init_database()
    tickers = get_tickers()
    
    print(f"\nCollecting short volume for {len(tickers)} tickers...")
    print(f"Rate limit: {REQUESTS_PER_SECOND} requests/second")
    print()
    
    start_time = time.time()
    success_count = 0
    fail_count = 0
    total_new_records = 0
    
    for i, ticker in enumerate(tickers):
        try:
            data = fetch_short_volume(ticker)
            
            if data and 'data' in data:
                new_records = save_short_volume(ticker, data['data'])
                total_new_records += new_records
                success_count += 1
                
                if new_records > 0:
                    print(f"  [{i+1}/{len(tickers)}] {ticker}: +{new_records} new records")
                else:
                    print(f"  [{i+1}/{len(tickers)}] {ticker}: up to date")
            else:
                print(f"  [{i+1}/{len(tickers)}] {ticker}: no data")
                fail_count += 1
            
            # Rate limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
        except Exception as e:
            print(f"  [{i+1}/{len(tickers)}] {ticker}: ERROR - {e}")
            fail_count += 1
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    duration = time.time() - start_time
    
    # Log the collection
    log_collection(len(tickers), success_count, fail_count, total_new_records, duration)
    
    # Summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Tickers processed: {success_count}/{len(tickers)}")
    print(f"New records added: {total_new_records}")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Database: {DB_PATH}")
    print()
    
    # Show database stats
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM short_volume")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM short_volume")
    total_tickers = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(market_date), MAX(market_date) FROM short_volume")
    date_range = cursor.fetchone()
    
    conn.close()
    
    print(f"Total records: {total_records:,}")
    print(f"Tickers with data: {total_tickers}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    print()
    print("Done.")


if __name__ == '__main__':
    main()