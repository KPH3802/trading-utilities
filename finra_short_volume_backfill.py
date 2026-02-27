#!/usr/bin/env python3
"""
FINRA Short Volume Backfill via Query API
==========================================
Pulls historical Reg SHO Daily Short Sale Volume data from FINRA's Query API.
Stores in the same short_volume.db schema used by the daily collector.

Usage:
    python3 finra_short_volume_backfill.py            # Run backfill (resumes automatically)
    python3 finra_short_volume_backfill.py stats       # Show DB stats
    python3 finra_short_volume_backfill.py metadata    # Check API metadata/fields

Requirements:
    pip install requests

FINRA API docs: https://developer.finra.org/docs
Dataset: OTCMarket / regShoDaily
Auth: OAuth2 client_credentials via FIP endpoint
"""

import sqlite3
import requests
import time
import base64
import json
import os
import sys
from datetime import datetime, timedelta, timezone

# =============================================================================
# CONFIGURATION
# =============================================================================

# FINRA API credentials
CLIENT_ID = 'f757a07bd87047c38bb6'
CLIENT_SECRET = '45$6Pr*vj^A4^*Y1Nso3jr*k5dQ&W&Lj'

# Endpoints
TOKEN_URL = 'https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials'
API_BASE = 'https://api.finra.org'
DATASET_GROUP = 'otcmarket'
DATASET_NAME = 'regShoDaily'
DATA_URL = f'{API_BASE}/data/group/{DATASET_GROUP}/name/{DATASET_NAME}'
METADATA_URL = f'{API_BASE}/metadata/group/{DATASET_GROUP}/name/{DATASET_NAME}'

# Database
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_PATH, 'short_volume.db')

# Backfill settings
START_DATE = '2020-01-02'  # Start of backfill
END_DATE = None            # None = today
BATCH_DAYS = 5             # Days per API call (each day has ~8K+ tickers)
ROWS_PER_REQUEST = 5000    # Max rows per page
DELAY_BETWEEN_REQUESTS = 1.0
MAX_RETRIES = 3

# =============================================================================
# AUTHENTICATION
# =============================================================================

_access_token = None
_token_expiry = None

def get_access_token():
    """Get OAuth2 access token using client credentials grant."""
    global _access_token, _token_expiry
    
    if _access_token and _token_expiry and datetime.now() < _token_expiry - timedelta(seconds=60):
        return _access_token
    
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        'Authorization': f'Basic {encoded}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(TOKEN_URL, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                _access_token = data.get('access_token')
                expires_in = int(data.get('expires_in', 3600))
                _token_expiry = datetime.now() + timedelta(seconds=expires_in)
                print(f"  [AUTH] Token obtained, expires in {expires_in}s")
                return _access_token
            else:
                print(f"  [AUTH] Failed (HTTP {response.status_code}): {response.text[:200]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
        except Exception as e:
            print(f"  [AUTH] Error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    
    raise Exception("Failed to obtain access token after retries")


# =============================================================================
# API CALLS
# =============================================================================

def check_metadata():
    """Fetch dataset metadata to see available fields."""
    token = get_access_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    
    response = requests.get(METADATA_URL, headers=headers, timeout=30)
    
    if response.status_code == 200:
        meta = response.json()
        print("\n=== DATASET METADATA ===")
        print(json.dumps(meta, indent=2)[:3000])
        return meta
    else:
        print(f"Metadata request failed: {response.status_code} - {response.text[:300]}")
        return None


def fetch_short_volume_batch(date_start, date_end, offset=0):
    """
    Fetch short volume data for a date range via POST.
    Returns (records_list, has_more_pages).
    """
    token = get_access_token()
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # FINRA uses dateRangeFilters for date partition fields
    # offset goes as query param, limit in body
    url = DATA_URL
    if offset > 0:
        url = f'{DATA_URL}?offset={offset}'
    
    body = {
        "limit": ROWS_PER_REQUEST,
        "dateRangeFilters": [
            {
                "fieldName": "tradeReportDate",
                "startDate": date_start,
                "endDate": date_end
            }
        ]
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=body, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                has_more = len(data) == ROWS_PER_REQUEST
                return data, has_more
            elif response.status_code == 401:
                print("  [API] Token expired, refreshing...")
                global _access_token
                _access_token = None
                token = get_access_token()
                headers['Authorization'] = f'Bearer {token}'
                continue
            elif response.status_code == 429:
                wait = int(response.headers.get('Retry-After', 60))
                print(f"  [API] Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            else:
                print(f"  [API] HTTP {response.status_code}: {response.text[:300]}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(5)
        except requests.exceptions.Timeout:
            print(f"  [API] Timeout (attempt {attempt+1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(10)
        except Exception as e:
            print(f"  [API] Error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    
    return [], False


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
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_short_volume_ticker ON short_volume(ticker)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_short_volume_date ON short_volume(market_date)')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS backfill_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_start TEXT NOT NULL,
            batch_end TEXT NOT NULL,
            records_fetched INTEGER,
            records_inserted INTEGER,
            completed_at TEXT NOT NULL
        )
    ''')
    
    # Collection log table (for compatibility with daily collector)
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


def get_last_backfill_date():
    """Get the last date that was successfully backfilled."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT MAX(batch_end) FROM backfill_progress')
        result = cursor.fetchone()
        if result and result[0]:
            conn.close()
            return result[0]
    except:
        pass
    conn.close()
    return None


def save_batch(records):
    """Save a batch of FINRA records. Returns count of new records inserted."""
    if not records:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    new_count = 0
    collected_at = datetime.now(timezone.utc).isoformat()
    
    for record in records:
        try:
            ticker = record.get('securitiesInformationProcessorSymbolIdentifier', '')
            trade_date = record.get('tradeReportDate', '')
            short_vol = int(record.get('shortParQuantity', 0) or 0)
            short_exempt = int(record.get('shortExemptParQuantity', 0) or 0)
            total_vol = int(record.get('totalParQuantity', 0) or 0)
            
            if not ticker or not trade_date or total_vol == 0:
                continue
            
            total_short = short_vol + short_exempt
            ratio = round(total_short / total_vol, 4) if total_vol > 0 else 0.0
            
            cursor.execute('''
                INSERT OR IGNORE INTO short_volume 
                (ticker, market_date, short_volume, total_volume, short_volume_ratio, collected_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (ticker, trade_date, total_short, total_vol, ratio, collected_at))
            
            if cursor.rowcount > 0:
                new_count += 1
        except:
            pass
    
    conn.commit()
    conn.close()
    return new_count


def log_batch_progress(batch_start, batch_end, fetched, inserted):
    """Log completed batch for resume capability."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO backfill_progress (batch_start, batch_end, records_fetched, records_inserted, completed_at)
        VALUES (?, ?, ?, ?, ?)
    ''', (batch_start, batch_end, fetched, inserted, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


# =============================================================================
# MAIN BACKFILL
# =============================================================================

def generate_date_batches(start_date, end_date, batch_size):
    """Generate (start, end) date pairs for batched pulls."""
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    while current <= end:
        batch_end = min(current + timedelta(days=batch_size - 1), end)
        yield current.strftime('%Y-%m-%d'), batch_end.strftime('%Y-%m-%d')
        current = batch_end + timedelta(days=1)


def run_backfill():
    """Main backfill with automatic resume."""
    print("=" * 65)
    print("FINRA SHORT VOLUME BACKFILL")
    print(f"Database: {DB_PATH}")
    print(f"Dataset:  {DATASET_GROUP}/{DATASET_NAME}")
    print("=" * 65)
    
    init_database()
    
    # Resume from last completed batch
    start = START_DATE
    last_done = get_last_backfill_date()
    if last_done:
        resume_date = (datetime.strptime(last_done, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        if resume_date > start:
            start = resume_date
            print(f"\nResuming from: {start} (last completed: {last_done})")
    
    end = END_DATE or datetime.now().strftime('%Y-%m-%d')
    
    if start > end:
        print("\nBackfill already complete!")
        return
    
    print(f"\nBackfill range: {start} → {end}")
    print(f"Batch size: {BATCH_DAYS} days | Rows/page: {ROWS_PER_REQUEST}")
    print()
    
    # Test auth
    print("Step 1: Testing authentication...")
    try:
        get_access_token()
        print("  ✓ Authenticated\n")
    except Exception as e:
        print(f"  ✗ Auth failed: {e}")
        return
    
    # Check metadata on first run
    if not last_done:
        print("Step 2: Checking dataset metadata...")
        check_metadata()
        print()
    
    # Pull data
    print("Step 3: Pulling historical data...\n")
    
    batches = list(generate_date_batches(start, end, BATCH_DAYS))
    total_batches = len(batches)
    total_records = 0
    total_inserted = 0
    start_time = time.time()
    
    for batch_num, (batch_start, batch_end) in enumerate(batches, 1):
        elapsed = time.time() - start_time
        rate = total_records / max(elapsed, 1)
        
        print(f"  Batch {batch_num}/{total_batches}: {batch_start} → {batch_end}  "
              f"[{total_records:,} fetched, {total_inserted:,} new, {rate:.0f} rec/s]")
        
        batch_fetched = 0
        batch_inserted = 0
        offset = 0
        
        while True:
            records, has_more = fetch_short_volume_batch(batch_start, batch_end, offset)
            
            if not records:
                if offset == 0:
                    print(f"    No data for {batch_start} → {batch_end}")
                break
            
            inserted = save_batch(records)
            batch_fetched += len(records)
            batch_inserted += inserted
            
            if has_more:
                offset += ROWS_PER_REQUEST
                time.sleep(DELAY_BETWEEN_REQUESTS * 0.5)
            else:
                break
        
        log_batch_progress(batch_start, batch_end, batch_fetched, batch_inserted)
        total_records += batch_fetched
        total_inserted += batch_inserted
        
        time.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Progress every 20 batches
        if batch_num % 20 == 0:
            elapsed = time.time() - start_time
            eta_min = (elapsed / batch_num) * (total_batches - batch_num) / 60
            print(f"\n  --- {batch_num}/{total_batches} batches | "
                  f"{total_inserted:,} new records | ETA: {eta_min:.0f} min ---\n")
    
    # Summary
    duration = time.time() - start_time
    print()
    print("=" * 65)
    print("BACKFILL COMPLETE")
    print("=" * 65)
    print(f"Records fetched:  {total_records:,}")
    print(f"New records:      {total_inserted:,}")
    print(f"Duration:         {duration/60:.1f} minutes")
    
    # DB stats
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM short_volume")
    total_in_db = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT ticker) FROM short_volume")
    tickers = c.fetchone()[0]
    c.execute("SELECT MIN(market_date), MAX(market_date) FROM short_volume")
    dr = c.fetchone()
    conn.close()
    
    print(f"\nDatabase totals:")
    print(f"  Records:  {total_in_db:,}")
    print(f"  Tickers:  {tickers:,}")
    print(f"  Range:    {dr[0]} → {dr[1]}")


# =============================================================================
# UTILITIES
# =============================================================================

def db_stats():
    """Print current database statistics."""
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM short_volume")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT ticker) FROM short_volume")
    tickers = c.fetchone()[0]
    c.execute("SELECT MIN(market_date), MAX(market_date) FROM short_volume")
    dates = c.fetchone()
    c.execute("SELECT COUNT(DISTINCT market_date) FROM short_volume")
    days = c.fetchone()[0]
    
    print(f"\nDatabase: {DB_PATH}")
    print(f"  Records:      {total:,}")
    print(f"  Tickers:      {tickers:,}")
    print(f"  Trading days: {days}")
    print(f"  Date range:   {dates[0]} → {dates[1]}")
    
    try:
        c.execute("SELECT COUNT(*), MIN(batch_start), MAX(batch_end) FROM backfill_progress")
        p = c.fetchone()
        if p[0]:
            print(f"\n  Backfill: {p[0]} batches, {p[1]} → {p[2]}")
    except:
        pass
    
    c.execute("""
        SELECT ticker, market_date, short_volume, total_volume, short_volume_ratio
        FROM short_volume ORDER BY market_date DESC LIMIT 5
    """)
    print(f"\n  Latest records:")
    for r in c.fetchall():
        print(f"    {r[0]:8s} {r[1]} short={r[2]:>12,} total={r[3]:>12,} ratio={r[4]:.4f}")
    
    # Top short ratios for latest date
    c.execute("SELECT MAX(market_date) FROM short_volume")
    latest = c.fetchone()[0]
    c.execute("""
        SELECT ticker, short_volume_ratio, short_volume, total_volume
        FROM short_volume WHERE market_date = ? AND total_volume > 100000
        ORDER BY short_volume_ratio DESC LIMIT 10
    """, (latest,))
    results = c.fetchall()
    if results:
        print(f"\n  Highest short ratios on {latest} (vol > 100K):")
        for r in results:
            print(f"    {r[0]:8s} ratio={r[1]:.4f}  short={r[2]:>12,}  total={r[3]:>12,}")
    
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'stats':
        db_stats()
    elif len(sys.argv) > 1 and sys.argv[1] == 'metadata':
        get_access_token()
        check_metadata()
    else:
        run_backfill()
