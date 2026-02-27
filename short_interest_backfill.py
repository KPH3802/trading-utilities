#!/usr/bin/env python3
"""
FINRA Consolidated Short Interest Backfill
===========================================
Pulls bi-monthly short interest data from FINRA's free API.
No authentication required. Data goes back to ~2017.

Stores in short_interest.db for insider + short interest backtest.

Usage:
    python3 short_interest_backfill.py           # Run backfill
    python3 short_interest_backfill.py stats      # Show DB stats

API: POST https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest
Fields: symbolCode, settlementDate, currentShortPositionQuantity,
        previousShortPositionQuantity, averageDailyVolumeQuantity,
        daysToCoverQuantity, changePercent, marketClassCode
"""

import sqlite3
import requests
import time
import os
import sys
from datetime import datetime, timezone

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_PATH, 'short_interest.db')

API_URL = 'https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest'

HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

ROWS_PER_REQUEST = 5000  # FINRA sync max
DELAY_BETWEEN_REQUESTS = 0.5  # Be polite


# =============================================================================
# DATABASE
# =============================================================================

def init_database():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS short_interest (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            settlement_date TEXT NOT NULL,
            short_position INTEGER,
            prev_short_position INTEGER,
            avg_daily_volume INTEGER,
            days_to_cover REAL,
            change_percent REAL,
            market_class TEXT,
            collected_at TEXT NOT NULL,
            UNIQUE(ticker, settlement_date)
        )
    ''')

    c.execute('CREATE INDEX IF NOT EXISTS idx_si_ticker ON short_interest(ticker)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_si_date ON short_interest(settlement_date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_si_ticker_date ON short_interest(ticker, settlement_date)')

    conn.commit()
    conn.close()


def save_records(records):
    if not records:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    new_count = 0
    collected_at = datetime.now(timezone.utc).isoformat()

    for r in records:
        try:
            ticker = r.get('symbolCode', '')
            date = r.get('settlementDate', '')
            if not ticker or not date:
                continue

            c.execute('''
                INSERT OR IGNORE INTO short_interest
                (ticker, settlement_date, short_position, prev_short_position,
                 avg_daily_volume, days_to_cover, change_percent, market_class, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker,
                date,
                int(r.get('currentShortPositionQuantity', 0) or 0),
                int(r.get('previousShortPositionQuantity', 0) or 0),
                int(r.get('averageDailyVolumeQuantity', 0) or 0),
                r.get('daysToCoverQuantity'),
                r.get('changePercent'),
                r.get('marketClassCode', ''),
                collected_at
            ))

            if c.rowcount > 0:
                new_count += 1
        except:
            pass

    conn.commit()
    conn.close()
    return new_count


# =============================================================================
# API
# =============================================================================

def get_settlement_dates():
    """Get all available settlement dates from AAPL as a reference ticker."""
    body = {
        "limit": 5000,
        "compareFilters": [
            {"compareType": "equal", "fieldName": "symbolCode", "fieldValue": "AAPL"}
        ]
    }
    r = requests.post(API_URL, headers=HEADERS, json=body, timeout=60)
    if r.status_code == 200:
        data = r.json()
        dates = sorted(set(d['settlementDate'] for d in data))
        return dates
    return []


def fetch_by_date(settlement_date, offset=0):
    """Fetch all tickers for a given settlement date with retries."""
    body = {
        "limit": ROWS_PER_REQUEST,
        "offset": offset,
        "dateRangeFilters": [
            {"fieldName": "settlementDate", "startDate": settlement_date, "endDate": settlement_date}
        ]
    }
    for attempt in range(3):
        try:
            r = requests.post(API_URL, headers=HEADERS, json=body, timeout=120)
            if r.status_code == 200:
                data = r.json()
                has_more = len(data) == ROWS_PER_REQUEST
                return data, has_more
            elif r.status_code == 204:
                return [], False
            elif r.status_code == 429:
                time.sleep(30)
                continue
            else:
                print(f"    HTTP {r.status_code}: {r.text[:200]}")
                return [], False
        except (requests.exceptions.RequestException) as e:
            print(f" [timeout, retry {attempt+1}/3]", end="")
            time.sleep(5 * (attempt + 1))
    return [], False


# =============================================================================
# MAIN
# =============================================================================

def run_backfill():
    print("=" * 60)
    print("FINRA CONSOLIDATED SHORT INTEREST BACKFILL")
    print(f"Database: {DB_PATH}")
    print("=" * 60)

    init_database()

    # Get existing dates to skip
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT settlement_date FROM short_interest")
    existing_dates = set(r[0] for r in c.fetchall())
    conn.close()

    # Get all available settlement dates
    print("\nFetching available settlement dates...")
    all_dates = get_settlement_dates()
    print(f"  Found {len(all_dates)} settlement dates ({all_dates[0]} → {all_dates[-1]})")

    # Filter to dates we don't have
    new_dates = [d for d in all_dates if d not in existing_dates]
    print(f"  Already have {len(existing_dates)} dates, need {len(new_dates)} more")

    if not new_dates:
        print("\nAll dates already backfilled!")
        return

    print(f"\nPulling data for {len(new_dates)} settlement dates...\n")

    total_records = 0
    total_inserted = 0
    start_time = time.time()

    for i, date in enumerate(new_dates, 1):
        elapsed = time.time() - start_time
        rate = total_records / max(elapsed, 1)

        print(f"  [{i}/{len(new_dates)}] {date}  "
              f"[{total_records:,} fetched, {total_inserted:,} new, {rate:.0f} rec/s]", end="")

        date_fetched = 0
        date_inserted = 0
        offset = 0

        while True:
            records, has_more = fetch_by_date(date, offset)

            if not records:
                break

            inserted = save_records(records)
            date_fetched += len(records)
            date_inserted += inserted

            if has_more:
                offset += ROWS_PER_REQUEST
                time.sleep(DELAY_BETWEEN_REQUESTS * 0.3)
            else:
                break

        total_records += date_fetched
        total_inserted += date_inserted
        print(f"  → {date_fetched:,} records, {date_inserted:,} new")

        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Summary
    duration = time.time() - start_time
    print()
    print("=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"Records fetched:  {total_records:,}")
    print(f"New records:      {total_inserted:,}")
    print(f"Duration:         {duration/60:.1f} minutes")
    print()


def db_stats():
    if not os.path.exists(DB_PATH):
        print("No database found.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM short_interest")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT ticker) FROM short_interest")
    tickers = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT settlement_date) FROM short_interest")
    dates = c.fetchone()[0]
    c.execute("SELECT MIN(settlement_date), MAX(settlement_date) FROM short_interest")
    dr = c.fetchone()

    print(f"\nDatabase: {DB_PATH}")
    print(f"  Records:          {total:,}")
    print(f"  Unique tickers:   {tickers:,}")
    print(f"  Settlement dates: {dates}")
    print(f"  Date range:       {dr[0]} → {dr[1]}")

    # Top short interest for latest date
    c.execute("SELECT MAX(settlement_date) FROM short_interest")
    latest = c.fetchone()[0]
    c.execute("""
        SELECT ticker, short_position, avg_daily_volume, days_to_cover, change_percent
        FROM short_interest
        WHERE settlement_date = ? AND avg_daily_volume > 100000
        ORDER BY days_to_cover DESC LIMIT 10
    """, (latest,))
    results = c.fetchall()
    if results:
        print(f"\n  Highest days-to-cover on {latest} (vol > 100K):")
        for r in results:
            print(f"    {r[0]:8s}  short={r[1]:>12,}  adv={r[2]:>12,}  dtc={r[3]:>6.1f}  chg={r[4]:>6.1f}%")

    conn.close()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'stats':
        db_stats()
    else:
        run_backfill()
