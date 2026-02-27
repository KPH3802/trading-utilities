#!/usr/bin/env python3
"""
Historical Price Database Collector v2
Fixed paths + alternative S&P 500 source
"""

import yfinance as yf
import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
import time
import os

# Configuration
DB_PATH = "price_history.db"
REQUEST_DELAY = 0.2

DEFAULT_YEARS_BACK = 10


def init_database():
    """Initialize price history database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            UNIQUE(ticker, date)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            sector TEXT,
            industry TEXT,
            market_cap REAL,
            exchange TEXT,
            currency TEXT,
            country TEXT,
            updated_at TEXT
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            collected_at TEXT,
            start_date TEXT,
            end_date TEXT,
            rows_added INTEGER,
            status TEXT
        )
    """)
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker ON daily_prices(ticker)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON daily_prices(ticker, date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_info_sector ON stock_info(sector)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_info_industry ON stock_info(industry)")
    
    conn.commit()
    conn.close()
    print("Price database initialized.")


def get_tickers_from_13f():
    """Get list of tickers from 13F holdings database."""
    db_path = os.path.expanduser("~/13F_detector/13f_holdings.db")
    
    if not os.path.exists(db_path):
        print(f"13F database not found at {db_path}")
        return []
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute("""
        SELECT DISTINCT ticker FROM ticker_cache 
        WHERE ticker IS NOT NULL AND ticker != ''
    """)
    tickers = [row[0] for row in c.fetchall()]
    
    conn.close()
    print(f"Found {len(tickers)} tickers from 13F database")
    return tickers


def get_tickers_from_form4():
    """Get list of tickers from Form 4 database."""
    db_path = os.path.expanduser("~/form4_scanner/form4_filings.db")
    
    if not os.path.exists(db_path):
        print(f"Form 4 database not found at {db_path}")
        return []
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Try common table/column names
    try:
        c.execute("SELECT DISTINCT ticker FROM transactions WHERE ticker IS NOT NULL")
        tickers = [row[0] for row in c.fetchall()]
    except:
        try:
            c.execute("SELECT DISTINCT ticker FROM filings WHERE ticker IS NOT NULL")
            tickers = [row[0] for row in c.fetchall()]
        except:
            tickers = []
    
    conn.close()
    print(f"Found {len(tickers)} tickers from Form 4 database")
    return tickers


def get_tickers_from_options():
    """Get list of tickers from options scanner database."""
    db_path = os.path.expanduser("~/options_scanner/options_scanner.db")
    
    if not os.path.exists(db_path):
        # Try alternate name
        db_path = os.path.expanduser("~/options_volume.db")
        if not os.path.exists(db_path):
            print(f"Options database not found")
            return []
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    try:
        c.execute("SELECT DISTINCT ticker FROM options_data WHERE ticker IS NOT NULL")
        tickers = [row[0] for row in c.fetchall()]
    except:
        try:
            c.execute("SELECT DISTINCT symbol FROM daily_volume WHERE symbol IS NOT NULL")
            tickers = [row[0] for row in c.fetchall()]
        except:
            tickers = []
    
    conn.close()
    print(f"Found {len(tickers)} tickers from options database")
    return tickers


def get_tickers_from_congress():
    """Get list of tickers from congress trading database."""
    db_path = os.path.expanduser("~/congress_tracker/congress_trades.db")
    
    if not os.path.exists(db_path):
        print(f"Congress database not found at {db_path}")
        return []
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    try:
        c.execute("SELECT DISTINCT ticker FROM trades WHERE ticker IS NOT NULL")
        tickers = [row[0] for row in c.fetchall()]
    except:
        tickers = []
    
    conn.close()
    print(f"Found {len(tickers)} tickers from congress database")
    return tickers


def get_all_tracked_tickers():
    """Get combined list of tickers from all our databases."""
    all_tickers = set()
    
    all_tickers.update(get_tickers_from_13f())
    all_tickers.update(get_tickers_from_form4())
    all_tickers.update(get_tickers_from_options())
    all_tickers.update(get_tickers_from_congress())
    
    # Clean up
    cleaned = set()
    for t in all_tickers:
        if t and isinstance(t, str) and len(t) <= 10:
            t = t.upper().strip()
            # Skip bad tickers
            if t and not any(c in t for c in ['/', '\\', ' ']):
                cleaned.add(t)
    
    print(f"\nTotal unique tickers from our databases: {len(cleaned)}")
    return sorted(cleaned)


def get_last_collected_date(ticker):
    """Get the most recent date we have data for a ticker."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT MAX(date) FROM daily_prices WHERE ticker = ?", (ticker,))
    result = c.fetchone()[0]
    conn.close()
    
    return result


def collect_price_history(ticker, years_back=DEFAULT_YEARS_BACK, update_only=True):
    """Collect historical price data for a ticker."""
    try:
        end_date = datetime.now()
        
        if update_only:
            last_date = get_last_collected_date(ticker)
            if last_date:
                start_date = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
                if start_date >= end_date:
                    return 0, "up_to_date"
            else:
                start_date = end_date - timedelta(days=years_back * 365)
        else:
            start_date = end_date - timedelta(days=years_back * 365)
        
        time.sleep(REQUEST_DELAY)
        
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date.strftime("%Y-%m-%d"), 
                          end=end_date.strftime("%Y-%m-%d"))
        
        if df.empty:
            return 0, "no_data"
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        rows_added = 0
        for date, row in df.iterrows():
            date_str = date.strftime("%Y-%m-%d")
            
            try:
                c.execute("""
                    INSERT OR REPLACE INTO daily_prices 
                    (ticker, date, open, high, low, close, adj_close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (ticker, date_str, 
                      row.get('Open'), row.get('High'), row.get('Low'), 
                      row.get('Close'), row.get('Close'),
                      int(row.get('Volume', 0))))
                rows_added += 1
            except:
                continue
        
        c.execute("""
            INSERT INTO collection_log (ticker, collected_at, start_date, end_date, rows_added, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ticker, datetime.now(timezone.utc).isoformat(),
              start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"),
              rows_added, "success"))
        
        conn.commit()
        conn.close()
        
        return rows_added, "success"
        
    except Exception as e:
        return 0, f"error: {str(e)[:50]}"


def collect_stock_info(ticker):
    """Collect metadata about a stock."""
    try:
        time.sleep(REQUEST_DELAY)
        
        stock = yf.Ticker(ticker)
        info = stock.info
        
        if not info:
            return False
        
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute("""
            INSERT OR REPLACE INTO stock_info 
            (ticker, name, sector, industry, market_cap, exchange, currency, country, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ticker,
            info.get('longName') or info.get('shortName'),
            info.get('sector'),
            info.get('industry'),
            info.get('marketCap'),
            info.get('exchange'),
            info.get('currency'),
            info.get('country'),
            datetime.now(timezone.utc).isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        return True
        
    except:
        return False


def batch_collect(tickers, max_tickers=None, collect_info=True):
    """Collect price history for multiple tickers."""
    if max_tickers:
        tickers = tickers[:max_tickers]
    
    print(f"\nCollecting data for {len(tickers)} tickers...")
    print("-" * 50)
    
    success = 0
    updated = 0
    failed = 0
    info_collected = 0
    
    for i, ticker in enumerate(tickers):
        rows, status = collect_price_history(ticker)
        
        if status == "success":
            if rows > 0:
                success += 1
                print(f"[{i+1}/{len(tickers)}] {ticker}: +{rows} rows")
                
                # Collect stock info for new tickers
                if collect_info:
                    if collect_stock_info(ticker):
                        info_collected += 1
            else:
                updated += 1
        elif status == "up_to_date":
            updated += 1
        else:
            failed += 1
            if "no_data" not in status:
                print(f"[{i+1}/{len(tickers)}] {ticker}: {status}")
    
    print("\n" + "-" * 50)
    print(f"Success: {success}, Up-to-date: {updated}, Failed: {failed}")
    print(f"Stock info collected: {info_collected}")
    
    return success, updated, failed


def get_major_indices():
    """Major index ETFs to always track."""
    return [
        # Major indices
        "SPY", "QQQ", "DIA", "IWM", "VTI",
        # Sector ETFs
        "XLF", "XLK", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE",
        # Commodities & Bonds
        "GLD", "SLV", "USO", "TLT", "HYG",
        # International
        "EEM", "EFA",
        # Volatility
        "VIXY", "VXX",
    ]


def get_sp500_tickers():
    """Get S&P 500 tickers - hardcoded top 100 + fetch attempt."""
    
    # Top 100 S&P 500 by weight (as of late 2025) - reliable fallback
    top_100 = [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "TSLA", "BRK-B", "UNH",
        "XOM", "JNJ", "JPM", "V", "PG", "MA", "HD", "CVX", "MRK", "ABBV",
        "LLY", "PEP", "KO", "COST", "AVGO", "WMT", "MCD", "CSCO", "TMO", "ACN",
        "ABT", "DHR", "NEE", "VZ", "CMCSA", "PM", "TXN", "CRM", "NKE", "UPS",
        "AMD", "RTX", "HON", "INTC", "ORCL", "IBM", "QCOM", "LOW", "SPGI", "BA",
        "CAT", "GE", "INTU", "AMAT", "SBUX", "PLD", "AMGN", "DE", "ADP", "BKNG",
        "MDLZ", "ADI", "ISRG", "GILD", "REGN", "VRTX", "SYK", "MMC", "TJX", "CB",
        "C", "BLK", "PGR", "SO", "DUK", "LRCX", "MO", "ZTS", "CI", "BDX",
        "CME", "EOG", "SLB", "CL", "SCHW", "AON", "NOC", "ITW", "FIS", "HUM",
        "EQIX", "MU", "ICE", "BSX", "WM", "FCX", "SHW", "MCK", "PNC", "USB"
    ]
    
    # Try to fetch full list
    try:
        # Alternative: use pandas datareader or direct URL
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        df = pd.read_csv(url)
        tickers = df['Symbol'].tolist()
        tickers = [t.replace('.', '-') for t in tickers]  # Yahoo format
        print(f"Fetched {len(tickers)} S&P 500 tickers from GitHub")
        return tickers
    except:
        pass
    
    print(f"Using hardcoded top {len(top_100)} S&P 500 tickers")
    return top_100


def show_database_stats():
    """Show current database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices")
    ticker_count = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM daily_prices")
    price_count = c.fetchone()[0]
    
    c.execute("SELECT MIN(date), MAX(date) FROM daily_prices")
    date_range = c.fetchone()
    
    c.execute("SELECT COUNT(*) FROM stock_info WHERE sector IS NOT NULL")
    info_count = c.fetchone()[0]
    
    c.execute("""
        SELECT sector, COUNT(*) as cnt 
        FROM stock_info 
        WHERE sector IS NOT NULL 
        GROUP BY sector 
        ORDER BY cnt DESC 
        LIMIT 10
    """)
    sectors = c.fetchall()
    
    conn.close()
    
    print("\n📊 Price Database Statistics:")
    print(f"  Tickers tracked: {ticker_count}")
    print(f"  Price records: {price_count:,}")
    if date_range[0]:
        print(f"  Date range: {date_range[0]} to {date_range[1]}")
    print(f"  Stocks with sector info: {info_count}")
    
    if sectors:
        print("\n  Sectors breakdown:")
        for sector, count in sectors:
            print(f"    {sector}: {count}")


def main():
    """Main function."""
    print("=" * 60)
    print("Historical Price Database Collector v2")
    print("=" * 60)
    
    init_database()
    
    all_tickers = set()
    
    # 1. Major indices and ETFs
    indices = get_major_indices()
    all_tickers.update(indices)
    print(f"\n✓ Added {len(indices)} major indices/ETFs")
    
    # 2. S&P 500
    sp500 = get_sp500_tickers()
    all_tickers.update(sp500)
    print(f"✓ Added {len(sp500)} S&P 500 components")
    
    # 3. From our databases
    tracked = get_all_tracked_tickers()
    all_tickers.update(tracked)
    print(f"✓ Added {len(tracked)} from our trading databases")
    
    all_tickers = sorted(all_tickers)
    print(f"\n📋 Total unique tickers: {len(all_tickers)}")
    
    # Collect
    batch_collect(all_tickers, collect_info=True)
    
    show_database_stats()
    
    print("\n" + "=" * 60)
    print("Collection complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
