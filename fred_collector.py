#!/usr/bin/env python3
"""
FRED Economic Data Collector
Collects macroeconomic indicators from Federal Reserve Economic Data.

Free API, requires key from: https://fred.stlouisfed.org/docs/api/api_key.html

Enables questions like:
- "How do stocks perform when Fed raises rates?"
- "What happens to tech stocks when unemployment spikes?"
- "Correlation between yield curve inversion and market drops?"
"""

import requests
import sqlite3
from datetime import datetime, timezone, timedelta
import time

# Configuration
DB_PATH = "fred_economic.db"
FRED_API_KEY = "47ad8361497547374d365bbf5a719049"  # Get free key at https://fred.stlouisfed.org/docs/api/api_key.html
FRED_BASE_URL = "https://api.stlouisfed.org/fred"

REQUEST_DELAY = 0.2  # FRED is generous but be polite

# Key economic series to track
# Format: (series_id, description, category)
ECONOMIC_SERIES = [
    # Interest Rates
    ("DFF", "Federal Funds Effective Rate", "interest_rates"),
    ("DGS2", "2-Year Treasury Yield", "interest_rates"),
    ("DGS10", "10-Year Treasury Yield", "interest_rates"),
    ("DGS30", "30-Year Treasury Yield", "interest_rates"),
    ("T10Y2Y", "10Y-2Y Treasury Spread (Yield Curve)", "interest_rates"),
    ("T10Y3M", "10Y-3M Treasury Spread", "interest_rates"),
    ("DPRIME", "Bank Prime Loan Rate", "interest_rates"),
    
    # Inflation
    ("CPIAUCSL", "Consumer Price Index (All Urban)", "inflation"),
    ("CPILFESL", "Core CPI (Less Food & Energy)", "inflation"),
    ("PCEPI", "PCE Price Index", "inflation"),
    ("PCEPILFE", "Core PCE Price Index", "inflation"),
    ("T5YIE", "5-Year Breakeven Inflation Rate", "inflation"),
    ("T10YIE", "10-Year Breakeven Inflation Rate", "inflation"),
    
    # Employment
    ("UNRATE", "Unemployment Rate", "employment"),
    ("PAYEMS", "Total Nonfarm Payrolls", "employment"),
    ("ICSA", "Initial Jobless Claims (Weekly)", "employment"),
    ("CCSA", "Continued Jobless Claims", "employment"),
    ("CIVPART", "Labor Force Participation Rate", "employment"),
    ("EMRATIO", "Employment-Population Ratio", "employment"),
    
    # GDP & Growth
    ("GDP", "Gross Domestic Product", "gdp"),
    ("GDPC1", "Real GDP", "gdp"),
    ("A191RL1Q225SBEA", "Real GDP Growth Rate", "gdp"),
    
    # Consumer
    ("UMCSENT", "U of Michigan Consumer Sentiment", "consumer"),
    ("RSAFS", "Retail Sales", "consumer"),
    ("PCE", "Personal Consumption Expenditures", "consumer"),
    ("PSAVERT", "Personal Saving Rate", "consumer"),
    
    # Housing
    ("HOUST", "Housing Starts", "housing"),
    ("PERMIT", "Building Permits", "housing"),
    ("CSUSHPISA", "Case-Shiller Home Price Index", "housing"),
    ("MORTGAGE30US", "30-Year Mortgage Rate", "housing"),
    
    # Manufacturing & Business
    ("INDPRO", "Industrial Production Index", "manufacturing"),
    ("UMTMVS", "Motor Vehicle Sales", "manufacturing"),
    ("NEWORDER", "Manufacturers New Orders", "manufacturing"),
    
    # Money Supply
    ("M2SL", "M2 Money Stock", "money_supply"),
    ("WALCL", "Fed Total Assets (Balance Sheet)", "money_supply"),
    
    # Commodities (from FRED)
    ("DCOILWTICO", "WTI Crude Oil Price", "commodities"),
    ("DCOILBRENTEU", "Brent Crude Oil Price", "commodities"),
    ("GOLDAMGBD228NLBM", "Gold Price (London)", "commodities"),
    ("GASREGW", "Regular Gas Price", "commodities"),
    
    # Market Stress
    ("VIXCLS", "VIX Volatility Index", "market_stress"),
    ("TEDRATE", "TED Spread", "market_stress"),
    ("BAMLH0A0HYM2", "High Yield Bond Spread", "market_stress"),
    
    # Credit
    ("TOTCI", "Commercial & Industrial Loans", "credit"),
    ("DRTSCILM", "Bank Lending Standards (Tightening)", "credit"),
]


def init_database():
    """Initialize FRED database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Series metadata
    c.execute("""
        CREATE TABLE IF NOT EXISTS series_info (
            series_id TEXT PRIMARY KEY,
            title TEXT,
            category TEXT,
            frequency TEXT,
            units TEXT,
            seasonal_adjustment TEXT,
            last_updated TEXT,
            observation_start TEXT,
            observation_end TEXT
        )
    """)
    
    # Economic data observations
    c.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT NOT NULL,
            date TEXT NOT NULL,
            value REAL,
            UNIQUE(series_id, date)
        )
    """)
    
    # Collection log
    c.execute("""
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id TEXT,
            collected_at TEXT,
            rows_added INTEGER,
            status TEXT
        )
    """)
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_obs_series ON observations(series_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_obs_date ON observations(date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_obs_series_date ON observations(series_id, date)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_info_category ON series_info(category)")
    
    conn.commit()
    conn.close()
    print("FRED database initialized.")


def get_series_info(series_id):
    """Get metadata about a FRED series."""
    url = f"{FRED_BASE_URL}/series"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "seriess" in data and data["seriess"]:
            return data["seriess"][0]
        return None
        
    except Exception as e:
        print(f"  Error getting series info: {e}")
        return None


def get_last_observation_date(series_id):
    """Get the most recent date we have for a series."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT MAX(date) FROM observations WHERE series_id = ?", (series_id,))
    result = c.fetchone()[0]
    conn.close()
    
    return result


def collect_series(series_id, description, category, years_back=20):
    """Collect observations for a FRED series."""
    try:
        # Check what we already have
        last_date = get_last_observation_date(series_id)
        
        if last_date:
            start_date = last_date
        else:
            start_date = (datetime.now() - timedelta(days=years_back * 365)).strftime("%Y-%m-%d")
        
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        time.sleep(REQUEST_DELAY)
        
        # Fetch observations
        url = f"{FRED_BASE_URL}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
            "sort_order": "asc"
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        observations = data.get("observations", [])
        
        if not observations:
            return 0, "no_data"
        
        # Store observations
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        rows_added = 0
        for obs in observations:
            date = obs.get("date")
            value = obs.get("value")
            
            # Skip missing values (FRED uses "." for missing)
            if value == "." or value is None:
                continue
            
            try:
                value = float(value)
                c.execute("""
                    INSERT OR REPLACE INTO observations (series_id, date, value)
                    VALUES (?, ?, ?)
                """, (series_id, date, value))
                rows_added += 1
            except:
                continue
        
        # Update series info
        info = get_series_info(series_id)
        if info:
            c.execute("""
                INSERT OR REPLACE INTO series_info 
                (series_id, title, category, frequency, units, seasonal_adjustment, 
                 last_updated, observation_start, observation_end)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                series_id,
                info.get("title", description),
                category,
                info.get("frequency_short"),
                info.get("units_short"),
                info.get("seasonal_adjustment_short"),
                info.get("last_updated"),
                info.get("observation_start"),
                info.get("observation_end")
            ))
        
        # Log collection
        c.execute("""
            INSERT INTO collection_log (series_id, collected_at, rows_added, status)
            VALUES (?, ?, ?, ?)
        """, (series_id, datetime.now(timezone.utc).isoformat(), rows_added, "success"))
        
        conn.commit()
        conn.close()
        
        return rows_added, "success"
        
    except requests.exceptions.HTTPError as e:
        if "400" in str(e) or "404" in str(e):
            return 0, "invalid_series"
        return 0, f"http_error: {e}"
    except Exception as e:
        return 0, f"error: {str(e)[:50]}"


def collect_all_series():
    """Collect all configured economic series."""
    print(f"\nCollecting {len(ECONOMIC_SERIES)} economic series...")
    print("-" * 50)
    
    success = 0
    failed = 0
    total_rows = 0
    
    by_category = {}
    
    for series_id, description, category in ECONOMIC_SERIES:
        print(f"  {series_id}: {description[:40]}...", end=" ")
        
        rows, status = collect_series(series_id, description, category)
        
        if status == "success":
            success += 1
            total_rows += rows
            print(f"+{rows} rows")
            
            if category not in by_category:
                by_category[category] = 0
            by_category[category] += 1
        else:
            failed += 1
            print(f"FAILED ({status})")
    
    print("\n" + "-" * 50)
    print(f"Success: {success}, Failed: {failed}, Total rows: {total_rows:,}")
    
    print("\nBy category:")
    for cat, count in sorted(by_category.items()):
        print(f"  {cat}: {count} series")
    
    return success, failed, total_rows


def show_database_stats():
    """Show database statistics."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT COUNT(DISTINCT series_id) FROM observations")
    series_count = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM observations")
    obs_count = c.fetchone()[0]
    
    c.execute("SELECT MIN(date), MAX(date) FROM observations")
    date_range = c.fetchone()
    
    c.execute("""
        SELECT category, COUNT(DISTINCT series_id) as cnt
        FROM series_info
        GROUP BY category
        ORDER BY cnt DESC
    """)
    categories = c.fetchall()
    
    # Latest values for key indicators
    c.execute("""
        SELECT s.series_id, s.title, o.date, o.value
        FROM observations o
        JOIN series_info s ON o.series_id = s.series_id
        WHERE o.date = (SELECT MAX(date) FROM observations WHERE series_id = o.series_id)
        AND o.series_id IN ('DFF', 'UNRATE', 'CPIAUCSL', 'T10Y2Y', 'VIXCLS')
        ORDER BY s.series_id
    """)
    latest = c.fetchall()
    
    conn.close()
    
    print("\n📊 FRED Database Statistics:")
    print(f"  Series tracked: {series_count}")
    print(f"  Total observations: {obs_count:,}")
    if date_range[0]:
        print(f"  Date range: {date_range[0]} to {date_range[1]}")
    
    if categories:
        print("\n  By category:")
        for cat, count in categories:
            print(f"    {cat}: {count} series")
    
    if latest:
        print("\n  Key indicators (latest):")
        for series_id, title, date, value in latest:
            # Format based on series type
            if series_id in ("UNRATE", "DFF"):
                val_str = f"{value:.2f}%"
            elif series_id == "VIXCLS":
                val_str = f"{value:.1f}"
            elif series_id == "T10Y2Y":
                val_str = f"{value:.2f}%"
            else:
                val_str = f"{value:,.2f}"
            print(f"    {title[:35]}: {val_str} ({date})")


def get_series_data(series_id, start_date=None, end_date=None):
    """
    Helper function to query series data.
    Returns list of (date, value) tuples.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    query = "SELECT date, value FROM observations WHERE series_id = ?"
    params = [series_id]
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date"
    
    c.execute(query, params)
    results = c.fetchall()
    conn.close()
    
    return results


def main():
    """Main function."""
    print("=" * 60)
    print("FRED Economic Data Collector")
    print("=" * 60)
    
    # Check for API key
    if FRED_API_KEY == "YOUR_FRED_API_KEY":
        print("\n⚠️  ERROR: You need a FRED API key!")
        print("   1. Go to: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("   2. Create free account and request API key")
        print("   3. Edit this file and set FRED_API_KEY")
        print("\n   It's free and takes 2 minutes.")
        return
    
    init_database()
    
    collect_all_series()
    
    show_database_stats()
    
    print("\n" + "=" * 60)
    print("Collection complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
