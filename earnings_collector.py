"""
Earnings Calendar Collector - Yahoo Finance Fetching
Uses same ticker list as Options Scanner for consistency.
"""

import yfinance as yf
from datetime import datetime, timedelta, timezone
from config import DAYS_AHEAD
import sys
sys.path.insert(0, '/home/KPH3802/options_scanner')
from tickers import get_ticker_list


def get_ticker_earnings(ticker_symbol):
    """
    Get earnings date for a single ticker.
    Returns dict with earnings info or None if not available.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # Get calendar info (includes earnings date)
        calendar = ticker.calendar
        
        if calendar is None or (hasattr(calendar, 'empty') and calendar.empty):
            return None
        
        # calendar can be a DataFrame or dict depending on yfinance version
        earnings_date = None
        
        if hasattr(calendar, 'to_dict'):
            cal_dict = calendar.to_dict()
            if 'Earnings Date' in cal_dict:
                earnings_dates = cal_dict['Earnings Date']
                if isinstance(earnings_dates, dict):
                    earnings_date = list(earnings_dates.values())[0]
                else:
                    earnings_date = earnings_dates
        elif isinstance(calendar, dict):
            if 'Earnings Date' in calendar:
                earnings_date = calendar['Earnings Date']
                if isinstance(earnings_date, list):
                    earnings_date = earnings_date[0] if earnings_date else None
        
        if earnings_date is None:
            return None
        
        # Convert to date string
        if hasattr(earnings_date, 'strftime'):
            earnings_date_str = earnings_date.strftime('%Y-%m-%d')
        else:
            earnings_date_str = str(earnings_date)[:10]
        
        # Get company name
        info = ticker.info
        company_name = info.get('longName') or info.get('shortName') or ticker_symbol
        
        return {
            'ticker': ticker_symbol,
            'company_name': company_name,
            'earnings_date': earnings_date_str,
            'time_of_day': None,  # yfinance doesn't reliably provide BMO/AMC
            'eps_estimate': info.get('epsCurrentYear'),
            'eps_actual': None,
            'revenue_estimate': info.get('revenueEstimate'),
            'revenue_actual': None
        }
        
    except Exception as e:
        print(f"  Warning: Could not fetch {ticker_symbol}: {e}")
        return None


def fetch_earnings_calendar(tickers=None):
    """
    Fetch earnings calendar from Yahoo Finance for given tickers.
    Returns list of earnings dictionaries within DAYS_AHEAD window.
    """
    if tickers is None:
        # Use same Russell 1000 list as Options Scanner
        tickers = get_ticker_list("russell1000")
        print(f"Using Russell 1000 ticker list: {len(tickers)} tickers")
    
    today = datetime.now(timezone.utc).date()
    end_date = today + timedelta(days=DAYS_AHEAD)
    
    print(f"Fetching earnings for {len(tickers)} tickers...")
    print(f"Looking for earnings between {today} and {end_date}")
    
    earnings_list = []
    checked = 0
    
    for ticker_symbol in tickers:
        checked += 1
        if checked % 50 == 0:
            print(f"  Progress: {checked}/{len(tickers)} tickers checked...")
        
        result = get_ticker_earnings(ticker_symbol)
        
        if result and result.get('earnings_date'):
            # Filter to our date range
            try:
                earn_date = datetime.strptime(result['earnings_date'], '%Y-%m-%d').date()
                if today <= earn_date <= end_date:
                    earnings_list.append(result)
                    print(f"  Found: {ticker_symbol} reports on {result['earnings_date']}")
            except ValueError:
                pass
    
    print(f"Completed. Found {len(earnings_list)} earnings in next {DAYS_AHEAD} days.")
    return earnings_list


if __name__ == "__main__":
    # Test the collector
    results = fetch_earnings_calendar()
    print("\n--- Results ---")
    for r in results:
        print(f"{r['ticker']}: {r['earnings_date']} - {r['company_name']}")
