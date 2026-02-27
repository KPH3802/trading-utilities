#!/usr/bin/env python3
"""
Trading Data Query Interface
==============================
Query all your trading databases from one place.

USAGE:
  python3 trading_query.py              # Interactive menu
  python3 trading_query.py --sql        # Jump straight to custom SQL mode

DATABASES:
  - Form 4 Insider Backtest (5,774 results + 7,402 clusters)
  - Form 4 Live Scanner (452 transactions)
  - Options Volume Scanner (9,033 daily + 75 anomalies)
  - Congress Tracker (transactions + politicians)
"""

import sqlite3
import os
import sys
from datetime import datetime, timedelta

# =============================================================================
# DATABASE PATHS
# =============================================================================
BASE = os.path.expanduser("~/Desktop/Claude_Programs/Trading_Programs")

DATABASES = {
    "backtest": os.path.join(BASE, "Form4_Scanner/insider_backtest_results.db"),
    "form4": os.path.join(BASE, "Form4_Scanner/form4_insider_trades.db"),
    "options": os.path.join(BASE, "Options_Scanner/options_data.db"),
    "congress": os.path.join(BASE, "Congress_Tracker/congress_trades.db"),
}


def get_conn(db_key):
    """Get a connection to the specified database."""
    path = DATABASES[db_key]
    if not os.path.exists(path):
        print(f"  ❌ Database not found: {path}")
        return None
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def print_table(rows, max_width=120):
    """Print rows as a formatted table."""
    if not rows:
        print("  (no results)")
        return
    
    # Convert to list of dicts
    if hasattr(rows[0], 'keys'):
        headers = rows[0].keys()
        data = [dict(r) for r in rows]
    elif isinstance(rows[0], dict):
        headers = rows[0].keys()
        data = rows
    else:
        print("  (unknown format)")
        return
    
    headers = list(headers)
    
    # Calculate column widths
    col_widths = {}
    for h in headers:
        values = [str(d.get(h, ''))[:40] for d in data]
        col_widths[h] = max(len(h), max(len(v) for v in values) if values else 0)
    
    # Truncate columns if total width too large
    total_width = sum(col_widths.values()) + 3 * (len(headers) - 1)
    if total_width > max_width and len(headers) > 3:
        # Show key columns at reasonable width, truncate others
        for h in headers:
            col_widths[h] = min(col_widths[h], 18)
    
    # Print header
    header_line = " | ".join(h[:col_widths[h]].ljust(col_widths[h]) for h in headers)
    print(f"  {header_line}")
    print(f"  {'-' * len(header_line)}")
    
    # Print rows
    for d in data:
        row_line = " | ".join(
            str(d.get(h, ''))[:col_widths[h]].ljust(col_widths[h]) 
            for h in headers
        )
        print(f"  {row_line}")


def print_result_count(rows):
    """Print how many results."""
    print(f"\n  📊 {len(rows)} result(s)\n")


# =============================================================================
# QUERY CATEGORIES
# =============================================================================

# --- INSIDER BACKTEST QUERIES ---

def q_top_alpha_5d(n=20):
    """Top insider clusters by 5-day alpha."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, company, signal_date, num_insiders, num_transactions,
               CAST(total_dollars AS INTEGER) as total_dollars, roles,
               ROUND(ret_5d, 2) as ret_5d, ROUND(spy_5d, 2) as spy_5d, 
               ROUND(alpha_5d, 2) as alpha_5d
        FROM backtest_results
        WHERE alpha_5d IS NOT NULL
        ORDER BY alpha_5d DESC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  🏆 Top {n} Insider Clusters by 5-Day Alpha\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_top_alpha_20d(n=20):
    """Top insider clusters by 20-day alpha."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, company, signal_date, num_insiders, num_transactions,
               CAST(total_dollars AS INTEGER) as total_dollars, roles,
               ROUND(ret_20d, 2) as ret_20d, ROUND(spy_20d, 2) as spy_20d,
               ROUND(alpha_20d, 2) as alpha_20d
        FROM backtest_results
        WHERE alpha_20d IS NOT NULL
        ORDER BY alpha_20d DESC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  🏆 Top {n} Insider Clusters by 20-Day Alpha\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_worst_alpha_20d(n=20):
    """Worst insider clusters by 20-day alpha (biggest losers)."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, company, signal_date, num_insiders, num_transactions,
               CAST(total_dollars AS INTEGER) as total_dollars, roles,
               ROUND(ret_20d, 2) as ret_20d, ROUND(spy_20d, 2) as spy_20d,
               ROUND(alpha_20d, 2) as alpha_20d
        FROM backtest_results
        WHERE alpha_20d IS NOT NULL
        ORDER BY alpha_20d ASC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  📉 Bottom {n} Insider Clusters by 20-Day Alpha\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_ceo_clusters():
    """Performance of clusters where CEO was buying."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT 
            'CEO Buying' as category,
            COUNT(*) as signals,
            ROUND(AVG(ret_5d), 2) as avg_ret_5d,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(ret_20d), 2) as avg_ret_20d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(ret_40d), 2) as avg_ret_40d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) as winners_20d,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d
        FROM backtest_results WHERE has_ceo = 1
        UNION ALL
        SELECT 
            'No CEO' as category,
            COUNT(*) as signals,
            ROUND(AVG(ret_5d), 2) as avg_ret_5d,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(ret_20d), 2) as avg_ret_20d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(ret_40d), 2) as avg_ret_40d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) as winners_20d,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d
        FROM backtest_results WHERE has_ceo = 0
        UNION ALL
        SELECT 
            'ALL' as category,
            COUNT(*) as signals,
            ROUND(AVG(ret_5d), 2) as avg_ret_5d,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(ret_20d), 2) as avg_ret_20d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(ret_40d), 2) as avg_ret_40d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) as winners_20d,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d
        FROM backtest_results
    """).fetchall()
    print(f"\n  👔 CEO Buying vs No CEO — Performance Comparison\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_yearly_performance():
    """Insider cluster performance by year."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT 
            year,
            COUNT(*) as signals,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            ROUND(100.0 * SUM(CASE WHEN alpha_5d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_5d_pct,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_20d_pct,
            CAST(ROUND(AVG(total_dollars)) AS INTEGER) as avg_dollars
        FROM backtest_results
        GROUP BY year
        ORDER BY year
    """).fetchall()
    print(f"\n  📅 Insider Cluster Alpha by Year\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_big_money_clusters(min_dollars=500000, n=20):
    """High-conviction: large dollar clusters and their performance."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, company, signal_date, num_insiders,
               '$' || PRINTF('%,d', CAST(total_dollars AS INTEGER)) as total_invested,
               roles,
               ROUND(alpha_5d, 2) as alpha_5d,
               ROUND(alpha_20d, 2) as alpha_20d,
               ROUND(alpha_40d, 2) as alpha_40d
        FROM backtest_results
        WHERE total_dollars >= ?
        ORDER BY total_dollars DESC
        LIMIT ?
    """, (min_dollars, n)).fetchall()
    print(f"\n  💰 Largest Dollar Insider Clusters (>${min_dollars:,}+)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_big_money_stats():
    """Performance comparison: large vs small clusters."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT 
            '$1M+' as size_bucket,
            COUNT(*) as n,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d
        FROM backtest_results WHERE total_dollars >= 1000000
        UNION ALL
        SELECT 
            '$500K-$1M' as size_bucket,
            COUNT(*) as n,
            ROUND(AVG(alpha_5d), 2),
            ROUND(AVG(alpha_20d), 2),
            ROUND(AVG(alpha_40d), 2),
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM backtest_results WHERE total_dollars >= 500000 AND total_dollars < 1000000
        UNION ALL
        SELECT 
            '$100K-$500K' as size_bucket,
            COUNT(*) as n,
            ROUND(AVG(alpha_5d), 2),
            ROUND(AVG(alpha_20d), 2),
            ROUND(AVG(alpha_40d), 2),
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM backtest_results WHERE total_dollars >= 100000 AND total_dollars < 500000
        UNION ALL
        SELECT 
            'Under $100K' as size_bucket,
            COUNT(*) as n,
            ROUND(AVG(alpha_5d), 2),
            ROUND(AVG(alpha_20d), 2),
            ROUND(AVG(alpha_40d), 2),
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM backtest_results WHERE total_dollars < 100000
    """).fetchall()
    print(f"\n  💰 Performance by Cluster Dollar Size\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_multi_insider_performance():
    """Performance by number of insiders in cluster."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT 
            num_insiders || ' insiders' as cluster_size,
            COUNT(*) as n,
            ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
            ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
            ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
            ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d,
            CAST(ROUND(AVG(total_dollars)) AS INTEGER) as avg_dollars
        FROM backtest_results
        GROUP BY num_insiders
        ORDER BY num_insiders
    """).fetchall()
    print(f"\n  👥 Performance by Number of Insiders in Cluster\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_repeat_tickers():
    """Tickers with multiple insider clusters and their average alpha."""
    conn = get_conn("backtest")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, company,
               COUNT(*) as num_clusters,
               ROUND(AVG(alpha_5d), 2) as avg_alpha_5d,
               ROUND(AVG(alpha_20d), 2) as avg_alpha_20d,
               ROUND(AVG(alpha_40d), 2) as avg_alpha_40d,
               ROUND(100.0 * SUM(CASE WHEN alpha_20d > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct_20d
        FROM backtest_results
        GROUP BY ticker
        HAVING COUNT(*) >= 3
        ORDER BY num_clusters DESC
        LIMIT 25
    """).fetchall()
    print(f"\n  🔄 Tickers with 3+ Insider Clusters (Repeat Signals)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


# --- FORM 4 LIVE QUERIES ---

def q_recent_insider_buys(days=30):
    """Recent insider purchases from live scanner."""
    conn = get_conn("form4")
    if not conn: return
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT issuer_ticker as ticker, issuer_name as company, 
               insider_name, insider_title,
               transaction_date, 
               CAST(shares_amount AS INTEGER) as shares,
               ROUND(price_per_share, 2) as price,
               CAST(total_value AS INTEGER) as total_value
        FROM form4_transactions
        WHERE transaction_code = 'P' AND transaction_date >= ?
        ORDER BY transaction_date DESC
        LIMIT 30
    """, (cutoff,)).fetchall()
    print(f"\n  🛒 Recent Insider Purchases (last {days} days)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_recent_insider_all(days=14):
    """All recent insider transactions from live scanner."""
    conn = get_conn("form4")
    if not conn: return
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT issuer_ticker as ticker, issuer_name as company,
               insider_name, insider_title, transaction_code as code,
               transaction_date,
               CAST(shares_amount AS INTEGER) as shares,
               ROUND(price_per_share, 2) as price,
               CAST(total_value AS INTEGER) as total_value,
               acquired_disposed as acq_disp
        FROM form4_transactions
        WHERE transaction_date >= ?
        ORDER BY transaction_date DESC
    """, (cutoff,)).fetchall()
    print(f"\n  📋 All Insider Transactions (last {days} days)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


# --- OPTIONS QUERIES ---

def q_options_anomalies(n=20):
    """Recent options volume anomalies."""
    conn = get_conn("options")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, detected_date, 
               CAST(volume_today AS INTEGER) as volume,
               CAST(avg_volume_1month AS INTEGER) as avg_1mo,
               ROUND(deviation_multiple, 1) as std_devs,
               ROUND(percentage_above_avg, 0) as pct_above,
               near_earnings, signal_type, notes
        FROM anomalies
        ORDER BY detected_date DESC, deviation_multiple DESC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  🔥 Recent Options Volume Anomalies\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_biggest_volume_spikes(n=20):
    """Biggest options volume spikes ever detected."""
    conn = get_conn("options")
    if not conn: return
    rows = conn.execute("""
        SELECT ticker, detected_date,
               CAST(volume_today AS INTEGER) as volume,
               CAST(avg_volume_1month AS INTEGER) as avg_1mo,
               ROUND(deviation_multiple, 1) as std_devs,
               ROUND(percentage_above_avg, 0) as pct_above,
               near_earnings, signal_type
        FROM anomalies
        ORDER BY deviation_multiple DESC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  🚀 Biggest Options Volume Spikes (by std devs)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_options_daily_leaders(days=5):
    """Top volume tickers from recent days."""
    conn = get_conn("options")
    if not conn: return
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT ticker, trade_date, 
               CAST(total_volume AS INTEGER) as total_vol,
               CAST(total_call_volume AS INTEGER) as calls,
               CAST(total_put_volume AS INTEGER) as puts,
               ROUND(1.0 * total_put_volume / NULLIF(total_call_volume, 0), 2) as pc_ratio,
               CAST(total_oi AS INTEGER) as open_interest
        FROM daily_options_volume
        WHERE trade_date >= ?
        ORDER BY total_volume DESC
        LIMIT 25
    """, (cutoff,)).fetchall()
    print(f"\n  📊 Top Volume Tickers (last {days} days)\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_high_put_call_ratio(days=7, min_volume=5000):
    """Tickers with elevated put/call ratios (bearish signal)."""
    conn = get_conn("options")
    if not conn: return
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT ticker, trade_date,
               CAST(total_call_volume AS INTEGER) as calls,
               CAST(total_put_volume AS INTEGER) as puts,
               CAST(total_volume AS INTEGER) as total,
               ROUND(1.0 * total_put_volume / NULLIF(total_call_volume, 0), 2) as pc_ratio
        FROM daily_options_volume
        WHERE trade_date >= ? AND total_volume >= ? AND total_call_volume > 0
        ORDER BY (1.0 * total_put_volume / total_call_volume) DESC
        LIMIT 25
    """, (cutoff, min_volume)).fetchall()
    print(f"\n  🐻 Highest Put/Call Ratios (last {days} days, min vol {min_volume:,})\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


# --- CONGRESS QUERIES ---

def q_congress_recent(n=25):
    """Recent congressional trades."""
    conn = get_conn("congress")
    if not conn: return
    rows = conn.execute("""
        SELECT politician, party, chamber, ticker, company,
               trade_type, trade_date, disclosure_date, amount_range,
               is_leadership
        FROM transactions
        ORDER BY trade_date DESC
        LIMIT ?
    """, (n,)).fetchall()
    print(f"\n  🏛️  Recent Congressional Trades\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


def q_congress_by_politician():
    """Trade count by politician."""
    conn = get_conn("congress")
    if not conn: return
    rows = conn.execute("""
        SELECT politician, party, chamber,
               COUNT(*) as trades,
               SUM(CASE WHEN trade_type = 'Purchase' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN trade_type = 'Sale' THEN 1 ELSE 0 END) as sells,
               COUNT(DISTINCT ticker) as unique_tickers
        FROM transactions
        GROUP BY politician
        ORDER BY trades DESC
    """).fetchall()
    print(f"\n  🏛️  Congressional Trading by Politician\n")
    print_table(rows)
    print_result_count(rows)
    conn.close()


# --- CROSS-SCANNER QUERIES ---

def q_cross_insider_options():
    """Tickers appearing in BOTH insider clusters and options anomalies."""
    conn_bt = get_conn("backtest")
    conn_opt = get_conn("options")
    if not conn_bt or not conn_opt: return
    
    # Get insider tickers from last 2 years
    insider_rows = conn_bt.execute("""
        SELECT DISTINCT ticker FROM backtest_results 
        WHERE signal_date >= '2024-01-01'
    """).fetchall()
    insider_tickers = set(r['ticker'] for r in insider_rows)
    
    # Get options anomaly tickers
    anomaly_rows = conn_opt.execute("""
        SELECT DISTINCT ticker FROM anomalies
    """).fetchall()
    anomaly_tickers = set(r['ticker'] for r in anomaly_rows)
    
    overlap = insider_tickers & anomaly_tickers
    
    if not overlap:
        print("\n  No overlapping tickers found between insider clusters and options anomalies.")
        conn_bt.close()
        conn_opt.close()
        return
    
    print(f"\n  🔗 Tickers in BOTH Insider Clusters AND Options Anomalies ({len(overlap)} found)\n")
    
    for ticker in sorted(overlap):
        # Get insider info
        bt = conn_bt.execute("""
            SELECT signal_date, num_insiders, ROUND(alpha_5d, 2) as alpha_5d, 
                   ROUND(alpha_20d, 2) as alpha_20d
            FROM backtest_results WHERE ticker = ? AND signal_date >= '2024-01-01'
            ORDER BY signal_date DESC LIMIT 1
        """, (ticker,)).fetchone()
        
        # Get anomaly info
        an = conn_opt.execute("""
            SELECT detected_date, ROUND(deviation_multiple, 1) as std_devs,
                   ROUND(percentage_above_avg, 0) as pct_above
            FROM anomalies WHERE ticker = ?
            ORDER BY detected_date DESC LIMIT 1
        """, (ticker,)).fetchone()
        
        print(f"  {ticker}:")
        if bt:
            print(f"    Insider: {bt['signal_date'][:10]} | {bt['num_insiders']} insiders | alpha_5d: {bt['alpha_5d']}% | alpha_20d: {bt['alpha_20d']}%")
        if an:
            print(f"    Options: {an['detected_date']} | {an['std_devs']} std devs | {an['pct_above']}% above avg")
        print()
    
    conn_bt.close()
    conn_opt.close()


def q_cross_insider_congress():
    """Tickers appearing in BOTH insider buying and congressional trades."""
    conn_bt = get_conn("backtest")
    conn_cg = get_conn("congress")
    if not conn_bt or not conn_cg: return
    
    insider_rows = conn_bt.execute("SELECT DISTINCT ticker FROM backtest_results").fetchall()
    insider_tickers = set(r['ticker'] for r in insider_rows)
    
    congress_rows = conn_cg.execute("SELECT DISTINCT ticker FROM transactions").fetchall()
    congress_tickers = set(r['ticker'] for r in congress_rows)
    
    overlap = insider_tickers & congress_tickers
    
    if not overlap:
        print("\n  No overlapping tickers found between insider clusters and congressional trades.")
    else:
        print(f"\n  🔗 Tickers in BOTH Insider Clusters AND Congressional Trades ({len(overlap)} found)\n")
        for ticker in sorted(overlap):
            bt = conn_bt.execute("""
                SELECT COUNT(*) as n, ROUND(AVG(alpha_20d), 2) as avg_alpha
                FROM backtest_results WHERE ticker = ?
            """, (ticker,)).fetchone()
            cg = conn_cg.execute("""
                SELECT politician, trade_type, trade_date 
                FROM transactions WHERE ticker = ?
                ORDER BY trade_date DESC LIMIT 1
            """, (ticker,)).fetchone()
            
            print(f"  {ticker}: {bt['n']} insider clusters (avg alpha_20d: {bt['avg_alpha']}%) | Congress: {cg['politician']} {cg['trade_type']} on {cg['trade_date']}")
    
    conn_bt.close()
    conn_cg.close()


# --- DASHBOARD / OVERVIEW ---

def q_dashboard():
    """Quick overview of all databases."""
    print(f"\n  {'='*60}")
    print(f"  📊 TRADING DATA DASHBOARD")
    print(f"  {'='*60}")
    
    # Backtest stats
    conn = get_conn("backtest")
    if conn:
        r = conn.execute("""
            SELECT COUNT(*) as total, 
                   ROUND(AVG(alpha_5d), 2) as avg_5d,
                   ROUND(AVG(alpha_20d), 2) as avg_20d,
                   MIN(signal_date) as earliest,
                   MAX(signal_date) as latest
            FROM backtest_results
        """).fetchone()
        print(f"\n  🔍 INSIDER BACKTEST")
        print(f"     {r['total']:,} clusters tested | Avg alpha: 5d={r['avg_5d']}%, 20d={r['avg_20d']}%")
        print(f"     Range: {r['earliest'][:10]} to {r['latest'][:10]}")
        conn.close()
    
    # Live Form 4
    conn = get_conn("form4")
    if conn:
        r = conn.execute("""
            SELECT COUNT(*) as total, 
                   MIN(transaction_date) as earliest,
                   MAX(transaction_date) as latest,
                   SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) as buys
            FROM form4_transactions
        """).fetchone()
        print(f"\n  📝 FORM 4 LIVE SCANNER")
        print(f"     {r['total']:,} transactions ({r['buys']} purchases)")
        print(f"     Range: {r['earliest']} to {r['latest']}")
        conn.close()
    
    # Options
    conn = get_conn("options")
    if conn:
        r1 = conn.execute("""
            SELECT COUNT(*) as total, COUNT(DISTINCT ticker) as tickers,
                   MIN(trade_date) as earliest, MAX(trade_date) as latest
            FROM daily_options_volume
        """).fetchone()
        r2 = conn.execute("SELECT COUNT(*) as total FROM anomalies").fetchone()
        print(f"\n  📈 OPTIONS SCANNER")
        print(f"     {r1['total']:,} daily records across {r1['tickers']} tickers")
        print(f"     {r2['total']} anomalies detected")
        print(f"     Range: {r1['earliest']} to {r1['latest']}")
        conn.close()
    
    # Congress
    conn = get_conn("congress")
    if conn:
        r = conn.execute("""
            SELECT COUNT(*) as total, COUNT(DISTINCT politician) as politicians,
                   COUNT(DISTINCT ticker) as tickers
            FROM transactions
        """).fetchone()
        print(f"\n  🏛️  CONGRESS TRACKER")
        print(f"     {r['total']} transactions | {r['politicians']} politicians | {r['tickers']} tickers")
        conn.close()
    
    print(f"\n  {'='*60}\n")


# --- CUSTOM SQL ---

def q_custom_sql():
    """Run custom SQL against any database."""
    print("\n  Available databases:")
    print("    1. backtest  - Form4 insider backtest results + clusters")
    print("    2. form4     - Live Form4 scanner transactions")
    print("    3. options   - Options volume + anomalies")
    print("    4. congress  - Congressional trades + politicians")
    
    db_choice = input("\n  Which database? (1-4 or name): ").strip()
    db_map = {"1": "backtest", "2": "form4", "3": "options", "4": "congress"}
    db_key = db_map.get(db_choice, db_choice)
    
    if db_key not in DATABASES:
        print(f"  ❌ Unknown database: {db_key}")
        return
    
    conn = get_conn(db_key)
    if not conn: return
    
    # Show tables
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"\n  Tables in {db_key}: {', '.join(t['name'] for t in tables)}")
    
    print("\n  Enter SQL (or 'back' to return):")
    while True:
        sql = input("\n  SQL> ").strip()
        if sql.lower() in ('back', 'quit', 'exit', ''):
            break
        try:
            rows = conn.execute(sql).fetchall()
            if rows:
                print_table(rows)
                print_result_count(rows)
            else:
                print("  (no results)")
        except Exception as e:
            print(f"  ❌ SQL Error: {e}")
    
    conn.close()


# --- LOOKUP ---

def q_ticker_lookup():
    """Look up everything we know about a specific ticker."""
    ticker = input("\n  Enter ticker: ").strip().upper()
    if not ticker:
        return
    
    print(f"\n  {'='*60}")
    print(f"  🔎 Everything about: {ticker}")
    print(f"  {'='*60}")
    
    # Backtest
    conn = get_conn("backtest")
    if conn:
        rows = conn.execute("""
            SELECT signal_date, num_insiders, num_transactions,
                   CAST(total_dollars AS INTEGER) as dollars, roles,
                   ROUND(alpha_5d, 2) as alpha_5d, ROUND(alpha_20d, 2) as alpha_20d,
                   ROUND(alpha_40d, 2) as alpha_40d
            FROM backtest_results WHERE ticker = ?
            ORDER BY signal_date DESC
        """, (ticker,)).fetchall()
        if rows:
            print(f"\n  📊 Insider Clusters ({len(rows)} found):")
            print_table(rows)
        else:
            print(f"\n  📊 Insider Clusters: None found")
        conn.close()
    
    # Live Form 4
    conn = get_conn("form4")
    if conn:
        rows = conn.execute("""
            SELECT insider_name, insider_title, transaction_code, transaction_date,
                   CAST(shares_amount AS INTEGER) as shares, 
                   ROUND(price_per_share, 2) as price,
                   CAST(total_value AS INTEGER) as value
            FROM form4_transactions WHERE issuer_ticker = ?
            ORDER BY transaction_date DESC
        """, (ticker,)).fetchall()
        if rows:
            print(f"\n  📝 Live Form 4 Filings ({len(rows)} found):")
            print_table(rows)
        else:
            print(f"\n  📝 Live Form 4 Filings: None found")
        conn.close()
    
    # Options
    conn = get_conn("options")
    if conn:
        rows = conn.execute("""
            SELECT detected_date, CAST(volume_today AS INTEGER) as volume,
                   ROUND(deviation_multiple, 1) as std_devs,
                   ROUND(percentage_above_avg, 0) as pct_above,
                   signal_type
            FROM anomalies WHERE ticker = ?
            ORDER BY detected_date DESC
        """, (ticker,)).fetchall()
        if rows:
            print(f"\n  🔥 Options Anomalies ({len(rows)} found):")
            print_table(rows)
        else:
            print(f"\n  🔥 Options Anomalies: None found")
        conn.close()
    
    # Congress
    conn = get_conn("congress")
    if conn:
        rows = conn.execute("""
            SELECT politician, party, trade_type, trade_date, amount_range
            FROM transactions WHERE ticker = ?
            ORDER BY trade_date DESC
        """, (ticker,)).fetchall()
        if rows:
            print(f"\n  🏛️  Congressional Trades ({len(rows)} found):")
            print_table(rows)
        else:
            print(f"\n  🏛️  Congressional Trades: None found")
        conn.close()
    
    print()


# =============================================================================
# MENU SYSTEM
# =============================================================================

MENU = {
    "DASHBOARD": [
        ("0", "📊 Dashboard Overview", q_dashboard),
        ("L", "🔎 Ticker Lookup (search all databases)", q_ticker_lookup),
    ],
    "INSIDER BACKTEST": [
        ("1", "🏆 Top 20 clusters by 5-day alpha", q_top_alpha_5d),
        ("2", "🏆 Top 20 clusters by 20-day alpha", q_top_alpha_20d),
        ("3", "📉 Worst 20 clusters by 20-day alpha", q_worst_alpha_20d),
        ("4", "👔 CEO buying vs no CEO (comparison)", q_ceo_clusters),
        ("5", "📅 Performance by year", q_yearly_performance),
        ("6", "💰 Biggest dollar clusters + performance", q_big_money_clusters),
        ("7", "💰 Performance by cluster dollar size", q_big_money_stats),
        ("8", "👥 Performance by number of insiders", q_multi_insider_performance),
        ("9", "🔄 Repeat signal tickers (3+ clusters)", q_repeat_tickers),
    ],
    "LIVE SCANNERS": [
        ("10", "🛒 Recent insider purchases (Form 4)", q_recent_insider_buys),
        ("11", "📋 All recent insider transactions", q_recent_insider_all),
        ("12", "🔥 Recent options anomalies", q_options_anomalies),
        ("13", "🚀 Biggest options volume spikes ever", q_biggest_volume_spikes),
        ("14", "📊 Top volume tickers (last 5 days)", q_options_daily_leaders),
        ("15", "🐻 Highest put/call ratios", q_high_put_call_ratio),
        ("16", "🏛️  Recent congressional trades", q_congress_recent),
        ("17", "🏛️  Trades by politician", q_congress_by_politician),
    ],
    "CROSS-SCANNER": [
        ("18", "🔗 Insider + Options overlap", q_cross_insider_options),
        ("19", "🔗 Insider + Congress overlap", q_cross_insider_congress),
    ],
    "ADVANCED": [
        ("SQL", "⚡ Custom SQL query", q_custom_sql),
    ],
}


def show_menu():
    print(f"\n  {'='*50}")
    print(f"  TRADING DATA QUERY INTERFACE")
    print(f"  {'='*50}")
    for category, items in MENU.items():
        print(f"\n  ── {category} ──")
        for key, label, _ in items:
            print(f"    [{key:>3}]  {label}")
    print(f"\n    [  Q]  Quit")
    print()


def main():
    # Verify databases exist
    print("\n  Checking databases...")
    for name, path in DATABASES.items():
        exists = "✅" if os.path.exists(path) else "❌ NOT FOUND"
        print(f"    {name:12s} {exists}")
    
    if "--sql" in sys.argv:
        q_custom_sql()
        return
    
    # Build lookup for menu
    menu_lookup = {}
    for category, items in MENU.items():
        for key, label, func in items:
            menu_lookup[key.upper()] = func
    
    show_menu()
    
    while True:
        choice = input("  Choose [0-19, L, SQL, Q]: ").strip().upper()
        
        if choice in ('Q', 'QUIT', 'EXIT'):
            print("\n  👋 Goodbye!\n")
            break
        elif choice == 'MENU' or choice == 'M' or choice == 'H' or choice == '?':
            show_menu()
        elif choice in menu_lookup:
            try:
                menu_lookup[choice]()
            except Exception as e:
                print(f"\n  ❌ Error: {e}")
        else:
            print(f"  ❌ Unknown option: {choice}. Type 'M' for menu.")


if __name__ == "__main__":
    main()
