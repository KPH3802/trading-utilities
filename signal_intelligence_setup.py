#!/usr/bin/env python3
"""
GMC Signal Intelligence — Setup & Backfill

Creates ~/gmc_data/signal_intelligence.db and backfills historical data
from all existing backtest DBs. This is the permanent signal archive.
All future scanner runs will append to it.

Usage:
  python3 signal_intelligence_setup.py          # Create DB + backfill
  python3 signal_intelligence_setup.py --verify  # Print summary only (no insert)
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SIGNAL_DB = os.path.expanduser("~/gmc_data/signal_intelligence.db")
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PEAD_DB    = os.path.join(BASE_DIR, "pead_backtest", "pead_results.db")
SI_DB      = os.path.join(BASE_DIR, "short_interest_backtest", "si_results.db")
COT_DB     = os.path.join(BASE_DIR, "cot_backtest", "cot_backtest.db")
CEL_DB     = os.path.join(BASE_DIR, "commodity_lag_backtest", "commodity_lag_backtest.db")
THIRTEENF_DB = os.path.join(BASE_DIR, "thirteenf_backtest", "thirteenf_data.db")
EIGHTK_DB  = os.path.join(BASE_DIR, "eight_k_research", "backtest_results_v2.db")
DIVCUT_DB  = os.path.join(BASE_DIR, "dividend_scanner", "dividend_scanner.db")


# ---------------------------------------------------------------------------
# Step 1: Create signal_intelligence.db
# ---------------------------------------------------------------------------

def create_db():
    """Create the signal_log table if it doesn't exist."""
    os.makedirs(os.path.dirname(SIGNAL_DB), exist_ok=True)
    conn = sqlite3.connect(SIGNAL_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS signal_log (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_date             TEXT,
            scanner               TEXT,
            ticker                TEXT,
            direction             TEXT,
            fired                 INTEGER,
            signal_strength       REAL,
            signal_bucket         TEXT,
            regime_filter_passed  INTEGER,
            regime_value          REAL,
            score                 INTEGER,
            autotrader_acted      INTEGER,
            entry_date            TEXT,
            exit_date             TEXT,
            ret_pct               REAL,
            alpha_vs_spy          REAL,
            UNIQUE(scanner, ticker, entry_date)
        )
    """)
    conn.commit()
    conn.close()
    print(f"[OK] signal_intelligence.db ready: {SIGNAL_DB}")


# ---------------------------------------------------------------------------
# Step 2: Backfill functions — one per scanner
# ---------------------------------------------------------------------------

def _connect_source(db_path):
    """Open source DB read-only. Returns conn or None."""
    if not os.path.exists(db_path):
        print(f"  [SKIP] DB not found: {db_path}")
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        print(f"  [WARN] Cannot open {db_path}: {e}")
        return None


def _safe_float(val):
    """Convert to float or None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_int(val):
    """Convert to int or None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def backfill_pead(dest):
    """PEAD: 9,653 rows — surprise_pct as strength, ret_4w as return."""
    src = _connect_source(PEAD_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT ticker, signal, surprise_pct, surprise_bucket,
               entry_date, ret_4w
        FROM pead_trades
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        direction = "BUY" if r["signal"] == "BULL" else "SHORT"
        scanner = "PEAD_BULL" if r["signal"] == "BULL" else "PEAD_BEAR"
        ret = _safe_float(r["ret_4w"])
        # Convert raw return to percentage (stored as decimal in DB)
        if ret is not None:
            ret = round(ret * 100, 4)
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, ?, ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL,
                        ?, NULL, ?, NULL)
            """, (
                r["entry_date"], scanner, r["ticker"], direction,
                _safe_float(r["surprise_pct"]),
                r["surprise_bucket"],
                r["entry_date"], ret,
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_si(dest):
    """SI Squeeze: market_class='SC' only (deployed filter). 5,805 rows.
    change_pct as strength, ret_4w as return (stored as decimal)."""
    src = _connect_source(SI_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT ticker, signal, change_pct, change_bucket,
               entry_date, ret_4w
        FROM si_trades
        WHERE market_class = 'SC'
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        # SI Squeeze is reframed: high SI increase = BULL (squeeze setup)
        direction = "BUY"
        ret = _safe_float(r["ret_4w"])
        if ret is not None:
            ret = round(ret * 100, 4)
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, 'SI_SQUEEZE', ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL,
                        ?, NULL, ?, NULL)
            """, (
                r["entry_date"], r["ticker"], direction,
                _safe_float(r["change_pct"]),
                r["change_bucket"],
                r["entry_date"], ret,
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_cot(dest):
    """COT: 500 rows — percentile_rank as strength, ret_pct and alpha_pct
    already as percentages."""
    src = _connect_source(COT_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT commodity, signal, signal_date, etf, entry_date, exit_date,
               ret_pct, alpha_pct, percentile_rank, percentile_bucket
        FROM cot_trades
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        direction = "BUY" if r["signal"] == "BULL" else "SHORT"
        scanner = "COT_BULL" if r["signal"] == "BULL" else "COT_BEAR"
        # Ticker = etf (XOP, USO, GLD, etc), commodity in bucket label
        ticker = r["etf"] or r["commodity"]
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, ?, ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL,
                        ?, ?, ?, ?)
            """, (
                r["signal_date"], scanner, ticker, direction,
                _safe_float(r["percentile_rank"]),
                r["percentile_bucket"],
                r["entry_date"], r["exit_date"],
                _safe_float(r["ret_pct"]),
                _safe_float(r["alpha_pct"]),
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_cel(dest):
    """CEL: 1,545 rows — uso_move_pct as strength, ret_5d and alpha_5d
    already as percentages."""
    src = _connect_source(CEL_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT signal_date, signal, etf, uso_move_pct, move_bucket,
               entry_date, exit_date, ret_5d, alpha_5d
        FROM cel_trades
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        direction = "SHORT"  # CEL is always SHORT
        ticker = r["etf"]
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, 'CEL_BEAR', ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL,
                        ?, ?, ?, ?)
            """, (
                r["signal_date"], ticker, direction,
                _safe_float(r["uso_move_pct"]),
                r["move_bucket"],
                r["entry_date"], r["exit_date"],
                _safe_float(r["ret_5d"]),
                _safe_float(r["alpha_5d"]),
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_thirteenf(dest):
    """13F: 587 rows with entry_date (2,022 total, many pre-entry).
    new_initiations as strength, initiator_bucket as bucket.
    Rows without ret_91d are valid fired signals awaiting returns."""
    src = _connect_source(THIRTEENF_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT ticker, filing_date, entry_date, exit_date,
               new_initiations, initiator_bucket,
               ret_91d, alpha_91d
        FROM initiation_signals
        WHERE entry_date IS NOT NULL
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        direction = "BUY"  # 13F is always BULL/BUY
        bucket = r["initiator_bucket"]
        if bucket:
            bucket = str(bucket)
            if bucket not in ("3", "4", "5+"):
                try:
                    n = int(bucket)
                    bucket = "5+" if n >= 5 else str(n)
                except ValueError:
                    pass
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, 'THIRTEENF_BULL', ?, ?, 1, ?, ?, NULL, NULL, NULL, NULL,
                        ?, ?, ?, ?)
            """, (
                r["filing_date"], r["ticker"], direction,
                _safe_float(r["new_initiations"]),
                bucket,
                r["entry_date"], r["exit_date"],
                _safe_float(r["ret_91d"]),
                _safe_float(r["alpha_91d"]),
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_eightk(dest):
    """8-K Item 1.01: 31,381 rows. No score column in backtest DB — all are
    item 1.01 (the deployed signal). ret_5d as return, abnret_5d as alpha.
    Both stored as decimals, convert to pct."""
    src = _connect_source(EIGHTK_DB)
    if not src:
        return 0

    rows = src.execute("""
        SELECT ticker, filing_date, filing_price,
               ret_5d, abnret_5d
        FROM filing_returns
        WHERE item_code = '1.01'
    """).fetchall()
    src.close()

    c = dest.cursor()
    inserted = 0
    for r in rows:
        direction = "SHORT"  # 8-K 1.01 is always SHORT
        ret = _safe_float(r["ret_5d"])
        alpha = _safe_float(r["abnret_5d"])
        # Convert from decimal to percentage
        if ret is not None:
            # For SHORT: positive price move = loss. Flip sign.
            ret = round(-ret * 100, 4)
        if alpha is not None:
            alpha = round(-alpha * 100, 4)
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, '8K_1.01', ?, ?, 1, NULL, NULL, NULL, NULL, NULL, NULL,
                        ?, NULL, ?, ?)
            """, (
                r["filing_date"], r["ticker"], direction,
                r["filing_date"],
                ret, alpha,
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


def backfill_divcut(dest):
    """Div Cut: check for backtest data. Scanner DB may be empty (only 1 live
    trade so far). Populate from positions.db if any DIV_CUT trades exist."""
    # First try the dividend_scanner.db
    src = _connect_source(DIVCUT_DB)
    if src:
        try:
            tables = [r[0] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            if tables:
                # Has tables — try to pull data
                for t in tables:
                    count = src.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
                    if count > 0:
                        print(f"  [INFO] Div Cut table {t}: {count} rows — needs manual mapping")
            else:
                print("  [INFO] dividend_scanner.db is empty (no tables)")
        except Exception:
            pass
        src.close()

    # Fallback: pull DIV_CUT trades from positions.db (live trades only)
    positions_db = os.path.expanduser("~/gmc_data/positions.db")
    src = _connect_source(positions_db)
    if not src:
        return 0

    rows = src.execute("""
        SELECT ticker, direction, entry_date, close_date, entry_price,
               return_pct, alpha_vs_spy, score
        FROM open_positions
        WHERE source = 'DIV_CUT'
    """).fetchall()
    src.close()

    if not rows:
        print("  [INFO] No DIV_CUT trades in positions.db")
        return 0

    c = dest.cursor()
    inserted = 0
    for r in rows:
        try:
            c.execute("""
                INSERT OR IGNORE INTO signal_log
                (scan_date, scanner, ticker, direction, fired,
                 signal_strength, signal_bucket, regime_filter_passed,
                 regime_value, score, autotrader_acted,
                 entry_date, exit_date, ret_pct, alpha_vs_spy)
                VALUES (?, 'DIV_CUT', ?, ?, 1, ?, NULL, NULL, NULL, ?, 1,
                        ?, ?, ?, ?)
            """, (
                r["entry_date"], r["ticker"], r["direction"] or "BUY",
                _safe_float(r["score"]),
                _safe_int(r["score"]),
                r["entry_date"], r["close_date"],
                _safe_float(r["return_pct"]),
                _safe_float(r["alpha_vs_spy"]),
            ))
            if c.rowcount > 0:
                inserted += 1
        except Exception:
            pass
    dest.commit()
    return inserted


# ---------------------------------------------------------------------------
# Step 3: Summary
# ---------------------------------------------------------------------------

def print_summary(db_path):
    """Print per-scanner stats and total."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    total = c.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"  SIGNAL INTELLIGENCE DB — BACKFILL SUMMARY")
    print(f"{'='*60}")
    print(f"  Location: {db_path}")
    print(f"  Total rows: {total:,}")
    print(f"{'='*60}")

    scanners = c.execute("""
        SELECT scanner, COUNT(*),
               MIN(entry_date), MAX(entry_date),
               ROUND(AVG(ret_pct), 2), ROUND(AVG(alpha_vs_spy), 2)
        FROM signal_log
        GROUP BY scanner
        ORDER BY COUNT(*) DESC
    """).fetchall()

    print(f"\n  {'Scanner':<20} {'Rows':>8}  {'From':>12}  {'To':>12}  {'Avg Ret%':>10}  {'Avg Alpha':>10}")
    print(f"  {'-'*20} {'-'*8}  {'-'*12}  {'-'*12}  {'-'*10}  {'-'*10}")
    for name, count, dt_min, dt_max, avg_ret, avg_alpha in scanners:
        avg_r = f"{avg_ret:+.2f}%" if avg_ret is not None else "N/A"
        avg_a = f"{avg_alpha:+.2f}%" if avg_alpha is not None else "N/A"
        dt_min = dt_min or "N/A"
        dt_max = dt_max or "N/A"
        print(f"  {name:<20} {count:>8,}  {dt_min:>12}  {dt_max:>12}  {avg_r:>10}  {avg_a:>10}")

    print(f"\n  Scanners represented: {len(scanners)}")
    print(f"{'='*60}\n")
    conn.close()


# ---------------------------------------------------------------------------
# Step 4: Live logging stub (reusable by all scanners)
# ---------------------------------------------------------------------------

LIVE_LOGGING_STUB = '''
# ---------------------------------------------------------------------------
# Signal Intelligence — Live Logging (add to each PA scanner)
# ---------------------------------------------------------------------------
import sqlite3

SIGNAL_INTELLIGENCE_DB = "/Users/kevinheaney/gmc_data/signal_intelligence.db"

def log_to_signal_intelligence(scan_date, scanner, ticker, direction, fired,
                                signal_strength=None, signal_bucket=None,
                                regime_filter_passed=None, regime_value=None,
                                score=None, autotrader_acted=None,
                                entry_date=None, exit_date=None,
                                ret_pct=None, alpha_vs_spy=None):
    """Log one signal evaluation to signal_intelligence.db.
    Fails silently -- never blocks the scanner if DB write fails.
    """
    try:
        conn = sqlite3.connect(SIGNAL_INTELLIGENCE_DB, timeout=5)
        conn.execute("""
            INSERT OR IGNORE INTO signal_log
            (scan_date, scanner, ticker, direction, fired,
             signal_strength, signal_bucket, regime_filter_passed,
             regime_value, score, autotrader_acted,
             entry_date, exit_date, ret_pct, alpha_vs_spy)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (scan_date, scanner, ticker, direction, fired,
              signal_strength, signal_bucket, regime_filter_passed,
              regime_value, score, autotrader_acted,
              entry_date, exit_date, ret_pct, alpha_vs_spy))
        conn.commit()
        conn.close()
    except Exception:
        pass  # Never block the scanner


# --- Example call inside a scanner's signal loop: ---
#
# # In PEAD scanner, after each signal evaluation:
# log_to_signal_intelligence(
#     scan_date=today_str,
#     scanner="PEAD_BULL",
#     ticker=ticker,
#     direction="BUY",
#     fired=1,
#     signal_strength=surprise_pct,
#     signal_bucket=bucket_label,
#     score=None,
#     autotrader_acted=1,
#     entry_date=today_str,
# )
#
# # In 8-K scanner, after a signal is blocked by M&A filter:
# log_to_signal_intelligence(
#     scan_date=today_str,
#     scanner="8K_1.01",
#     ticker=ticker,
#     direction="SHORT",
#     fired=0,  # blocked
#     signal_strength=score,
#     signal_bucket=str(score),
#     score=score,
#     autotrader_acted=0,
# )
'''


def print_live_stub():
    """Print the live logging stub for copy-paste into scanners."""
    print("\n" + "="*60)
    print("  LIVE LOGGING STUB — Add to each PA scanner")
    print("="*60)
    print(LIVE_LOGGING_STUB)
    print("="*60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Signal Intelligence DB setup & backfill")
    parser.add_argument("--verify", action="store_true", help="Print summary only, no insert")
    args = parser.parse_args()

    if args.verify:
        if os.path.exists(SIGNAL_DB):
            print_summary(SIGNAL_DB)
        else:
            print(f"[ERROR] DB not found: {SIGNAL_DB}")
        return

    # Step 1: Create DB
    print("[Step 1] Creating signal_intelligence.db...")
    create_db()

    # Step 2: Backfill
    print("\n[Step 2] Backfilling from backtest DBs...")
    dest = sqlite3.connect(SIGNAL_DB)

    results = {}
    scanners = [
        ("PEAD",          backfill_pead),
        ("SI_SQUEEZE",    backfill_si),
        ("COT",           backfill_cot),
        ("CEL_BEAR",      backfill_cel),
        ("THIRTEENF_BULL", backfill_thirteenf),
        ("8K_1.01",       backfill_eightk),
        ("DIV_CUT",       backfill_divcut),
    ]

    for label, func in scanners:
        print(f"\n  Backfilling {label}...")
        count = func(dest)
        results[label] = count
        print(f"  → {label}: {count:,} rows inserted")

    dest.close()

    # Step 3: Summary
    print_summary(SIGNAL_DB)

    # Step 4: Live logging stub
    print_live_stub()

    print("[DONE] Signal Intelligence DB ready for production.")


if __name__ == "__main__":
    main()
