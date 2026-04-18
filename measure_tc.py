#!/usr/bin/env python3
"""
measure_tc.py -- GMC Transfer Coefficient measurement

Computes TC per Clarke-de Silva-Thorley (2002), Eq 7 / A15:

    TC = corr(Dw_i * sigma_i,  alpha_i / sigma_i^2)

across every trading day since 2026-04-07, plus a pooled aggregate and a
counterfactual TC that assumes no cap constraint.

Also persists daily 60-day annualized sigma per ticker to
signal_intelligence.db::ticker_sigma_history so the series accumulates over
time (Data Collection Principle, Apr 11 2026).

Standalone, on-demand. Runtime target < 3 minutes.
"""

from __future__ import annotations

import math
import os
import sqlite3
import statistics
import sys
import time
from contextlib import closing
from datetime import date, datetime, timedelta

import numpy as np
import requests

try:
    import yfinance as yf
    HAVE_YFINANCE = True
except Exception:
    HAVE_YFINANCE = False

# ---------------------------------------------------------------------------
# Config (pulled from ib_execution/config.py where possible)
# ---------------------------------------------------------------------------
_IB_DIR = os.path.expanduser("~/Desktop/Claude_Programs/Trading_Programs/ib_execution")
if _IB_DIR not in sys.path:
    sys.path.insert(0, _IB_DIR)
try:
    from config import SCORE_PCT, EVENT_ALPHA_ACCOUNT_VALUE, FMP_API_KEY
except Exception as e:
    print(f"[FATAL] Failed to import ib_execution/config.py: {e}")
    sys.exit(1)

SIGNAL_DB = os.path.expanduser("~/gmc_data/signal_intelligence.db")
POSITIONS_DB = os.path.expanduser("~/gmc_data/positions.db")
LAST_EA_VALUE_PATH = os.path.expanduser("~/gmc_data/last_ea_value.txt")

START_DATE = "2026-04-07"
TODAY = date.today()
COMPUTE_DATE = TODAY.isoformat()
REPORT_PATH = f"/tmp/gmc_tc_report_{TODAY.strftime('%Y%m%d')}.txt"

# Counterfactual weight when score is NULL (DIV_CUT, CEL_BEAR). Mirrors
# score=3 and is surfaced in the report.
DEFAULT_CF_PCT = 0.05

# Scanner emails arrive EOD on PA; the autotrader fires the next NYSE session.
# So entry_date is typically scan_date + 1 or +2 calendar days.
LAG_MATCH_DAYS = 4


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def ensure_sigma_table():
    with closing(sqlite3.connect(SIGNAL_DB)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_sigma_history (
                ticker TEXT,
                compute_date TEXT,
                sigma_60d_annualized REAL,
                source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, compute_date)
            )
            """
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Price fetch: yfinance -> Yahoo direct -> FMP /stable/
# ---------------------------------------------------------------------------
def _closes_yfinance(ticker: str, days: int = 100):
    if not HAVE_YFINANCE:
        return None
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=f"{days}d", auto_adjust=True)
        if hist is None or hist.empty:
            return None
        closes = [float(x) for x in hist["Close"].dropna().tolist()]
        if len(closes) < 30:
            return None
        return closes, "yfinance"
    except Exception:
        return None


def _closes_yahoo_direct(ticker: str, days: int = 100):
    try:
        end = int(time.time())
        start = end - (days + 14) * 86400
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
            f"?period1={start}&period2={end}&interval=1d&events=history"
        )
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200 or len(r.text) < 100:
            return None
        closes = []
        for line in r.text.splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 6:
                continue
            try:
                closes.append(float(parts[4]))
            except Exception:
                continue
        if len(closes) < 30:
            return None
        return closes, "yahoo_direct"
    except Exception:
        return None


def _closes_fmp(ticker: str, days: int = 100):
    try:
        url = (
            "https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={ticker}&apikey={FMP_API_KEY}"
        )
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        rows = data if isinstance(data, list) else data.get("historical", [])
        if not rows:
            return None
        rows = sorted(rows, key=lambda x: x.get("date", ""))
        closes = []
        for row in rows:
            c = row.get("close") or row.get("adjClose")
            if c is None:
                continue
            try:
                closes.append(float(c))
            except Exception:
                continue
        closes = closes[-days:]
        if len(closes) < 30:
            return None
        return closes, "fmp_stable"
    except Exception:
        return None


def compute_sigma_60d(ticker: str):
    for fetcher in (_closes_yfinance, _closes_yahoo_direct, _closes_fmp):
        res = fetcher(ticker, days=100)
        if not res:
            continue
        closes, src = res
        closes = closes[-60:]
        if len(closes) < 30:
            continue
        arr = np.asarray(closes, dtype=float)
        if np.any(arr <= 0):
            continue
        log_rets = np.diff(np.log(arr))
        if log_rets.size < 20:
            continue
        sd = float(np.std(log_rets, ddof=1))
        if not math.isfinite(sd) or sd <= 0:
            continue
        return sd * math.sqrt(252), src
    return None, None


def get_sigma(ticker: str):
    """Return cached sigma for (ticker, today) or compute + persist.
    Third tuple element: True if this call wrote a new row."""
    with closing(sqlite3.connect(SIGNAL_DB)) as conn:
        row = conn.execute(
            "SELECT sigma_60d_annualized, source FROM ticker_sigma_history "
            "WHERE ticker=? AND compute_date=?",
            (ticker, COMPUTE_DATE),
        ).fetchone()
    if row and row[0] is not None:
        return float(row[0]), row[1], False
    sigma, src = compute_sigma_60d(ticker)
    inserted = False
    if sigma is not None:
        with closing(sqlite3.connect(SIGNAL_DB)) as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO ticker_sigma_history "
                "(ticker, compute_date, sigma_60d_annualized, source) VALUES (?,?,?,?)",
                (ticker, COMPUTE_DATE, sigma, src),
            )
            conn.commit()
            inserted = cur.rowcount > 0
    return sigma, src, inserted


# ---------------------------------------------------------------------------
# Capital
# ---------------------------------------------------------------------------
def get_event_alpha_capital():
    if os.path.exists(LAST_EA_VALUE_PATH):
        try:
            with open(LAST_EA_VALUE_PATH) as f:
                val = float(f.read().strip())
            if val > 0:
                return val, "last_ea_value.txt"
        except Exception:
            pass
    return float(EVENT_ALPHA_ACCOUNT_VALUE), "config_fallback"


# ---------------------------------------------------------------------------
# DB pulls
# ---------------------------------------------------------------------------
def fetch_fired_signals():
    with closing(sqlite3.connect(SIGNAL_DB)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT scan_date, scanner, ticker, direction, fired, score, created_at
            FROM signal_log
            WHERE scan_date >= ? AND fired = 1
            ORDER BY scan_date, scanner, ticker
            """,
            (START_DATE,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_positions():
    # open_positions holds both OPEN and CLOSED rows -- status column carries
    # the state. There is no separate closed_positions table.
    with closing(sqlite3.connect(POSITIONS_DB)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT ticker, entry_date, source, direction, position_size, score, status
            FROM open_positions
            WHERE entry_date >= ?
            """,
            (START_DATE,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_capacity_events():
    with closing(sqlite3.connect(POSITIONS_DB)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT event_date, blocked_ticker, blocked_source, blocked_direction, blocked_score
            FROM capacity_events
            WHERE event_date >= ?
            """,
            (START_DATE,),
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_benchmarks():
    out = {}
    with closing(sqlite3.connect(POSITIONS_DB)) as conn:
        cur = conn.execute(
            "SELECT source, direction, expected_return_pct, expected_hold_days FROM signal_benchmarks"
        )
        for src, direction, ret_pct, hold in cur.fetchall():
            out[(src, direction)] = (ret_pct, hold)
    return out


# ---------------------------------------------------------------------------
# Match helpers
# ---------------------------------------------------------------------------
def _parse_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def find_position(positions, signal):
    ticker, scanner, scan_date = signal["ticker"], signal["scanner"], signal["scan_date"]
    sd = _parse_date(scan_date)
    for p in positions:
        if p["ticker"] == ticker and p["source"] == scanner and p["entry_date"] == scan_date:
            return p, "exact"
    if sd is None:
        return None, None
    for delta in range(1, LAG_MATCH_DAYS + 1):
        cand = (sd + timedelta(days=delta)).isoformat()
        for p in positions:
            if p["ticker"] == ticker and p["source"] == scanner and p["entry_date"] == cand:
                return p, "lagged"
    return None, None


def in_cap_events(cap_events, signal):
    ticker, scanner, scan_date = signal["ticker"], signal["scanner"], signal["scan_date"]
    sd = _parse_date(scan_date)
    for e in cap_events:
        if e["blocked_ticker"] != ticker:
            continue
        if (e["blocked_source"] or "") != scanner:
            continue
        ed = _parse_date(e["event_date"])
        if ed is None or sd is None:
            continue
        if 0 <= (ed - sd).days <= LAG_MATCH_DAYS:
            return True
    return False


def lookup_alpha(benchmarks, scanner, direction, score):
    if score is not None:
        k = (f"{scanner}_S{score}", direction)
        if k in benchmarks:
            return benchmarks[k][0] / 100.0
    k = (scanner, direction)
    if k in benchmarks:
        return benchmarks[k][0] / 100.0
    return None


def cf_weight(score):
    if score is None:
        return DEFAULT_CF_PCT
    try:
        return SCORE_PCT.get(int(score), DEFAULT_CF_PCT)
    except Exception:
        return DEFAULT_CF_PCT


def reconciliation_stats(signals, cap_events):
    """For every fired signal, find the nearest (scanner, ticker)-matched row
    in capacity_events and record the event_date - scan_date offset. Group by
    scanner. A systematic non-zero median offset indicates pipeline timing
    drift between PA's scanner logs and autotrader execution attempts."""
    idx: dict = {}
    for e in cap_events:
        k = ((e["blocked_source"] or ""), e["blocked_ticker"])
        ed = _parse_date(e["event_date"])
        if ed is None:
            continue
        idx.setdefault(k, []).append(ed)

    per_scanner: dict = {}
    for s in signals:
        scanner = s["scanner"]
        per_scanner.setdefault(scanner, {"n_fires": 0, "offsets": []})
        per_scanner[scanner]["n_fires"] += 1
        sd = _parse_date(s["scan_date"])
        if sd is None:
            continue
        best = None
        for cd in idx.get((scanner, s["ticker"]), []):
            off = (cd - sd).days
            if best is None or abs(off) < abs(best):
                best = off
        if best is not None:
            per_scanner[scanner]["offsets"].append(best)
    return per_scanner


def count_unexplained(signals, positions, cap_events):
    n = 0
    for s in signals:
        pos, _ = find_position(positions, s)
        if pos is None and not in_cap_events(cap_events, s):
            n += 1
    return n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    lines = []

    def p(s=""):
        print(s)
        lines.append(s)

    ensure_sigma_table()
    ea_value, ea_source = get_event_alpha_capital()

    signals = fetch_fired_signals()
    positions = fetch_positions()
    cap_events = fetch_capacity_events()
    benchmarks = fetch_benchmarks()

    p("=" * 70)
    p("GMC TRANSFER COEFFICIENT ANALYSIS")
    p(f"{START_DATE} -> {TODAY.isoformat()}")
    p("(Clarke-de Silva-Thorley 2002, Eq 7 / A15)")
    p("=" * 70)
    p("")
    p(f"Event Alpha capital: ${ea_value:,.2f}  [source: {ea_source}]")
    p(f"Fired signals:        {len(signals)}")
    p(f"Positions since start: {len(positions)}")
    p(f"Capacity events:      {len(cap_events)}")
    p(f"Benchmarks loaded:    {len(benchmarks)}")
    p("")

    # Early warning banner -- uses cheap match classification (no sigma needed)
    prelim_unex = count_unexplained(signals, positions, cap_events)
    if signals and (prelim_unex / len(signals)) >= 0.10:
        pct = prelim_unex / len(signals) * 100
        p("*" * 70)
        p(f"*  WARNING: {prelim_unex}/{len(signals)} ({pct:.0f}%) signals are 'unexplained'")
        p(f"*  (no position match, not in capacity_events). TC is a LOWER BOUND.")
        p(f"*  See SCANNER-EXECUTION RECONCILIATION below for root cause.")
        p("*" * 70)
        p("")

    # -----------------------------------------------------------------------
    # Sigma collection (persisted to ticker_sigma_history)
    # -----------------------------------------------------------------------
    unique_tickers = sorted({s["ticker"] for s in signals if s["ticker"]})
    p(f"Computing 60d annualized sigma for {len(unique_tickers)} unique tickers ...")
    sigma_map: dict[str, float] = {}
    sigma_src_map: dict[str, str] = {}
    sigma_missing: list[str] = []
    new_sigma_rows = 0
    for i, t in enumerate(unique_tickers, 1):
        sigma, src, inserted = get_sigma(t)
        if sigma is None:
            sigma_missing.append(t)
            print(f"  [{i:>3}/{len(unique_tickers)}] {t:<8} sigma=MISSING")
        else:
            sigma_map[t] = sigma
            sigma_src_map[t] = src or ""
            if inserted:
                new_sigma_rows += 1
            print(f"  [{i:>3}/{len(unique_tickers)}] {t:<8} sigma={sigma:.4f} ({src})")
    p("")

    # -----------------------------------------------------------------------
    # Enrich each signal
    # -----------------------------------------------------------------------
    enriched = []
    by_scanner: dict[str, list] = {}
    for s in signals:
        ticker = s["ticker"]
        scanner = s["scanner"]
        direction = s["direction"]
        score = s["score"]
        scan_date = s["scan_date"]

        sigma = sigma_map.get(ticker)
        alpha = lookup_alpha(benchmarks, scanner, direction, score)
        pos, match_type = find_position(positions, s)
        cap_hit = in_cap_events(cap_events, s)

        if pos is not None:
            pos_size = float(pos["position_size"] or 0.0)
            dw = pos_size / ea_value if ea_value else 0.0
            pos_status = pos.get("status") or "UNKNOWN"
            reason = f"filled({match_type})"
        else:
            pos_size = 0.0
            dw = 0.0
            pos_status = None
            reason = "cap_blocked" if cap_hit else "unexplained"

        row = {
            "scan_date": scan_date,
            "scanner": scanner,
            "ticker": ticker,
            "direction": direction,
            "score": score,
            "sigma": sigma,
            "alpha": alpha,
            "dw_actual": dw,
            "dw_cf": cf_weight(score),
            "position_size": pos_size,
            "match_type": match_type,
            "pos_status": pos_status,
            "reason": reason,
            "cap_hit": cap_hit,
        }
        enriched.append(row)
        by_scanner.setdefault(scanner, []).append(row)

    # -----------------------------------------------------------------------
    # Daily TC
    # -----------------------------------------------------------------------
    p("DAILY TC:")
    p(f"{'date':<11} {'N':>3} {'cap':>4} {'TC_actual':>10} {'TC_cf':>8} {'delta':>8}")

    by_date: dict[str, list] = {}
    for e in enriched:
        by_date.setdefault(e["scan_date"], []).append(e)

    daily_rows = []
    for day in sorted(by_date):
        day_signals = by_date[day]
        usable = [e for e in day_signals if e["sigma"] and e["alpha"] is not None and e["sigma"] > 0]
        cap_bound = any(e["cap_hit"] for e in day_signals)

        if len(usable) < 4:
            p(f"{day:<11} {len(day_signals):>3}  {'Y' if cap_bound else 'N':>3} "
              f"{'SKIP (N<4)':>10} {'':>8} {'':>8}")
            daily_rows.append({"date": day, "n": len(day_signals), "skipped": True,
                               "cap_bound": cap_bound})
            continue

        x = np.array([e["dw_actual"] * e["sigma"] for e in usable])
        y = np.array([e["alpha"] / (e["sigma"] ** 2) for e in usable])
        xcf = np.array([e["dw_cf"] * e["sigma"] for e in usable])

        def _corr(a, b):
            if a.std() == 0 or b.std() == 0:
                return float("nan")
            c = np.corrcoef(a, b)[0, 1]
            return float(c) if math.isfinite(c) else float("nan")

        tc_act = _corr(x, y)
        tc_cf = _corr(xcf, y)
        dtc = tc_cf - tc_act if math.isfinite(tc_act) and math.isfinite(tc_cf) else float("nan")

        for name, val in (("TC_actual", tc_act), ("TC_cf", tc_cf)):
            if math.isfinite(val) and (val < -1.0001 or val > 1.0001):
                raise ValueError(f"{name} out of [-1,1] on {day}: {val}")

        p(f"{day:<11} {len(usable):>3}  {'Y' if cap_bound else 'N':>3} "
          f"{tc_act:>10.3f} {tc_cf:>8.3f} {dtc:>+8.3f}")
        daily_rows.append({
            "date": day, "n": len(usable), "cap_bound": cap_bound,
            "tc_actual": tc_act, "tc_cf": tc_cf, "delta": dtc, "skipped": False,
        })

    drift_violations = [
        r for r in daily_rows
        if not r.get("skipped")
        and not r["cap_bound"]
        and math.isfinite(r["delta"])
        and abs(r["delta"]) >= 0.05
    ]
    p("")

    # -----------------------------------------------------------------------
    # Aggregate (pooled)
    # -----------------------------------------------------------------------
    pooled = [e for e in enriched if e["sigma"] and e["alpha"] is not None and e["sigma"] > 0]
    if len(pooled) >= 4:
        x_all = np.array([e["dw_actual"] * e["sigma"] for e in pooled])
        y_all = np.array([e["alpha"] / (e["sigma"] ** 2) for e in pooled])
        xcf_all = np.array([e["dw_cf"] * e["sigma"] for e in pooled])
        tc_all = (float(np.corrcoef(x_all, y_all)[0, 1])
                  if x_all.std() > 0 and y_all.std() > 0 else float("nan"))
        tc_cf_all = (float(np.corrcoef(xcf_all, y_all)[0, 1])
                     if xcf_all.std() > 0 and y_all.std() > 0 else float("nan"))
    else:
        tc_all, tc_cf_all = float("nan"), float("nan")

    p("AGGREGATE (all days pooled):")
    p(f"  N usable signals:           {len(pooled)} / {len(enriched)}")
    p(f"  TC_actual (pooled):         {tc_all:.3f}" if math.isfinite(tc_all)
      else "  TC_actual (pooled):         n/a")
    p(f"  TC_counterfactual (pooled): {tc_cf_all:.3f}" if math.isfinite(tc_cf_all)
      else "  TC_counterfactual (pooled): n/a")
    if math.isfinite(tc_all) and math.isfinite(tc_cf_all):
        p(f"  Cap-attributable delta-TC:  {(tc_cf_all - tc_all):+.3f}")
    p("")

    if math.isfinite(tc_all):
        tc2 = tc_all ** 2
        p("INTERPRETATION (CDT Eq 8-9, ex-post variance decomposition):")
        p(f"  TC^2   = {tc2*100:5.1f}%  of performance variance from signal quality")
        p(f"  1-TC^2 = {(1-tc2)*100:5.1f}%  from constraint noise")
        p("")

    # -----------------------------------------------------------------------
    # Per-scanner breakdown
    # -----------------------------------------------------------------------
    p("BY SIGNAL SOURCE:")
    for scanner in sorted(by_scanner):
        rows = by_scanner[scanner]
        filled = [r for r in rows if r["dw_actual"] > 0]
        mean_size = (np.mean([r["dw_actual"] for r in rows]) * 100) if rows else 0.0
        bench = next((r["alpha"] * 100 for r in rows if r["alpha"] is not None), None)
        bench_str = f"{bench:.2f}%" if bench is not None else "n/a"
        p(f"  {scanner:<12} N={len(rows):<3} filled={len(filled):<3} "
          f"mean_sized={mean_size:5.2f}%  expected={bench_str}")
    p("")

    # -----------------------------------------------------------------------
    # Cap binding
    # -----------------------------------------------------------------------
    cap_days = [r for r in daily_rows if r.get("cap_bound")]
    blocked_per_day = [
        sum(1 for e in by_date[r["date"]] if e["cap_hit"]) for r in cap_days
    ]
    total_days = len(by_date)
    p("CAP BINDING:")
    p(f"  Cap-binding days: {len(cap_days)} / {total_days}")
    p(f"  Avg blocked signals per cap-binding day: "
      f"{(np.mean(blocked_per_day) if blocked_per_day else 0.0):.1f}")
    p("")

    # -----------------------------------------------------------------------
    # Scanner-execution reconciliation
    # -----------------------------------------------------------------------
    recon = reconciliation_stats(signals, cap_events)
    p("SCANNER-EXECUTION RECONCILIATION:")
    p("  Per-scanner fire -> cap-block date offset (fires in signal_log vs")
    p("  blocks for same ticker in capacity_events). A systematic non-zero")
    p("  median offset indicates a scanner/autotrader pipeline timing mismatch.")
    p("")
    p(f"  {'scanner':<12} {'N_fires':>7} {'matched':>8} "
      f"{'median_offset':>14} {'mean_offset':>12}")
    pead_lag_detected = False
    for scanner in sorted(recon):
        stats = recon[scanner]
        offsets = stats["offsets"]
        if offsets:
            med = statistics.median(offsets)
            mean = statistics.mean(offsets)
            p(f"  {scanner:<12} {stats['n_fires']:>7} {len(offsets):>8} "
              f"{med:>+14.1f} {mean:>+12.2f}")
            if scanner.startswith("PEAD") and med >= 1.5:
                pead_lag_detected = True
        else:
            p(f"  {scanner:<12} {stats['n_fires']:>7} {0:>8} "
              f"{'n/a':>14} {'n/a':>12}")
    p("")

    if pead_lag_detected:
        p("FINDING -- PEAD 2-DAY EXECUTION LAG (diagnosed 2026-04-18):")
        p("  Median +2.0-day offset for PEAD scanners means the autotrader")
        p("  processes PEAD signals two days after PA's pead_scanner logs them")
        p("  fired.")
        p("")
        p("  Root cause: ib_autotrader.py line 94 -- PEAD_LOOKBACK_DAYS = 2.")
        p("  The comment on that line states: \"1 = today's email only --")
        p("  matches backtest entry timing.\" The current value of 2 is a")
        p("  deviation from backtest timing.")
        p("")
        p("  Mechanism: autotrader parses PEAD emails dated D-2. By the time")
        p("  those stale signals flow through, the MAX_TOTAL_OPEN=20 cap is")
        p("  full of newer (8K/CEL/SI) fires, so every PEAD ticker hits")
        p("  capacity and blocks. signal_log 'fired' tickers for day D and")
        p("  capacity_events 'blocked' tickers for day D appear to share")
        p("  almost no overlap -- because the cap-blocks on day D correspond")
        p("  to fires from day D-2, not day D.")
        p("")
        p("  Effect on TC: PEAD fires since Apr 7 produced 0 positions. With")
        p("  no ticker-level match to capacity_events on scan_date, they land")
        p("  in the 'unexplained' bucket even though they are really cap-")
        p("  blocked on a 2-day-lagged schedule. TC_actual is biased downward;")
        p("  1 - TC^2 'constraint noise' overstates the true capacity cost.")
        p("")
        p("  Fix: set PEAD_LOOKBACK_DAYS=1 in ib_autotrader.py. Validate entry")
        p("  timing matches the backtest before deploy. DEFERRED -- live-")
        p("  config change, separate session.")
        p("")

    # -----------------------------------------------------------------------
    # Data quality
    # -----------------------------------------------------------------------
    unexplained = [e for e in enriched if e["reason"] == "unexplained"]
    alpha_missing = [e for e in enriched if e["alpha"] is None]
    filled_open = [e for e in enriched if e["dw_actual"] > 0 and e["pos_status"] == "OPEN"]
    filled_closed = [e for e in enriched if e["dw_actual"] > 0 and e["pos_status"] == "CLOSED"]
    filled_other = [e for e in enriched if e["dw_actual"] > 0
                    and e["pos_status"] not in ("OPEN", "CLOSED")]

    p("DATA QUALITY:")
    p(f"  sigma computed:                {len(sigma_map)} / {len(unique_tickers)}")
    p(f"  sigma missing (excluded):      {len(sigma_missing)}"
      + (f" -> {', '.join(sigma_missing[:12])}{' ...' if len(sigma_missing) > 12 else ''}"
         if sigma_missing else ""))
    p(f"  ticker_sigma_history new rows: {new_sigma_rows}")
    p(f"  alpha missing (excluded):      {len(alpha_missing)}")
    p(f"  Filled signals -- OPEN:        {len(filled_open)}")
    p(f"  Filled signals -- CLOSED:      {len(filled_closed)}")
    if filled_other:
        p(f"  Filled signals -- OTHER:       {len(filled_other)}")
    p(f"  Unexplained Dw=0 signals:      {len(unexplained)}")

    # Per-signal matched status (helps a human debug lag/fill behavior)
    if filled_open or filled_closed:
        p("")
        p("MATCHED SIGNALS (status per fill):")
        shown = 0
        for e in enriched:
            if e["dw_actual"] <= 0:
                continue
            p(f"  {e['scan_date']} {e['scanner']:<12} {e['ticker']:<8} "
              f"{e['direction']:<5} score={str(e['score']):<4} "
              f"match={e['match_type']:<6} status={e['pos_status']}  "
              f"dw={e['dw_actual']*100:5.2f}%")
            shown += 1
            if shown >= 30:
                remaining = len([x for x in enriched if x["dw_actual"] > 0]) - shown
                if remaining > 0:
                    p(f"  ... {remaining} more")
                break

    if unexplained:
        p("")
        p("UNEXPLAINED Dw=0 SIGNALS (no position match, not in capacity_events):")
        for u in unexplained[:20]:
            p(f"  - {u['scan_date']} {u['scanner']:<12} {u['ticker']:<8} "
              f"{u['direction']:<5} score={u['score']}")
        if len(unexplained) > 20:
            p(f"  ... {len(unexplained) - 20} more")

    if drift_violations:
        p("")
        p("WARNING: non-cap-bound days with |TC - TC_cf| >= 0.05:")
        for r in drift_violations:
            p(f"  - {r['date']}  TC={r['tc_actual']:.3f}  TC_cf={r['tc_cf']:.3f}  "
              f"delta={r['delta']:+.3f}")
    p("")

    # -----------------------------------------------------------------------
    # ticker_sigma_history proof-of-collection
    # -----------------------------------------------------------------------
    p("TICKER_SIGMA_HISTORY (verification):")
    with closing(sqlite3.connect(SIGNAL_DB)) as conn:
        total = conn.execute("SELECT COUNT(*) FROM ticker_sigma_history").fetchone()[0]
        first5 = conn.execute(
            "SELECT ticker, compute_date, sigma_60d_annualized, source "
            "FROM ticker_sigma_history ORDER BY compute_date, ticker LIMIT 5"
        ).fetchall()
        last5 = conn.execute(
            "SELECT ticker, compute_date, sigma_60d_annualized, source "
            "FROM ticker_sigma_history ORDER BY compute_date DESC, ticker DESC LIMIT 5"
        ).fetchall()
    p(f"  total rows: {total}")
    p("  first 5:")
    for r in first5:
        p(f"    {r[0]:<8} {r[1]} sigma={r[2]:.4f} ({r[3]})")
    p("  last 5:")
    for r in last5:
        p(f"    {r[0]:<8} {r[1]} sigma={r[2]:.4f} ({r[3]})")
    p("")
    p(f"Report written to: {REPORT_PATH}")

    with open(REPORT_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
