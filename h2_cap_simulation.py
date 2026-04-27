#!/usr/bin/env python3
"""
h2_cap_simulation.py — Cap-attributable alpha leak quantification

Memo support for Step 2 H2 (Followup #4 from Apr 18 Overnight Agents note).

Method:
  For each candidate MAX_TOTAL_OPEN cap C in {20, 25, 30, 40, infinity},
  replay every fired signal chronologically. Maintain a virtual portfolio:
    - positions_open: list of (ticker, source, score, entry_date, exit_date, weight, alpha)
    - On each signal fire, first close any positions where exit_date <= signal_date
    - Then attempt to fill: if (len(positions_open) < C) AND (sum_weights + new_weight <= 1.0),
      add to portfolio; else block.
  Track captured EV ($) and leaked EV ($) per cap.

Constraints:
  (a) Capital constraint: sum of position weights cannot exceed 100% of NLV.
      When binding, signal is blocked (no partial fills).
  (c) Actual cap=20 simulation: cross-check against observed fills.
      Using expected_hold_days for exits (we don't know realized exit dates of
      blocked signals, so apply the same convention for actual cap=20 sim for
      apples-to-apples vs counterfactuals).
  (d) Counterfactual caps: expected_hold_days from signal_benchmarks.

Outputs:
  Table of {cap, captured_signals, blocked_signals, captured_ev_pct,
           leaked_ev_pct, capital_blocked_signals, count_cap_blocked}
  Plus annualized leak in bps of NLV.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

# Config
SIGNAL_DB = os.path.expanduser("~/gmc_data/signal_intelligence.db")
POSITIONS_DB = os.path.expanduser("~/gmc_data/positions.db")
START_DATE = "2026-04-07"
END_DATE = "2026-04-23"  # last scan_date in window
NLV = 6103.45  # most recent live cron NLV

# SCORE_PCT mapping (from ib_execution/config.py)
SCORE_PCT = {2: 0.03, 3: 0.05, 4: 0.08, 5: 0.08}
DEFAULT_PCT = 0.05  # for score=None signals (CEL_BEAR, DIV_CUT)

# Caps to simulate
CAPS = [20, 25, 30, 40, 999]

# Signals to exclude (DIV_CUT pre-go-live manual closes are not a normal trade)
EXCLUDE_SIGNALS = {"DIV_CUT"}


@dataclass
class Position:
    ticker: str
    source: str
    score: Optional[int]
    entry_date: date
    exit_date: date
    weight: float
    alpha: float  # expected return per trade (decimal, e.g. 0.0424)
    captured_ev_dollars: float = 0.0


def parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def load_signals():
    """Load all fired signals in window, chronological."""
    with closing(sqlite3.connect(SIGNAL_DB)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT scan_date, scanner, ticker, direction, score
            FROM signal_log
            WHERE scan_date BETWEEN ? AND ?
              AND fired = 1
            ORDER BY scan_date, scanner, ticker
            """,
            (START_DATE, END_DATE),
        ).fetchall()
    return [dict(r) for r in rows]


def load_benchmarks():
    """Map (source, direction) → (alpha_pct, hold_days)."""
    out = {}
    with closing(sqlite3.connect(POSITIONS_DB)) as conn:
        for src, direction, ret_pct, hold in conn.execute(
            "SELECT source, direction, expected_return_pct, expected_hold_days FROM signal_benchmarks"
        ):
            out[(src, direction)] = (ret_pct / 100.0, int(hold))
    return out


def score_pct(score):
    if score is None:
        return DEFAULT_PCT
    try:
        return SCORE_PCT.get(int(score), DEFAULT_PCT)
    except Exception:
        return DEFAULT_PCT


def simulate(signals, benchmarks, cap):
    """
    Simulate one cap level. Returns dict with:
      captured_count, blocked_cap_count, blocked_capital_count,
      captured_ev, leaked_ev, blocked_signals_detail
    """
    open_positions: list[Position] = []
    captured_count = 0
    blocked_cap = 0
    blocked_capital = 0
    captured_ev = 0.0  # in $
    leaked_ev = 0.0  # in $
    blocked_detail = []  # (date, scanner, ticker, score, alpha, weight, reason)

    for sig in signals:
        if sig["scanner"] in EXCLUDE_SIGNALS:
            continue
        sd = parse_date(sig["scan_date"])
        if sd is None:
            continue

        # Look up alpha + hold
        bench_key = (sig["scanner"], sig["direction"])
        if bench_key not in benchmarks:
            continue
        alpha_pct, hold_days = benchmarks[bench_key]
        weight = score_pct(sig["score"])
        ev_dollars = weight * NLV * alpha_pct

        # Close positions whose hold period has ended
        open_positions = [p for p in open_positions if p.exit_date > sd]

        # Attempt fill
        deployed = sum(p.weight for p in open_positions)
        if len(open_positions) >= cap:
            blocked_cap += 1
            leaked_ev += ev_dollars
            blocked_detail.append(
                (sig["scan_date"], sig["scanner"], sig["ticker"],
                 sig["score"], alpha_pct, weight, "CAP")
            )
            continue
        if deployed + weight > 1.0:
            blocked_capital += 1
            leaked_ev += ev_dollars
            blocked_detail.append(
                (sig["scan_date"], sig["scanner"], sig["ticker"],
                 sig["score"], alpha_pct, weight, "CAPITAL")
            )
            continue

        # Fill
        exit_date = sd + timedelta(days=hold_days)
        pos = Position(
            ticker=sig["ticker"],
            source=sig["scanner"],
            score=sig["score"],
            entry_date=sd,
            exit_date=exit_date,
            weight=weight,
            alpha=alpha_pct,
            captured_ev_dollars=ev_dollars,
        )
        open_positions.append(pos)
        captured_count += 1
        captured_ev += ev_dollars

    total_signals = captured_count + blocked_cap + blocked_capital
    return {
        "cap": cap,
        "total": total_signals,
        "captured_count": captured_count,
        "blocked_cap_count": blocked_cap,
        "blocked_capital_count": blocked_capital,
        "captured_ev_dollars": captured_ev,
        "leaked_ev_dollars": leaked_ev,
        "captured_ev_pct_nlv": captured_ev / NLV * 100,
        "leaked_ev_pct_nlv": leaked_ev / NLV * 100,
        "blocked_detail": blocked_detail,
    }


def annualize(window_pct, window_trading_days, annual_days=252):
    return window_pct * (annual_days / window_trading_days)


def run_pead_offset_sensitivity(signals, benchmarks):
    """
    Followup #3 sensitivity: simulate as if PEAD signals were processed
    same-day (no +2 offset). The offset is structural in PA's pead_scanner,
    not in autotrader behavior, so the simulation already treats every signal
    as same-day-processable. The offset matters for measure_tc.py's matching
    of capacity_events to signal_log -- but in this simulation we ignore
    capacity_events and replay directly from signal_log.

    So the as-is simulation IS the post-Followup-#3 state for simulation
    purposes. The offset sensitivity question becomes: does removing the +2
    offset change anything? Answer: NO, because the simulation processes each
    signal on its scan_date regardless of offset.

    The REAL sensitivity is whether the +2 offset, in production today, means
    PEAD signals are competing for slots that have already been taken by 2-
    day-newer 8K/CEL fires. The simulation as written matches the post-fix
    state. To model the as-is state (with +2 offset), we'd need to bump PEAD
    scan_dates forward by 2 days, putting them at the back of any same-day
    queue. That's the sensitivity case we want.
    """
    bumped_signals = []
    for sig in signals:
        sig_copy = dict(sig)
        if sig["scanner"].startswith("PEAD"):
            sd = parse_date(sig["scan_date"])
            if sd is not None:
                sig_copy["scan_date"] = (sd + timedelta(days=2)).isoformat()
        bumped_signals.append(sig_copy)
    bumped_signals.sort(key=lambda s: (s["scan_date"], s["scanner"], s["ticker"]))
    return bumped_signals


def trading_days_in_window(signals):
    """Count unique scan_dates in signals (proxy for trading days)."""
    return len({s["scan_date"] for s in signals})


def main():
    print("=" * 78)
    print("H2 CAP-ATTRIBUTABLE ALPHA LEAK SIMULATION")
    print(f"Window: {START_DATE} → {END_DATE}")
    print(f"NLV: ${NLV:,.2f}")
    print(f"SCORE_PCT: {SCORE_PCT}, default (None) = {DEFAULT_PCT}")
    print(f"Excluded scanners: {EXCLUDE_SIGNALS}")
    print("=" * 78)

    signals = load_signals()
    benchmarks = load_benchmarks()

    print(f"\nLoaded {len(signals)} fired signals (pre-exclusion)")
    excluded = sum(1 for s in signals if s["scanner"] in EXCLUDE_SIGNALS)
    print(f"After excluding {EXCLUDE_SIGNALS}: {len(signals) - excluded} signals")
    print(f"Loaded {len(benchmarks)} benchmark entries")

    n_trading_days = trading_days_in_window(signals)
    print(f"Unique scan_dates with fires: {n_trading_days}")
    annualization = 252 / n_trading_days
    print(f"Annualization factor: 252/{n_trading_days} = {annualization:.3f}x")

    # Distribution of fires by scanner+score
    print("\n--- Signal distribution (post-exclusion) ---")
    dist = {}
    for s in signals:
        if s["scanner"] in EXCLUDE_SIGNALS:
            continue
        key = (s["scanner"], s["score"])
        dist[key] = dist.get(key, 0) + 1
    print(f"{'Scanner':<14} {'Score':<6} {'Fires':>6} {'SCORE_PCT':>10} {'Alpha':>8}")
    for (scanner, score), n in sorted(dist.items()):
        bench_key = (scanner, "BUY" if "BULL" in scanner or scanner == "DIV_CUT"
                     else "SHORT")
        if bench_key not in benchmarks:
            # Try direction lookup from a sample signal
            for s in signals:
                if s["scanner"] == scanner:
                    bench_key = (scanner, s["direction"])
                    break
        alpha = benchmarks.get(bench_key, (0, 0))[0]
        pct = score_pct(score)
        print(f"{scanner:<14} {str(score):<6} {n:>6} {pct*100:>9.1f}% {alpha*100:>7.2f}%")

    # ----- AS-IS (current PEAD +2 offset structurally bumps PEAD to back of queue) -----
    print("\n" + "=" * 78)
    print("AS-IS SIMULATION: PEAD +2 day offset (structural, not yet fixed)")
    print("PEAD signals scan_date is bumped +2 days to model autotrader's lag.")
    print("=" * 78)

    asis_signals = run_pead_offset_sensitivity(signals, benchmarks)
    asis_results = []
    for cap in CAPS:
        r = simulate(asis_signals, benchmarks, cap)
        asis_results.append(r)
        print(f"\nCap = {cap if cap < 999 else 'INFINITY'}")
        print(f"  Total signals processed:    {r['total']}")
        print(f"  Captured (filled):          {r['captured_count']}")
        print(f"  Blocked (cap):              {r['blocked_cap_count']}")
        print(f"  Blocked (capital):          {r['blocked_capital_count']}")
        print(f"  Captured EV ($):            ${r['captured_ev_dollars']:,.2f}")
        print(f"  Leaked EV ($):              ${r['leaked_ev_dollars']:,.2f}")
        print(f"  Captured EV (% NLV):        {r['captured_ev_pct_nlv']:.3f}%")
        print(f"  Leaked EV (% NLV):          {r['leaked_ev_pct_nlv']:.3f}%")
        ann_capt = annualize(r['captured_ev_pct_nlv'], n_trading_days)
        ann_leak = annualize(r['leaked_ev_pct_nlv'], n_trading_days)
        print(f"  ANNUALIZED captured (% NLV):{ann_capt:.2f}%  ({ann_capt*100:.0f} bps)")
        print(f"  ANNUALIZED leaked (% NLV):  {ann_leak:.2f}%  ({ann_leak*100:.0f} bps)")

    # ----- POST-FOLLOWUP-#3: PEAD same-day processing -----
    print("\n" + "=" * 78)
    print("POST-FOLLOWUP-#3 SIMULATION: PEAD scan_date used as-is (no +2 bump)")
    print("Models the world AFTER pead_scanner.py line 721 fix is deployed.")
    print("=" * 78)

    post_results = []
    for cap in CAPS:
        r = simulate(signals, benchmarks, cap)
        post_results.append(r)
        print(f"\nCap = {cap if cap < 999 else 'INFINITY'}")
        print(f"  Total signals processed:    {r['total']}")
        print(f"  Captured (filled):          {r['captured_count']}")
        print(f"  Blocked (cap):              {r['blocked_cap_count']}")
        print(f"  Blocked (capital):          {r['blocked_capital_count']}")
        print(f"  Captured EV ($):            ${r['captured_ev_dollars']:,.2f}")
        print(f"  Leaked EV ($):              ${r['leaked_ev_dollars']:,.2f}")
        print(f"  Captured EV (% NLV):        {r['captured_ev_pct_nlv']:.3f}%")
        print(f"  Leaked EV (% NLV):          {r['leaked_ev_pct_nlv']:.3f}%")
        ann_capt = annualize(r['captured_ev_pct_nlv'], n_trading_days)
        ann_leak = annualize(r['leaked_ev_pct_nlv'], n_trading_days)
        print(f"  ANNUALIZED captured (% NLV):{ann_capt:.2f}%  ({ann_capt*100:.0f} bps)")
        print(f"  ANNUALIZED leaked (% NLV):  {ann_leak:.2f}%  ({ann_leak*100:.0f} bps)")

    # ----- SUMMARY TABLE -----
    print("\n" + "=" * 78)
    print("SUMMARY TABLE — annualized as % of NLV (and bps in parens)")
    print("=" * 78)
    print(f"\n{'Cap':<8} {'AS-IS Capture':>16} {'AS-IS Leak':>16} {'POST-FX Capture':>18} {'POST-FX Leak':>16}")
    for asis, post in zip(asis_results, post_results):
        cap_str = str(asis['cap']) if asis['cap'] < 999 else "INF"
        ac = annualize(asis['captured_ev_pct_nlv'], n_trading_days)
        al = annualize(asis['leaked_ev_pct_nlv'], n_trading_days)
        pc = annualize(post['captured_ev_pct_nlv'], n_trading_days)
        pl = annualize(post['leaked_ev_pct_nlv'], n_trading_days)
        print(f"{cap_str:<8} {ac:>11.2f}% ({ac*100:>4.0f} bp) {al:>11.2f}% ({al*100:>4.0f} bp) {pc:>13.2f}% ({pc*100:>4.0f} bp) {pl:>11.2f}% ({pl*100:>4.0f} bp)")

    # ----- INCREMENTAL CAPTURE PER ADDITIONAL SLOT -----
    print("\n" + "=" * 78)
    print("INCREMENTAL CAPTURE PER ADDITIONAL CAP SLOT (annualized bps)")
    print("=" * 78)
    print(f"\n{'Cap transition':<18} {'AS-IS Δ-capture':>18} {'POST-FX Δ-capture':>20}")
    for i in range(1, len(CAPS)):
        prev_a, curr_a = asis_results[i - 1], asis_results[i]
        prev_p, curr_p = post_results[i - 1], post_results[i]
        a_delta = annualize(curr_a['captured_ev_pct_nlv'] - prev_a['captured_ev_pct_nlv'], n_trading_days) * 100
        p_delta = annualize(curr_p['captured_ev_pct_nlv'] - prev_p['captured_ev_pct_nlv'], n_trading_days) * 100
        prev_cap = str(CAPS[i - 1]) if CAPS[i - 1] < 999 else "INF"
        curr_cap = str(CAPS[i]) if CAPS[i] < 999 else "INF"
        slots = CAPS[i] - CAPS[i - 1] if CAPS[i] < 999 else "+inf"
        print(f"{prev_cap}→{curr_cap:<6} (+{slots:<4}){a_delta:>14.0f} bp{p_delta:>16.0f} bp")

    # ----- CAPITAL CONSTRAINT BIND POINT -----
    print("\n" + "=" * 78)
    print("CAPITAL CONSTRAINT (when 100% deployed binds before cap):")
    print("=" * 78)
    for r in asis_results + post_results:
        if r['blocked_capital_count'] > 0:
            cap_str = str(r['cap']) if r['cap'] < 999 else "INF"
            label = "AS-IS" if r in asis_results else "POST-FX"
            print(f"  {label} Cap={cap_str}: {r['blocked_capital_count']} signals blocked by capital "
                  f"({r['blocked_capital_count']*100/r['total']:.1f}% of total)")

    print("\n" + "=" * 78)
    print("Done. Use this output to populate the H2 memo.")
    print("=" * 78)


if __name__ == "__main__":
    main()
