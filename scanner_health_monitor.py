#!/usr/bin/env python3
"""
Scanner-Execution Health Monitor -- Block 4 (Apr 27 2026).

Two-layer continuous monitor that surfaces scanner failures before they
cause silent alpha loss.

  L1 -- PA Task Return-Code Monitor
        Iterates every enabled scheduled task on PythonAnywhere, fetches
        the task log via the Files API, parses the most recent
        "Completed task ... return code was X" line, and compares the
        timestamp against the expected daily fire time. Alerts fire when
        return code != 0 or no completion within 90 minutes of expected.

  L2 -- signal_log Freshness Monitor
        Queries the local signal_intelligence.db for MAX(scan_date) per
        scanner. Alerts fire when a scanner has not posted a row within
        its expected cadence window. Scanners that should be writing
        but produce no rows at all (e.g. DIV_INITIATION) surface here.

Alert routing reuses the Layer A Pushover wrapper. Each distinct failure
is deduplicated within a rolling 6-hour window via state file at
~/.gmc_health_state.json. Three or more concurrent failures collapse
into a single [CRITICAL][GMC] bundle. Script exits 0 unconditionally so
cron remains green; failures are logged and pushed.

Cron entry (Mac Studio):
    */30 * * * * /bin/bash /Users/kevinheaney/run_scanner_health_monitor.sh
"""
from __future__ import annotations

import json
import os
import re
import sys
import sqlite3
import urllib.error
import urllib.request
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, date as ddate
from pathlib import Path

# ---------------------------------------------------------------------------
# Imports from ib_execution: PA token + Pushover wrapper
# ---------------------------------------------------------------------------
_IB_DIR = os.path.expanduser("~/Desktop/Claude_Programs/Trading_Programs/ib_execution")
if _IB_DIR not in sys.path:
    sys.path.insert(0, _IB_DIR)

try:
    from config import PA_API_TOKEN, PA_USERNAME
except Exception as e:
    print(f"[FATAL] cannot import PA_API_TOKEN/PA_USERNAME from config.py: {e}",
          file=sys.stderr)
    sys.exit(0)  # exit 0 per spec; cron stays green even on misconfig

try:
    from pushover_alerts import send_pushover
    HAVE_PUSHOVER = True
except Exception as e:
    print(f"[WARN] pushover_alerts unavailable: {e}", file=sys.stderr)
    HAVE_PUSHOVER = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_PATH = os.path.expanduser("~/gmc_data/signal_intelligence.db")
STATE_PATH = os.path.expanduser("~/.gmc_health_state.json")
LOG_PATH = os.path.expanduser("~/gmc_health_monitor.log")
LOG_MAX_LINES = 500
LOG_RETAIN_LINES = 400
DEDUP_WINDOW_SECONDS = 6 * 3600

PA_API_BASE = "https://www.pythonanywhere.com/api/v0/user"
PA_REQUEST_TIMEOUT = 15.0

# L1: alert if no successful completion within this window past expected fire.
L1_GRACE_MINUTES = 90

# L2 cadence registry. Scanners whose absence should trigger alerts. The
# scanner names match the literal `scanner` column values in signal_log.
# THIRTEENF_BULL is intentionally omitted -- quarterly cadence is too noisy
# for half-hourly polling.
L2_SCANNERS = [
    ("8K_1.01",         "weekday_daily"),
    ("PEAD_BULL",       "weekday_daily"),
    ("PEAD_BEAR",       "weekday_daily"),
    ("CEL_BEAR",        "weekday_daily"),
    ("SI_SQUEEZE",      "weekday_daily"),
    ("DIV_CUT",         "weekday_daily"),
    ("DIV_INITIATION",  "weekday_daily"),
    ("F4_BUY_CLUSTER",  "weekday_daily"),
    ("F4_SELL_S1",      "weekday_daily"),
    ("F4_SELL_S2",      "weekday_daily"),
    ("COT_BEAR",        "weekly_tue"),
    ("COT_BULL",        "weekly_tue"),
]

# Number of simultaneous failures that triggers a single CRITICAL bundle
# instead of N separate HIGH messages.
BUNDLE_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Time helpers (PA server time is UTC; signal_log dates are local YYYY-MM-DD)
# ---------------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def expected_last_daily_fire_utc(hour: int, minute: int) -> datetime:
    """Most recent UTC timestamp <= now at HH:MM."""
    n = now_utc()
    cand = n.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if cand > n:
        cand -= timedelta(days=1)
    return cand


def most_recent_weekday_today_or_prior() -> ddate:
    """Most recent weekday date including today if today is a weekday."""
    d = now_utc().date()
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def most_recent_weekday_completed() -> ddate:
    """Most recent completed weekday: today only after 22:00 UTC, else prior weekday."""
    n = now_utc()
    cutoff = n.replace(hour=22, minute=0, second=0, microsecond=0)
    d = n.date() if n >= cutoff else (n.date() - timedelta(days=1))
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def most_recent_tuesday() -> ddate:
    """Most recent Tuesday (CFTC release day) on or before today."""
    d = now_utc().date()
    while d.weekday() != 1:  # Mon=0 Tue=1
        d -= timedelta(days=1)
    return d


# ---------------------------------------------------------------------------
# Logger (file-based, capped, no exceptions raised)
# ---------------------------------------------------------------------------
class CappedLog:
    def __init__(self, path: str, max_lines: int, retain: int):
        self.path = path
        self.max_lines = max_lines
        self.retain = retain
        self._buf: list[str] = []

    def write(self, level: str, msg: str) -> None:
        ts = now_utc().strftime("%Y-%m-%d %H:%M:%S UTC")
        line = f"[{ts}] {level:<5} {msg}"
        self._buf.append(line)
        print(line)

    def flush(self) -> None:
        try:
            existing: list[str] = []
            if os.path.exists(self.path):
                with open(self.path, "r", encoding="utf-8") as fh:
                    existing = fh.read().splitlines()
            combined = existing + self._buf
            if len(combined) > self.max_lines:
                combined = combined[-self.retain:]
            with open(self.path, "w", encoding="utf-8") as fh:
                fh.write("\n".join(combined) + "\n")
        except Exception as e:
            print(f"[LOG_FLUSH_FAIL] {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# State file (for dedup)
# ---------------------------------------------------------------------------
def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, sort_keys=True)
    except Exception as e:
        print(f"[STATE_SAVE_FAIL] {e}", file=sys.stderr)


def is_deduped(state: dict, key: str, window_seconds: int) -> bool:
    last = state.get(key)
    if not last:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
    except Exception:
        return False
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    return (now_utc() - last_dt).total_seconds() < window_seconds


def mark_sent(state: dict, key: str) -> None:
    state[key] = now_utc().isoformat()


# ---------------------------------------------------------------------------
# PA API
# ---------------------------------------------------------------------------
class PAUnreachable(Exception):
    pass


def _pa_request(path: str) -> str:
    url = f"{PA_API_BASE}/{PA_USERNAME}/{path}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Token {PA_API_TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=PA_REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        raise PAUnreachable(f"HTTP {e.code} on {path}") from e
    except Exception as e:
        raise PAUnreachable(f"{type(e).__name__}: {e}") from e


def fetch_pa_tasks() -> list[dict]:
    body = _pa_request("schedule/")
    return json.loads(body)


def fetch_pa_task_log(task_id: int) -> str:
    return _pa_request(f"files/path/var/log/schedule-log-{task_id}.log")


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------
_COMPLETED_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) -- Completed task,.*?return code was (?P<rc>-?\d+)\.?\s*$"
)


def parse_last_completion(log_text: str) -> tuple[datetime, int] | None:
    """Return (utc_ts, return_code) for the most recent completion line, else None."""
    last_match = None
    for line in log_text.splitlines():
        m = _COMPLETED_RE.match(line)
        if m:
            last_match = m
    if not last_match:
        return None
    ts = datetime.strptime(last_match.group("ts"), "%Y-%m-%d %H:%M:%S")
    ts = ts.replace(tzinfo=timezone.utc)
    return ts, int(last_match.group("rc"))


_PY_RE = re.compile(r"([\w_-]+)\.py")


def task_label(task: dict) -> str:
    """Friendly identifier built from the command -- last *.py basename."""
    cmd = task.get("command", "")
    matches = _PY_RE.findall(cmd)
    if matches:
        return matches[-1]
    return f"task_{task['id']}"


# ---------------------------------------------------------------------------
# L1 evaluation
# ---------------------------------------------------------------------------
@dataclass
class Failure:
    layer: str          # "L1" or "L2"
    key: str            # dedup key
    severity: str       # "high" | "emergency"
    title: str          # subject line
    body: str           # full message body
    short: str = ""     # one-line summary for bundling


def evaluate_l1(log: CappedLog) -> tuple[list[Failure], bool]:
    """Returns (failures, pa_ok). pa_ok=False means PA API unreachable."""
    failures: list[Failure] = []

    try:
        tasks = fetch_pa_tasks()
    except PAUnreachable as e:
        log.write("ERROR", f"L1: PA tasks fetch failed: {e}")
        failures.append(Failure(
            layer="L1",
            key="L1:pa_api_unreachable",
            severity="emergency",
            title="[CRITICAL][GMC] PA API unreachable",
            body=f"Scanner health monitor cannot reach PythonAnywhere Schedule API.\n"
                 f"Reason: {e}\nL1 task return-code monitoring is offline.",
            short="PA API unreachable",
        ))
        return failures, False
    except Exception as e:
        log.write("ERROR", f"L1: unexpected fetch error: {e}")
        failures.append(Failure(
            layer="L1",
            key="L1:pa_api_unreachable",
            severity="emergency",
            title="[CRITICAL][GMC] PA API unreachable",
            body=f"Scanner health monitor failed parsing PA task list.\nReason: {e}",
            short="PA API unreachable",
        ))
        return failures, False

    enabled = [t for t in tasks if t.get("enabled")]
    log.write("INFO", f"L1: {len(enabled)} enabled tasks (skipped {len(tasks) - len(enabled)} disabled)")

    for task in enabled:
        tid = task["id"]
        label = task_label(task)
        interval = task.get("interval", "daily")
        if interval != "daily":
            # Spec confirmed: PA tasks are all daily today; if PA ever exposes
            # weekly tasks here we lack the day-of-week field to evaluate them.
            log.write("WARN", f"L1: skipping {label} ({tid}) -- non-daily interval={interval}")
            continue

        hour = int(task["hour"])
        minute = int(task["minute"])
        expected = expected_last_daily_fire_utc(hour, minute)
        deadline = expected + timedelta(minutes=L1_GRACE_MINUTES)
        if now_utc() < deadline:
            # Inside grace window for the most recent expected fire.
            log.write("INFO",
                      f"L1: {label} ({tid}) within grace window (expected={expected:%H:%M UTC})")
            continue

        try:
            log_text = fetch_pa_task_log(tid)
        except Exception as e:
            log.write("ERROR", f"L1: log fetch failed for {label} ({tid}): {e}")
            failures.append(Failure(
                layer="L1",
                key=f"L1:{tid}:log_fetch_failed",
                severity="high",
                title=f"[HIGH][GMC] L1 log fetch failed: {label}",
                body=f"Could not fetch PA log for task {label} (id={tid}): {e}",
                short=f"{label}: log fetch failed",
            ))
            continue

        parsed = parse_last_completion(log_text)
        if parsed is None:
            log.write("WARN", f"L1: {label} ({tid}) no completion line in log")
            failures.append(Failure(
                layer="L1",
                key=f"L1:{tid}:no_completion",
                severity="high",
                title=f"[HIGH][GMC] {label} no completion record",
                body=f"PA task {label} (id={tid}) log has no 'Completed task ...' line.\n"
                     f"Expected most recent fire at {expected:%Y-%m-%d %H:%M UTC}.",
                short=f"{label}: no completion record",
            ))
            continue

        last_ts, rc = parsed

        if last_ts < expected - timedelta(minutes=5):
            # Last completion is older than the most recent expected fire.
            age_h = (now_utc() - last_ts).total_seconds() / 3600.0
            log.write("WARN",
                      f"L1: {label} ({tid}) STALE last={last_ts:%Y-%m-%d %H:%M UTC} "
                      f"expected>={expected:%H:%M UTC} age={age_h:.1f}h")
            sev = "emergency" if age_h > 48 else "high"
            failures.append(Failure(
                layer="L1",
                key=f"L1:{tid}:no_recent_run",
                severity=sev,
                title=f"[HIGH][GMC] {label} did not fire",
                body=f"PA task {label} (id={tid}) has not run since "
                     f"{last_ts:%Y-%m-%d %H:%M UTC}.\n"
                     f"Expected fire >= {expected:%Y-%m-%d %H:%M UTC} "
                     f"(now {age_h:.1f}h stale).",
                short=f"{label}: stale {age_h:.1f}h",
            ))
            continue

        if rc != 0:
            log.write("WARN", f"L1: {label} ({tid}) RC={rc} at {last_ts:%Y-%m-%d %H:%M UTC}")
            failures.append(Failure(
                layer="L1",
                key=f"L1:{tid}:rc_nonzero",
                severity="high",
                title=f"[HIGH][GMC] {label} returned RC={rc}",
                body=f"PA task {label} (id={tid}) completed with non-zero return code.\n"
                     f"Last fire: {last_ts:%Y-%m-%d %H:%M UTC}\n"
                     f"Return code: {rc}",
                short=f"{label}: RC={rc}",
            ))
            continue

        log.write("INFO",
                  f"L1: {label} ({tid}) ok last={last_ts:%Y-%m-%d %H:%M UTC} rc=0")

    return failures, True


# ---------------------------------------------------------------------------
# L2 evaluation
# ---------------------------------------------------------------------------
def expected_signal_date(cadence: str) -> tuple[ddate, int]:
    """Returns (expected MAX(scan_date), 2x_threshold_age_in_days).

    For weekday_daily, expected is the most recent completed weekday, and
    the 2x threshold is 2 weekdays of staleness.
    For weekly_tue (CFTC release), expected is the most recent Tuesday and
    the 2x threshold is 14 days.
    """
    if cadence == "weekday_daily":
        return most_recent_weekday_completed(), 2
    if cadence == "weekly_tue":
        return most_recent_tuesday(), 14
    raise ValueError(f"unknown cadence {cadence}")


def evaluate_l2(log: CappedLog) -> list[Failure]:
    failures: list[Failure] = []

    if not os.path.exists(DB_PATH):
        log.write("ERROR", f"L2: DB missing at {DB_PATH}")
        failures.append(Failure(
            layer="L2",
            key="L2:db_missing",
            severity="emergency",
            title="[CRITICAL][GMC] signal_intelligence.db missing",
            body=f"L2 freshness monitor cannot find DB at {DB_PATH}.",
            short="signal_intelligence.db missing",
        ))
        return failures

    try:
        with closing(sqlite3.connect(DB_PATH)) as conn:
            rows = conn.execute(
                "SELECT scanner, MAX(scan_date) FROM signal_log GROUP BY scanner"
            ).fetchall()
    except Exception as e:
        log.write("ERROR", f"L2: DB query failed: {e}")
        failures.append(Failure(
            layer="L2",
            key="L2:db_query_failed",
            severity="high",
            title="[HIGH][GMC] L2 DB query failed",
            body=f"signal_intelligence.db query raised {type(e).__name__}: {e}",
            short="L2 DB query failed",
        ))
        return failures

    db_max: dict[str, str | None] = {scanner: max_d for scanner, max_d in rows}

    today = now_utc().date()

    for scanner, cadence in L2_SCANNERS:
        expected_date, two_x_days = expected_signal_date(cadence)
        max_date_str = db_max.get(scanner)

        if not max_date_str:
            log.write("WARN", f"L2: {scanner} NEVER recorded any signal_log row")
            failures.append(Failure(
                layer="L2",
                key=f"L2:{scanner}:never_recorded",
                severity="high",
                title=f"[HIGH][GMC] {scanner} has zero signal_log rows",
                body=f"{scanner} ({cadence}) has never written to signal_log.\n"
                     f"Expected MAX(scan_date) >= {expected_date}.",
                short=f"{scanner}: zero rows",
            ))
            continue

        try:
            max_date = ddate.fromisoformat(max_date_str)
        except Exception as e:
            log.write("ERROR", f"L2: {scanner} bad scan_date '{max_date_str}': {e}")
            continue

        age_days = (today - max_date).days

        if max_date >= expected_date:
            log.write("INFO", f"L2: {scanner} fresh MAX(scan_date)={max_date_str}")
            continue

        # Stale. Bump severity if beyond 2x cadence.
        beyond_2x = age_days > two_x_days
        sev = "high" if beyond_2x else "high"  # both tiers ride high; bundle handles CRITICAL
        log.write("WARN",
                  f"L2: {scanner} STALE MAX(scan_date)={max_date_str} "
                  f"expected>={expected_date} age={age_days}d 2x={two_x_days}d")
        title_tag = "[HIGH][GMC]" if beyond_2x else "[GMC]"
        failures.append(Failure(
            layer="L2",
            key=f"L2:{scanner}:stale",
            severity=sev,
            title=f"{title_tag} {scanner} signal_log stale {age_days}d",
            body=f"{scanner} MAX(scan_date) = {max_date_str}\n"
                 f"Expected >= {expected_date} ({cadence})\n"
                 f"Age: {age_days} days "
                 f"({'beyond 2x cadence' if beyond_2x else 'within 2x cadence'})",
            short=f"{scanner}: stale {age_days}d",
        ))

    return failures


# ---------------------------------------------------------------------------
# Alert dispatch
# ---------------------------------------------------------------------------
def dispatch(failures: list[Failure], state: dict, log: CappedLog) -> None:
    if not failures:
        log.write("INFO", "All checks passed -- no alerts to send")
        return

    # If a CRITICAL severity is already present (e.g. PA unreachable), keep
    # individual messaging for visibility on the cause but still respect dedup.
    critical_present = any(f.severity == "emergency" for f in failures)

    # Bundle path: 3+ concurrent failures collapse into one CRITICAL message.
    if len(failures) >= BUNDLE_THRESHOLD and not critical_present:
        bundle_key = "L0:multi_failure_bundle"
        if is_deduped(state, bundle_key, DEDUP_WINDOW_SECONDS):
            log.write("INFO",
                      f"Bundle of {len(failures)} suppressed by 6h dedup ({bundle_key})")
            return
        title = f"[CRITICAL][GMC] {len(failures)} concurrent scanner failures"
        body_lines = [
            f"GMC scanner health monitor detected {len(failures)} concurrent failures:",
            "",
            *[f"  - {f.short or f.title}" for f in failures],
            "",
            "Run scanner_health_monitor.py interactively for details.",
        ]
        ok = _send(title, "\n".join(body_lines), priority="emergency", log=log)
        if ok:
            mark_sent(state, bundle_key)
            # Mark each underlying failure too so we don't immediately re-page
            # them once dedup expires on the bundle.
            for f in failures:
                mark_sent(state, f.key)
        return

    # Per-failure path with dedup.
    for f in failures:
        if is_deduped(state, f.key, DEDUP_WINDOW_SECONDS):
            log.write("INFO", f"Alert suppressed by 6h dedup: {f.key}")
            continue
        ok = _send(f.title, f.body, priority=f.severity, log=log)
        if ok:
            mark_sent(state, f.key)


def _send(title: str, body: str, priority: str, log: CappedLog) -> bool:
    if not HAVE_PUSHOVER:
        log.write("ERROR", f"Pushover unavailable, would have sent: {title}")
        return False
    try:
        ok = send_pushover(body, priority=priority, title=title)
        log.write("INFO" if ok else "WARN",
                  f"Pushover send: priority={priority} title={title!r} ok={ok}")
        return bool(ok)
    except Exception as e:
        log.write("ERROR", f"Pushover send raised {type(e).__name__}: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    log = CappedLog(LOG_PATH, LOG_MAX_LINES, LOG_RETAIN_LINES)
    log.write("INFO", "=== scanner_health_monitor run start ===")

    try:
        state = load_state()

        l1_failures, _pa_ok = evaluate_l1(log)
        l2_failures = evaluate_l2(log)

        all_failures = l1_failures + l2_failures
        log.write("INFO",
                  f"Summary: L1={len(l1_failures)} L2={len(l2_failures)} "
                  f"total={len(all_failures)}")

        dispatch(all_failures, state, log)
        save_state(state)

    except Exception as e:
        log.write("ERROR", f"Top-level exception {type(e).__name__}: {e}")

    log.write("INFO", "=== scanner_health_monitor run end ===")
    log.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
