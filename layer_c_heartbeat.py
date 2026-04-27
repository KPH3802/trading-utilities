#!/usr/bin/env python3
"""
GMC Layer C Heartbeat -- Out-of-band liveness + DB/cron freshness check.

Runs hourly via cron on Mac Studio. For each monitored source, decides if it
is fresh relative to its expected cron schedule. Three-state output:

    All fresh        -> ping HEALTHCHECKS_LAYER_C_URL              (Healthchecks stays green)
    Stale found      -> ping {url}/fail with body + send Pushover (Healthchecks alerts now)
    Script unable    -> no ping; Healthchecks alerts after 90m grace period

Monitored sources (10):
    1. signal_intelligence.db::signal_log MAX(scan_date)            [data freshness, weekday 06:15 CT]
    2-6. ~/gmc_data/macro_data/{cot,eia,flows,geopolitical,vix_skew}_data.db  [weekday 17:00 CT]
    7. ~/Desktop/.../ib_execution/cron_run.log                       [weekday 08:00 CT]
    8. ~/gmc_macro_runner.log                                        [weekday 17:00 CT]
    9. ~/gmc_sync_cron.log                                           [weekday 06:15 CT]
    10. ~/gmc_rsync.log                                              [daily 01:30 CT]

Built Apr 26 2026 -- Layer C v1, scope locked Apr 25 AM Addendum + expanded Apr 26 PM (Option B).
"""
from __future__ import annotations

import os
import sys
import sqlite3
import urllib.request
from contextlib import closing
from datetime import datetime, timedelta, date as ddate
from pathlib import Path
import zoneinfo

# ---------------------------------------------------------------------------
# Imports from ib_execution config + Pushover
# ---------------------------------------------------------------------------
_IB_DIR = os.path.expanduser("~/Desktop/Claude_Programs/Trading_Programs/ib_execution")
if _IB_DIR not in sys.path:
    sys.path.insert(0, _IB_DIR)

try:
    from config import HEALTHCHECKS_LAYER_C_URL
except Exception as e:
    print(f"[FATAL] cannot import HEALTHCHECKS_LAYER_C_URL from config.py: {e}", file=sys.stderr)
    sys.exit(2)

try:
    from pushover_alerts import send_pushover
    HAVE_PUSHOVER = True
except Exception as e:
    print(f"[WARN] pushover_alerts unavailable, will skip Pushover on stale: {e}", file=sys.stderr)
    HAVE_PUSHOVER = False


# ---------------------------------------------------------------------------
# Time / schedule helpers (US/Central, weekday-aware)
# ---------------------------------------------------------------------------
CT = zoneinfo.ZoneInfo("America/Chicago")

def now_ct() -> datetime:
    return datetime.now(CT)

def expected_last_fire(hour: int, minute: int, weekdays_only: bool) -> datetime:
    """Most recent timestamp <= now_ct() at HH:MM CT matching schedule.

    weekday(): Mon=0..Sun=6, so weekdays_only walks back over Sat/Sun.
    """
    n = now_ct()
    candidate = n.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate > n:
        candidate -= timedelta(days=1)
    if weekdays_only:
        while candidate.weekday() >= 5:  # 5=Sat, 6=Sun
            candidate -= timedelta(days=1)
    return candidate

def file_mtime_ct(path: str) -> datetime | None:
    p = Path(path).expanduser()
    if not p.exists():
        return None
    return datetime.fromtimestamp(p.stat().st_mtime, tz=CT)


# ---------------------------------------------------------------------------
# Individual checks: each returns (ok: bool, msg: str)
# ---------------------------------------------------------------------------
GRACE_HOURS = 1.0  # how late after expected fire before flagging stale

def check_file_fresh(name: str, path: str, hour: int, minute: int, weekdays_only: bool):
    expected = expected_last_fire(hour, minute, weekdays_only)
    mtime = file_mtime_ct(path)
    if mtime is None:
        return False, f"{name}: FILE MISSING at {path}"
    # mtime should be >= expected - grace.
    # Equivalently: (expected - mtime) > grace means stale.
    if (expected - mtime) > timedelta(hours=GRACE_HOURS):
        age_h = (now_ct() - mtime).total_seconds() / 3600.0
        return False, (
            f"{name}: STALE mtime={mtime:%Y-%m-%d %H:%M %Z} "
            f"expected>={expected:%Y-%m-%d %H:%M %Z} "
            f"age={age_h:.1f}h"
        )
    return True, f"{name}: fresh mtime={mtime:%Y-%m-%d %H:%M %Z}"

def check_signal_log_fresh():
    """signal_log MAX(scan_date) should be >= most recent post-sync weekday date.

    sync_signal_intelligence cron: weekday 06:15 CT.
    """
    db = os.path.expanduser("~/gmc_data/signal_intelligence.db")
    if not os.path.exists(db):
        return False, "signal_log: DB MISSING"
    try:
        with closing(sqlite3.connect(db)) as conn:
            row = conn.execute("SELECT MAX(scan_date) FROM signal_log").fetchone()
        max_date_str = row[0] if row else None
        if not max_date_str:
            return False, "signal_log: empty MAX(scan_date)"

        # Determine expected date: most recent weekday for which 06:15 CT has passed.
        n = now_ct()
        sync_today = n.replace(hour=6, minute=15, second=0, microsecond=0)
        candidate = n if n >= sync_today else (n - timedelta(days=1))
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        expected_date = candidate.date()

        max_date = ddate.fromisoformat(max_date_str)
        if max_date < expected_date:
            return False, (
                f"signal_log: STALE MAX(scan_date)={max_date_str} "
                f"expected>={expected_date}"
            )
        return True, f"signal_log: fresh MAX(scan_date)={max_date_str}"
    except Exception as e:
        return False, f"signal_log: ERROR {e}"


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------
def run_all_checks():
    results = []  # list of (ok, msg)

    # 1. signal_log content freshness
    results.append(check_signal_log_fresh())

    # 2-6. 5 macro DBs (mtime, weekday 17:00 CT)
    macro_dir = os.path.expanduser("~/gmc_data/macro_data")
    for db in ("cot_data.db", "eia_data.db", "flows.db",
               "geopolitical_indices.db", "vix_skew_data.db"):
        results.append(check_file_fresh(
            f"macro:{db}", os.path.join(macro_dir, db),
            hour=17, minute=0, weekdays_only=True,
        ))

    # 7. ib_autotrader cron log (weekday 08:00 CT)
    results.append(check_file_fresh(
        "cron:ib_autotrader",
        "~/Desktop/Claude_Programs/Trading_Programs/ib_execution/cron_run.log",
        hour=8, minute=0, weekdays_only=True,
    ))

    # 8. macro_runner cron log (weekday 17:00 CT)
    results.append(check_file_fresh(
        "cron:macro_runner",
        "~/gmc_macro_runner.log",
        hour=17, minute=0, weekdays_only=True,
    ))

    # 9. sync_signal_intelligence cron log (weekday 06:15 CT)
    results.append(check_file_fresh(
        "cron:sync_signal",
        "~/gmc_sync_cron.log",
        hour=6, minute=15, weekdays_only=True,
    ))

    # 10. gmc_rsync cron log (daily 01:30 CT)
    results.append(check_file_fresh(
        "cron:rsync",
        "~/gmc_rsync.log",
        hour=1, minute=30, weekdays_only=False,
    ))

    return results


# ---------------------------------------------------------------------------
# Reporting / pinging
# ---------------------------------------------------------------------------
def ping_url(url: str, body: bytes | None = None, timeout: float = 10.0) -> tuple[bool, str]:
    try:
        if body is None:
            urllib.request.urlopen(url, timeout=timeout)
        else:
            req = urllib.request.Request(
                url, data=body, headers={"Content-Type": "text/plain; charset=utf-8"},
            )
            urllib.request.urlopen(req, timeout=timeout)
        return True, "ok"
    except Exception as e:
        return False, str(e)


def main() -> int:
    ts = now_ct().strftime("%Y-%m-%d %H:%M:%S %Z")
    results = run_all_checks()

    failures = [m for ok, m in results if not ok]
    successes = [m for ok, m in results if ok]

    if failures:
        body_lines = [
            f"GMC Layer C STALE @ {ts}",
            "",
            f"FAILURES ({len(failures)}):",
            *[f"  - {m}" for m in failures],
            "",
            f"FRESH ({len(successes)}):",
            *[f"  - {m}" for m in successes],
        ]
        body = "\n".join(body_lines)
        print(body)

        # Ping /fail with details
        fail_url = HEALTHCHECKS_LAYER_C_URL.rstrip("/") + "/fail"
        ok, err = ping_url(fail_url, body=body.encode("utf-8"))
        print(f"[{ts}] /fail ping: {'OK' if ok else 'ERROR ' + err}")

        # Pushover with concise summary
        if HAVE_PUSHOVER:
            try:
                names = [m.split(":", 1)[0] for m in failures]
                push_msg = (
                    f"Layer C STALE: {len(failures)} source(s)\n"
                    + "\n".join(f"- {n}" for n in names)
                )
                send_pushover(push_msg, priority="high", title="GMC Layer C")
                print(f"[{ts}] Pushover sent")
            except Exception as e:
                print(f"[{ts}] Pushover ERROR: {e}")

        return 1

    # All fresh -> success ping
    print(f"GMC Layer C OK @ {ts} -- all {len(successes)} sources fresh")
    for m in successes:
        print(f"  + {m}")
    ok, err = ping_url(HEALTHCHECKS_LAYER_C_URL)
    print(f"[{ts}] success ping: {'OK' if ok else 'ERROR ' + err}")
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
