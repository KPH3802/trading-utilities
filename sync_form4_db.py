"""
sync_form4_db.py

Pulls PA's form4_insider_trades.db (~975 MB) to Mac Studio so ib_autotrader.py
reads fresh F4 data each morning. Streaming download with atomic replace --
the live local DB is never overwritten unless the staged file passes
size, SQLite-integrity, and table-presence checks.

Read-only on PA. Single-file replace on Mac. Pattern parallels
sync_signal_intelligence.py (Apr 18, commit 90a5d97).

Exit codes:
    0  download ok, validations pass, atomic replace committed
    1  any hard failure (download, size floor, corrupt SQLite,
       missing canonical table, row-count floor, atomic rename)
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

import requests

# Config imported from ib_execution/config.py -- never hardcoded
sys.path.insert(
    0,
    str(Path("/Users/kevinheaney/Desktop/Claude_Programs/Trading_Programs/ib_execution")),
)
import config  # noqa: E402

# --------------------------------------------------------------------------- #
# Constants                                                                   #
# --------------------------------------------------------------------------- #

# Canonical local DB path -- derived from ib_execution/config.py FORM4_DB,
# which is "../form4_scanner/form4_insider_trades.db" relative to ib_execution.
LOCAL_DB = Path(
    "/Users/kevinheaney/gmc_data/form4_scanner/form4_insider_trades.db"  # Apr 29 2026: relocated to ~/gmc_data/
)
TMP_DB = LOCAL_DB.with_suffix(LOCAL_DB.suffix + ".tmp")

PA_URL = (
    f"https://www.pythonanywhere.com/api/v0/user/{config.PA_USERNAME}"
    f"/files/path/home/{config.PA_USERNAME}/form4_scanner/form4_insider_trades.db"
)

# Validation -- compare-against-incumbent. No magic size/row floors.
# Bootstrap (first usable run) accepts any SQLite-clean file with the
# required tables; subsequent runs apply strict guards (Apr 27 2026).
REQUIRED_TABLES = ("sent_alerts", "form4_transactions")  # ib_autotrader queries sent_alerts
SENT_ALERTS_ROW_FLOOR_PCT = 0.90  # new must have >= 90% of incumbent rows

SQLITE_MAGIC = b"SQLite format 3\x00"

# Log + heartbeat artifacts
LOG_PATH = Path.home() / "gmc_form4_sync.log"
SUCCESS_TOUCH = Path.home() / ".gmc_form4_sync_last_success"
LOG_RETAIN_LINES = 200

# Network timeouts: (connect, read-between-chunks). 975 MB at 20-50 MB/s
# is 20-50s end-to-end; per-chunk inactivity must allow slow segments.
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 120
CHUNK_SIZE = 1024 * 1024                 # 1 MB chunks


# --------------------------------------------------------------------------- #
# Logging                                                                     #
# --------------------------------------------------------------------------- #

_log_lines: list[str] = []


def emit(msg: str = "") -> None:
    line = msg
    print(line, flush=True)
    _log_lines.append(line)


def flush_log() -> None:
    """Append this run's lines to ~/gmc_form4_sync.log, retain last 200 lines."""
    try:
        existing: list[str] = []
        if LOG_PATH.exists():
            existing = LOG_PATH.read_text().splitlines()
        combined = existing + _log_lines
        retained = combined[-LOG_RETAIN_LINES:]
        LOG_PATH.write_text("\n".join(retained) + "\n")
    except Exception as e:
        # Don't let log writing failure mask the real result -- emit to stderr
        print(f"WARN: failed to write {LOG_PATH}: {e}", file=sys.stderr)


def cleanup_tmp() -> None:
    if TMP_DB.exists():
        try:
            TMP_DB.unlink()
        except Exception:
            pass


def die(msg: str) -> int:
    emit(f"FAIL: {msg}")
    emit("RESULT: FAILURE")
    cleanup_tmp()
    flush_log()
    return 1


# --------------------------------------------------------------------------- #
# Download                                                                    #
# --------------------------------------------------------------------------- #

def download() -> tuple[bool, str]:
    """Stream PA's form4_insider_trades.db into TMP_DB. Returns (ok, error)."""
    cleanup_tmp()  # remove any leftover from a prior crashed run

    try:
        resp = requests.get(
            PA_URL,
            headers={"Authorization": f"Token {config.PA_API_TOKEN}"},
            stream=True,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
    except requests.RequestException as e:
        return False, f"PA download request failed: {e}"

    if resp.status_code != 200:
        return False, f"PA download HTTP {resp.status_code}: {resp.text[:200]}"

    bytes_written = 0
    try:
        with open(TMP_DB, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    fh.write(chunk)
                    bytes_written += len(chunk)
    except (requests.RequestException, OSError) as e:
        return False, f"streaming write failed at {bytes_written} bytes: {e}"

    return True, ""


# --------------------------------------------------------------------------- #
# Validation                                                                  #
# --------------------------------------------------------------------------- #

def _inspect(db_path: Path) -> dict | None:
    """
    Open db_path as SQLite and return {tables, sent_alerts_rows, sent_alerts_max_date,
    size_bytes}. Returns None if path doesn't exist, file isn't SQLite-clean, required
    tables are missing, or any read errors -- caller treats None as "not a usable
    comparator" (bootstrap path).
    """
    if not db_path.exists():
        return None
    try:
        with open(db_path, "rb") as fh:
            if fh.read(16) != SQLITE_MAGIC:
                return None
        with closing(sqlite3.connect(db_path)) as conn:
            integrity = conn.execute("PRAGMA quick_check").fetchone()
            if not integrity or integrity[0] != "ok":
                return None
            tables = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            for t in REQUIRED_TABLES:
                if t not in tables:
                    return None
            sent_rows = conn.execute("SELECT COUNT(*) FROM sent_alerts").fetchone()[0]
            sent_max = conn.execute("SELECT MAX(alert_date) FROM sent_alerts").fetchone()[0]
            f4_rows = conn.execute("SELECT COUNT(*) FROM form4_transactions").fetchone()[0]
    except sqlite3.DatabaseError:
        return None

    return {
        "size_bytes": db_path.stat().st_size,
        "tables": sorted(tables),
        "sent_alerts_rows": sent_rows,
        "sent_alerts_max_date": sent_max,    # ISO 'YYYY-MM-DD' or None -> string compare ok
        "form4_transactions_rows": f4_rows,
    }


def validate(tmp_path: Path) -> tuple[bool, str, dict]:
    """
    Validate TMP_DB against the incumbent LOCAL_DB before atomic replace.

    Returns (ok, error, stats). stats always carries the new-file inspection
    plus a 'mode' key of either 'bootstrap' or 'strict'.

    Bootstrap (incumbent is not a usable comparator -- missing/corrupt/0 rows):
      accept if new is SQLite-clean and has required tables.

    Strict (incumbent is usable):
      new sent_alerts row count >= 90% of incumbent
      new sent_alerts MAX(alert_date) >= incumbent MAX(alert_date)
    """
    new_stats = _inspect(tmp_path)
    if new_stats is None:
        return False, "new file failed SQLite/table-presence check", {}

    inc = _inspect(LOCAL_DB)
    bootstrap = (inc is None) or (inc["sent_alerts_rows"] == 0)
    new_stats["mode"] = "bootstrap" if bootstrap else "strict"
    if inc is not None:
        new_stats["incumbent_sent_alerts_rows"] = inc["sent_alerts_rows"]
        new_stats["incumbent_sent_alerts_max_date"] = inc["sent_alerts_max_date"]

    if bootstrap:
        return True, "", new_stats

    # Strict guards
    floor = int(inc["sent_alerts_rows"] * SENT_ALERTS_ROW_FLOOR_PCT)
    if new_stats["sent_alerts_rows"] < floor:
        return (
            False,
            f"sent_alerts row count {new_stats['sent_alerts_rows']:,} below "
            f"{int(SENT_ALERTS_ROW_FLOOR_PCT * 100)}% of incumbent "
            f"{inc['sent_alerts_rows']:,} (floor {floor:,})",
            new_stats,
        )

    new_max = new_stats["sent_alerts_max_date"]
    inc_max = inc["sent_alerts_max_date"]
    if inc_max is not None and (new_max is None or new_max < inc_max):
        return (
            False,
            f"sent_alerts MAX(alert_date) regressed: new={new_max} < incumbent={inc_max}",
            new_stats,
        )

    return True, "", new_stats


# --------------------------------------------------------------------------- #
# Atomic replace                                                              #
# --------------------------------------------------------------------------- #

def atomic_replace() -> tuple[bool, str]:
    """os.rename TMP_DB over LOCAL_DB. Atomic on the same filesystem."""
    if not LOCAL_DB.parent.exists():
        return False, f"target dir does not exist: {LOCAL_DB.parent}"
    try:
        os.rename(TMP_DB, LOCAL_DB)
    except OSError as e:
        return False, f"os.rename failed: {e}"
    return True, ""


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

def main() -> int:
    started = time.time()
    pre_mtime = LOCAL_DB.stat().st_mtime if LOCAL_DB.exists() else None
    pre_size = LOCAL_DB.stat().st_size if LOCAL_DB.exists() else 0

    emit("PA FORM4 DB SYNC")
    emit(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    emit(f"Source:  {PA_URL}")
    emit(f"Target:  {LOCAL_DB}")
    if pre_mtime is not None:
        emit(
            "Local pre-state: "
            f"{pre_size/1024/1024:.1f} MB, "
            f"mtime {datetime.fromtimestamp(pre_mtime).strftime('%Y-%m-%d %H:%M:%S')}"
        )
    else:
        emit("Local pre-state: (no local DB yet)")
    emit("=" * 45)

    emit("DOWNLOAD")
    ok, err = download()
    if not ok:
        return die(err)
    dl_size = TMP_DB.stat().st_size
    emit(f"  Streamed bytes:    {dl_size:,} ({dl_size/1024/1024:.1f} MB)")
    emit(f"  Tmp path:          {TMP_DB}")

    emit("VALIDATE")
    ok, err, stats = validate(TMP_DB)
    if not ok:
        return die(f"validation failed: {err}")
    emit(f"  Size:              {stats['size_bytes']/1024/1024:.2f} MB")
    emit(f"  SQLite integrity:  ok")
    emit(f"  Tables present:    {', '.join(stats['tables'])}")
    emit(f"  sent_alerts rows:  {stats['sent_alerts_rows']:,}  (max alert_date {stats['sent_alerts_max_date']})")
    emit(f"  form4_transactions rows: {stats['form4_transactions_rows']:,}")
    emit(f"  Mode:              {stats['mode']}")
    if stats["mode"] == "strict":
        emit(
            f"  Incumbent ref:     {stats['incumbent_sent_alerts_rows']:,} rows, "
            f"max alert_date {stats['incumbent_sent_alerts_max_date']}"
        )

    emit("REPLACE")
    ok, err = atomic_replace()
    if not ok:
        return die(err)
    new_mtime = LOCAL_DB.stat().st_mtime
    emit(f"  Atomic rename:     ok")
    emit(f"  New mtime:         {datetime.fromtimestamp(new_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        SUCCESS_TOUCH.touch()
    except Exception as e:
        emit(f"WARN: success-touch failed: {e}")

    elapsed = time.time() - started
    size_mb = stats["size_bytes"] / 1024 / 1024
    sent_rows = stats["sent_alerts_rows"]
    f4_rows = stats["form4_transactions_rows"]
    emit("")
    emit(
        f"[SUCCESS] form4_insider_trades.db synced from PA, "
        f"{sent_rows:,} sent_alerts / {f4_rows:,} form4_transactions, "
        f"{size_mb:.2f} MB, took {elapsed:.0f}s"
    )
    emit("RESULT: SUCCESS")
    flush_log()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        emit("INTERRUPT: KeyboardInterrupt -- leaving local DB untouched")
        cleanup_tmp()
        flush_log()
        sys.exit(1)
