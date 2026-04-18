"""
sync_signal_intelligence.py

Pulls PA's signal_intelligence.db, merges new rows into Mac Studio's consolidated
signal_log (deduped on scan_date+scanner+ticker+direction+fired), archives a
point-in-time snapshot, and reports deltas plus scanner health.

Read-only on PA. All writes land on Mac Studio. Data mover only -- no analysis.

Exit codes:
    0  download ok, schema ok, merge committed, archive copied
    1  any hard failure (download, corrupt SQLite, schema drift, dedup violation,
       transaction rollback, archive failure)
"""

from __future__ import annotations

import logging
import shutil
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
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

MAC_DB = Path("/Users/kevinheaney/gmc_data/signal_intelligence.db")
ARCHIVE_DIR = Path("/Users/kevinheaney/gmc_data/archives/signal_intelligence")
PA_URL = (
    f"https://www.pythonanywhere.com/api/v0/user/{config.PA_USERNAME}"
    f"/files/path/home/{config.PA_USERNAME}/signal_intelligence.db"
)

# Live-signal cutoff for health-surface counts -- per briefing
LIVE_CUTOFF = "2026-04-07"

SQLITE_MAGIC = b"SQLite format 3\x00"

# PA's signal_log schema (13 columns) -- must match this exactly
EXPECTED_PA_COLS = [
    ("id", "INTEGER"),
    ("scan_date", "TEXT"),
    ("scanner", "TEXT"),
    ("ticker", "TEXT"),
    ("direction", "TEXT"),
    ("fired", "INTEGER"),
    ("signal_strength", "REAL"),
    ("signal_bucket", "TEXT"),
    ("regime_filter_passed", "INTEGER"),
    ("regime_value", "REAL"),
    ("score", "INTEGER"),
    ("autotrader_acted", "INTEGER"),
    ("created_at", "TEXT"),
]

# Mac-only columns (outcome/backfill fields) -- populated by separate process
MAC_ONLY_COLS = [
    ("entry_date", "TEXT"),
    ("exit_date", "TEXT"),
    ("ret_pct", "REAL"),
    ("alpha_vs_spy", "REAL"),
]

# Full expected Mac schema = PA schema + Mac-only outcome cols
EXPECTED_MAC_COLS = [
    ("id", "INTEGER"),
    ("scan_date", "TEXT"),
    ("scanner", "TEXT"),
    ("ticker", "TEXT"),
    ("direction", "TEXT"),
    ("fired", "INTEGER"),
    ("signal_strength", "REAL"),
    ("signal_bucket", "TEXT"),
    ("regime_filter_passed", "INTEGER"),
    ("regime_value", "REAL"),
    ("score", "INTEGER"),
    ("autotrader_acted", "INTEGER"),
    ("entry_date", "TEXT"),
    ("exit_date", "TEXT"),
    ("ret_pct", "REAL"),
    ("alpha_vs_spy", "REAL"),
    ("created_at", "TEXT"),
]

# Columns actually copied from PA into Mac (id excluded -- AUTOINCREMENT assigns)
INSERT_COLS = [
    "scan_date", "scanner", "ticker", "direction", "fired",
    "signal_strength", "signal_bucket", "regime_filter_passed", "regime_value",
    "score", "autotrader_acted", "created_at",
]

# Scanners we expect to see firing live. From Master_status PA Scheduled Tasks.
EXPECTED_SCANNERS = {
    "8K_1.01", "PEAD_BULL", "PEAD_BEAR", "SI_SQUEEZE",
    "COT_BULL", "COT_BEAR", "CEL_BEAR", "THIRTEENF_BULL",
    "DIV_CUT", "DIV_INITIATION",
    "F4_BUY_CLUSTER", "F4_SELL_S1", "F4_SELL_S2",
}


# --------------------------------------------------------------------------- #
# Logging: stdout + timestamped /tmp log file                                 #
# --------------------------------------------------------------------------- #

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH = Path(f"/tmp/sync_signal_intelligence_{RUN_TS}.log")

_logger = logging.getLogger("sync_signal_intelligence")
_logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(message)s")
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
_fh = logging.FileHandler(LOG_PATH)
_fh.setFormatter(_fmt)
_logger.addHandler(_sh)
_logger.addHandler(_fh)


def emit(msg: str = "") -> None:
    _logger.info(msg)


def die(msg: str) -> None:
    _logger.error(f"FAIL: {msg}")
    _logger.info("RESULT: FAILURE")
    sys.exit(1)


# --------------------------------------------------------------------------- #
# Step 1: download                                                            #
# --------------------------------------------------------------------------- #

def download_pa_db() -> Path:
    iso = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    tmp = Path(f"/tmp/pa_signal_intelligence_{iso}.db")

    try:
        resp = requests.get(
            PA_URL,
            headers={"Authorization": f"Token {config.PA_API_TOKEN}"},
            stream=True,
            timeout=30,
        )
    except requests.RequestException as e:
        die(f"PA download request failed: {e}")

    if resp.status_code != 200:
        die(f"PA download HTTP {resp.status_code}: {resp.text[:200]}")

    with open(tmp, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)

    size = tmp.stat().st_size
    if size <= 1024:
        die(f"PA download too small: {size} bytes (<=1024)")

    with open(tmp, "rb") as fh:
        head = fh.read(16)
    if head != SQLITE_MAGIC:
        die(f"PA download missing SQLite magic (got {head!r})")

    emit("DOWNLOAD")
    emit(f"  URL size:          {size/1024:.2f} KB")
    emit("  SQLite signature:  valid")
    emit(f"  Temp path:         {tmp}")
    emit()
    return tmp


# --------------------------------------------------------------------------- #
# Step 2: schema verification                                                 #
# --------------------------------------------------------------------------- #

def _cols(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    return [(r[1], r[2].upper()) for r in conn.execute("PRAGMA table_info(signal_log)")]


def verify_schema(pa_db: Path) -> None:
    with closing(sqlite3.connect(pa_db)) as pa_conn, \
         closing(sqlite3.connect(MAC_DB)) as mac_conn:
        pa_cols = _cols(pa_conn)
        mac_cols = _cols(mac_conn)

    expected_pa = [(n, t.upper()) for n, t in EXPECTED_PA_COLS]
    expected_mac = [(n, t.upper()) for n, t in EXPECTED_MAC_COLS]

    emit("SCHEMA CHECK")
    emit(f"  PA columns:        {len(pa_cols)}")
    emit(f"  Mac columns:       {len(mac_cols)}")

    pa_match = pa_cols == expected_pa
    mac_match = sorted(mac_cols) == sorted(expected_mac)

    if not pa_match:
        emit("  Match:             NO")
        emit("  PA schema diff:")
        for i, (got, exp) in enumerate(zip(pa_cols, expected_pa)):
            if got != exp:
                emit(f"    idx {i}: got={got} expected={exp}")
        if len(pa_cols) != len(expected_pa):
            emit(f"    length mismatch: got {len(pa_cols)}, expected {len(expected_pa)}")
        die("PA schema does not match expected shape")

    if not mac_match:
        emit("  Match:             NO")
        emit("  Mac schema diff (unordered set compare):")
        got_set, exp_set = set(mac_cols), set(expected_mac)
        for extra in got_set - exp_set:
            emit(f"    unexpected on Mac: {extra}")
        for missing in exp_set - got_set:
            emit(f"    missing from Mac: {missing}")
        die("Mac schema does not match expected shape")

    emit("  Match:             YES")
    emit()


# --------------------------------------------------------------------------- #
# Step 3: ensure dedup index                                                  #
# --------------------------------------------------------------------------- #

def ensure_dedup_index() -> None:
    with closing(sqlite3.connect(MAC_DB)) as conn:
        existed = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_signal_log_dedup'"
        ).fetchone()

        # Pre-check for existing duplicates under the new key -- otherwise
        # CREATE UNIQUE INDEX on a table with dupes is a silent data issue.
        dupes = conn.execute("""
            SELECT scan_date, scanner, ticker, direction, fired, COUNT(*) AS n
            FROM signal_log
            GROUP BY scan_date, scanner, ticker, direction, fired
            HAVING n > 1
        """).fetchall()
        if dupes:
            emit("DEDUP INDEX")
            emit("  Pre-create duplicate check: FAIL")
            for d in dupes[:20]:
                emit(f"    {d}")
            if len(dupes) > 20:
                emit(f"    ... {len(dupes) - 20} more")
            die("Existing rows violate new uniqueness key -- aborting before index creation")

        try:
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_log_dedup
                  ON signal_log(scan_date, scanner, ticker, direction, fired)
            """)
        except sqlite3.IntegrityError as e:
            die(f"Dedup index creation failed: {e}")

    emit("DEDUP INDEX")
    emit(f"  idx_signal_log_dedup: {'already existed' if existed else 'created this run'}")
    emit()


# --------------------------------------------------------------------------- #
# Step 4 / 6: snapshots                                                       #
# --------------------------------------------------------------------------- #

def snapshot(db_path: Path) -> dict:
    with closing(sqlite3.connect(db_path)) as conn:
        total = conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
        fires_since = conn.execute(
            "SELECT COUNT(*) FROM signal_log WHERE fired=1 AND scan_date >= ?",
            (LIVE_CUTOFF,),
        ).fetchone()[0]
        by_scanner = conn.execute(
            """
            SELECT scanner,
                   COUNT(*),
                   SUM(CASE WHEN fired=1 AND scan_date >= ? THEN 1 ELSE 0 END),
                   MIN(scan_date),
                   MAX(scan_date)
            FROM signal_log
            GROUP BY scanner
            ORDER BY scanner
            """,
            (LIVE_CUTOFF,),
        ).fetchall()
    return {
        "total": total,
        "fires_since": fires_since,
        "by_scanner": by_scanner,
        "scanners": {r[0] for r in by_scanner},
    }


def print_snapshot(label: str, snap: dict) -> None:
    emit(label)
    emit(f"  Total rows:        {snap['total']:,}")
    emit(f"  Fires >= {LIVE_CUTOFF}:  {snap['fires_since']}")
    emit("  By scanner (total / fires since cutoff / date range):")
    for scanner, total, fires, mn, mx in snap["by_scanner"]:
        fires = fires or 0
        emit(f"    {scanner:<20} {total:>7,} / {fires:>3}   {mn} .. {mx}")
    emit()


# --------------------------------------------------------------------------- #
# Step 5: merge                                                               #
# --------------------------------------------------------------------------- #

def merge(pa_db: Path) -> tuple[int, int]:
    with closing(sqlite3.connect(pa_db)) as pa_conn:
        attempted = pa_conn.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]

    with closing(sqlite3.connect(MAC_DB)) as conn:
        conn.isolation_level = None
        c = conn.cursor()
        # ATTACH must occur outside a transaction (SQLite constraint)
        c.execute(f"ATTACH DATABASE '{pa_db}' AS pa")
        try:
            c.execute("BEGIN IMMEDIATE")
            pre_total = c.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
            col_list = ", ".join(INSERT_COLS)
            c.execute(f"""
                INSERT OR IGNORE INTO signal_log ({col_list})
                SELECT {col_list} FROM pa.signal_log
            """)
            post_total = c.execute("SELECT COUNT(*) FROM signal_log").fetchone()[0]
            inserted = post_total - pre_total
            c.execute("COMMIT")
        except Exception as e:
            c.execute("ROLLBACK")
            die(f"Merge transaction rolled back: {e}")
        finally:
            try:
                c.execute("DETACH DATABASE pa")
            except Exception:
                pass

    skipped = attempted - inserted
    emit("MERGE")
    emit(f"  Rows attempted:    {attempted}")
    emit(f"  Rows inserted:     {inserted}")
    emit(f"  Rows skipped:      {skipped}  (dedup OR IGNORE)")
    emit("  Transaction:       committed")
    emit()
    return attempted, inserted


# --------------------------------------------------------------------------- #
# Step 7: archive                                                             #
# --------------------------------------------------------------------------- #

def archive_snapshot(pa_db: Path) -> Path:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    target = ARCHIVE_DIR / f"pa_{datetime.now().strftime('%Y-%m-%d')}.db"
    try:
        shutil.copy2(pa_db, target)
    except Exception as e:
        die(f"Archive copy failed: {e}")

    if not target.exists() or target.stat().st_size <= 1024:
        die(f"Archive post-copy verification failed: {target}")

    emit("ARCHIVE")
    emit(f"  Snapshot:          {target}")
    emit(f"  Size:              {target.stat().st_size/1024:.2f} KB")
    emit(f"  Archive dir holds: {len(list(ARCHIVE_DIR.glob('pa_*.db')))} snapshot(s)")
    emit()
    return target


# --------------------------------------------------------------------------- #
# Step 8: scanner health                                                      #
# --------------------------------------------------------------------------- #

def scanner_health(pre: dict, post: dict) -> None:
    # Scanner fires from Mac Studio post-merge (authoritative consolidated view)
    fires_by_scanner = {
        row[0]: (row[2] or 0) for row in post["by_scanner"]
    }
    seen_scanners = set(fires_by_scanner)

    with_fires = sorted(s for s, n in fires_by_scanner.items() if n > 0)
    zero_fire_expected = sorted(EXPECTED_SCANNERS - {s for s, n in fires_by_scanner.items() if n > 0})
    unexpected = sorted(seen_scanners - EXPECTED_SCANNERS)

    emit(f"SCANNER HEALTH (since {LIVE_CUTOFF}, post-merge)")
    emit(f"  Scanners with fires:       {', '.join(with_fires) if with_fires else '(none)'}")
    emit(f"  Scanners with zero fires:  {', '.join(zero_fire_expected) if zero_fire_expected else '(none)'}")
    emit(f"  Unexpected scanner names:  {', '.join(unexpected) if unexpected else '(none)'}")
    emit()


# --------------------------------------------------------------------------- #
# Main                                                                        #
# --------------------------------------------------------------------------- #

def main() -> int:
    emit("PA SIGNAL INTELLIGENCE SYNC")
    emit(f"Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    emit("=" * 45)
    emit()

    pa_db = download_pa_db()
    verify_schema(pa_db)
    ensure_dedup_index()

    pre = snapshot(MAC_DB)
    pa_snap = snapshot(pa_db)
    print_snapshot("PRE-MERGE  (Mac Studio)", pre)
    print_snapshot("PRE-MERGE  (PA)", pa_snap)

    merge(pa_db)

    post = snapshot(MAC_DB)
    post_header = (
        f"POST-MERGE (Mac Studio)   "
        f"total +{post['total'] - pre['total']}   "
        f"fires-since-cutoff +{post['fires_since'] - pre['fires_since']}"
    )
    print_snapshot(post_header, post)

    new_scanners = post["scanners"] - pre["scanners"]
    if new_scanners:
        emit(f"NEW SCANNERS (first appearance this merge): {', '.join(sorted(new_scanners))}")
        emit()

    archive_snapshot(pa_db)
    scanner_health(pre, post)

    emit("RESULT: SUCCESS")
    emit(f"Log: {LOG_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
