"""Change detection — content fingerprints and an append-only change log.

WHY THIS EXISTS
---------------
Refreshing the dataset by re-scraping it is unaffordable. A Study Abroad
programme costs ~109 KB from its own page but ~2.5 KB via the listing API — 43x
cheaper. So the refresh strategy is: sweep the cheap listings, fingerprint what
comes back, and only spend a detail request on records whose fingerprint moved.
This module is that fingerprint, plus the record of what actually changed.

It is deliberately generic: it takes a db_path and a table name and works the
same for the domestic, Study Abroad and Course Finder databases. Nothing here
imports db/sa_db/cf_db, so it can't couple the three together.

WHAT IT ADDS TO A TABLE
-----------------------
  content_hash     fingerprint of the meaningful columns
  first_seen_at    when this row was first recorded
  last_seen_at     when a sweep last confirmed it exists (even if unchanged)
  last_changed_at  when its fingerprint last moved
  inactive_since   set when a sweep no longer finds it. NEVER deleted.

`last_seen_at` and `inactive_since` are the pair that answers "is this still
real" — a question the scraper currently cannot answer at all, because a college
that disappears from Collegedunia simply stays in the database forever.

NON-DESTRUCTIVE
---------------
Nothing here deletes a row or overwrites scraped values. It adds columns, writes
an append-only log, and sets `inactive_since` on rows that vanish. That matches
the standing rule that no data is ever deleted.
"""
from __future__ import annotations

BUILD = "2026-09-17a"

import hashlib
import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Columns appended to every tracked table.
FRESHNESS_COLS: Tuple[Tuple[str, str], ...] = (
    ("content_hash", "TEXT"),
    ("first_seen_at", "REAL"),
    ("last_seen_at", "REAL"),
    ("last_changed_at", "REAL"),
    ("inactive_since", "REAL"),
)
FRESHNESS_NAMES = {c for c, _ in FRESHNESS_COLS}

# Columns that must NOT influence the fingerprint. Two kinds: bookkeeping that
# changes on every write (so including it would make every row look changed),
# and giant blobs that are derivable from the fields we already hash.
EXCLUDED_FROM_HASH = FRESHNESS_NAMES | {
    "id",                      # autoincrement surrogate, not identity
    "scraped_at", "updated_at", "enriched_at",
    "detail_scraped_at", "program_detail_scraped_at",
    "source_job_id", "job_id",
    "raw_json", "detail_json",
}

# A changed value is logged, but descriptions run to thousands of characters and
# the log would dwarf the data. Store enough to see WHAT changed.
MAX_LOGGED_VALUE = 400

CHANGE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS data_changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name  TEXT NOT NULL,
    pk          TEXT NOT NULL,
    field       TEXT NOT NULL,     -- '*' for a row appearing or vanishing
    old_value   TEXT,
    new_value   TEXT,
    change_type TEXT NOT NULL,     -- 'new' | 'changed' | 'gone' | 'returned'
    changed_at  REAL NOT NULL,
    job_id      INTEGER
);
CREATE INDEX IF NOT EXISTS idx_changes_table ON data_changes(table_name, changed_at);
CREATE INDEX IF NOT EXISTS idx_changes_pk    ON data_changes(table_name, pk);
CREATE INDEX IF NOT EXISTS idx_changes_when  ON data_changes(changed_at);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def ensure_schema(conn: sqlite3.Connection, table: str) -> List[str]:
    """Add the freshness columns and the change log. Returns columns added."""
    conn.executescript(CHANGE_LOG_DDL)
    have = set(table_columns(conn, table))
    if not have:
        raise ValueError(f"no such table: {table}")
    added = []
    for col, typ in FRESHNESS_COLS:
        if col not in have:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
            added.append(col)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_hash "
                 f"ON {table}(content_hash)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_seen "
                 f"ON {table}(last_seen_at)")
    return added


def tracked_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """The columns that make up the fingerprint, in a stable order."""
    return sorted(c for c in table_columns(conn, table)
                  if c not in EXCLUDED_FROM_HASH)


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------
def _norm(v: Any) -> str:
    """Canonical text for one value.

    None and '' must hash identically: SQLite stores 'missing' both ways
    depending on which parser wrote the row, and a NULL becoming '' is not a
    change anyone cares about. Floats are formatted with %r so 1.0 and 1 do not
    read as different.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        return repr(int(v)) if v.is_integer() else repr(v)
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, ensure_ascii=False)
    return str(v).strip()


def row_hash(row: Any, cols: Sequence[str]) -> str:
    """Stable fingerprint over `cols`. Column names are included so that adding
    a column changes the hash (a new field IS new information), while reordering
    the SELECT does not."""
    h = hashlib.sha1()
    for c in cols:
        try:
            v = row[c]
        except (KeyError, IndexError):
            v = None
        h.update(c.encode("utf-8"))
        h.update(b"\x1f")
        h.update(_norm(v).encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()


def _pk_of(row: Any, pk_cols: Sequence[str]) -> str:
    return "|".join(_norm(row[c]) for c in pk_cols)


def _clip(v: Any) -> Optional[str]:
    s = _norm(v)
    if s == "":
        return None
    return s if len(s) <= MAX_LOGGED_VALUE else s[:MAX_LOGGED_VALUE] + "…"


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------
def diff_row(old: Any, new: Any, cols: Sequence[str]) -> List[Tuple[str, Any, Any]]:
    """Fields that actually differ, old value first."""
    out = []
    for c in cols:
        try:
            a = old[c]
        except (KeyError, IndexError, TypeError):
            a = None
        try:
            b = new[c]
        except (KeyError, IndexError, TypeError):
            b = None
        if _norm(a) != _norm(b):
            out.append((c, a, b))
    return out


def log_change(conn: sqlite3.Connection, table: str, pk: str, field: str,
               old: Any, new: Any, change_type: str,
               job_id: Optional[int] = None, ts: Optional[float] = None) -> None:
    conn.execute(
        "INSERT INTO data_changes(table_name,pk,field,old_value,new_value,"
        "change_type,changed_at,job_id) VALUES(?,?,?,?,?,?,?,?)",
        (table, pk, field, _clip(old), _clip(new), change_type,
         ts if ts is not None else time.time(), job_id))


def observe(conn: sqlite3.Connection, table: str, pk_cols: Sequence[str],
            new_row: Dict[str, Any], job_id: Optional[int] = None,
            cols: Optional[Sequence[str]] = None,
            ts: Optional[float] = None) -> str:
    """Compare one freshly-scraped row against what is stored.

    Returns 'new', 'changed', 'same' or 'returned'. Does NOT write the row's
    data — the vertical's own upsert owns that. This only maintains the
    freshness columns and the change log, so it can sit alongside any parser
    without knowing anything about it.
    """
    cols = list(cols) if cols else tracked_columns(conn, table)
    now = ts if ts is not None else time.time()
    pk = _pk_of(new_row, pk_cols)
    where = " AND ".join(f"{c}=?" for c in pk_cols)
    args = [new_row[c] for c in pk_cols]
    old = conn.execute(f"SELECT * FROM {table} WHERE {where}", args).fetchone()
    h = row_hash(new_row, cols)

    if old is None:
        log_change(conn, table, pk, "*", None, None, "new", job_id, now)
        return "new"

    was_gone = old["inactive_since"] if "inactive_since" in old.keys() else None
    if old["content_hash"] == h:
        conn.execute(f"UPDATE {table} SET last_seen_at=?, inactive_since=NULL "
                     f"WHERE {where}", [now] + args)
        if was_gone:
            log_change(conn, table, pk, "*", None, None, "returned", job_id, now)
            return "returned"
        return "same"

    for field, a, b in diff_row(old, new_row, cols):
        log_change(conn, table, pk, field, a, b, "changed", job_id, now)
    conn.execute(
        f"UPDATE {table} SET content_hash=?, last_seen_at=?, last_changed_at=?, "
        f"inactive_since=NULL WHERE {where}", [h, now, now] + args)
    return "returned" if was_gone else "changed"


def stamp_new(conn: sqlite3.Connection, table: str, pk_cols: Sequence[str],
              row: Dict[str, Any], cols: Optional[Sequence[str]] = None,
              ts: Optional[float] = None) -> None:
    """Set the freshness columns on a row the vertical has just inserted."""
    cols = list(cols) if cols else tracked_columns(conn, table)
    now = ts if ts is not None else time.time()
    where = " AND ".join(f"{c}=?" for c in pk_cols)
    args = [row[c] for c in pk_cols]
    conn.execute(
        f"UPDATE {table} SET content_hash=?, "
        f"first_seen_at=COALESCE(first_seen_at,?), last_seen_at=?, "
        f"last_changed_at=COALESCE(last_changed_at,?) WHERE {where}",
        [row_hash(row, cols), now, now, now] + args)


def mark_missing(conn: sqlite3.Connection, table: str, pk_cols: Sequence[str],
                 sweep_started_at: float, job_id: Optional[int] = None,
                 scope_sql: str = "", scope_args: Sequence[Any] = ()) -> int:
    """Flag rows a completed sweep did not see. NEVER deletes them.

    Call only after a sweep that covered the whole scope — otherwise a run that
    stopped early would mark everything it hadn't reached as gone. `scope_sql`
    narrows it to the slice the sweep actually covered (e.g. one country).
    """
    where = ("(last_seen_at IS NULL OR last_seen_at < ?) "
             "AND inactive_since IS NULL")
    args: List[Any] = [sweep_started_at]
    if scope_sql:
        where += f" AND ({scope_sql})"
        args += list(scope_args)
    rows = conn.execute(
        f"SELECT {','.join(pk_cols)} FROM {table} WHERE {where}", args).fetchall()
    now = time.time()
    for r in rows:
        log_change(conn, table, _pk_of(r, pk_cols), "*", None, None,
                   "gone", job_id, now)
    conn.execute(f"UPDATE {table} SET inactive_since=? WHERE {where}",
                 [now] + args)
    return len(rows)


# ---------------------------------------------------------------------------
# Backfill — zero requests, runs against rows already stored
# ---------------------------------------------------------------------------
def backfill(db_path: str, table: str, pk_cols: Sequence[str],
             batch: int = 5000, apply: bool = True) -> Dict[str, int]:
    """Compute fingerprints for existing rows.

    Rows scraped before this module existed have no hash, so the first refresh
    sweep would see every one of them as changed and re-fetch the lot. Seeding
    the hashes now is what makes the first sweep cheap.

    `first_seen_at` is seeded from whatever timestamp the row already carries
    rather than 'now', so the history is not retroactively flattened to today.
    """
    conn = connect(db_path)
    try:
        ensure_schema(conn, table)
        cols = tracked_columns(conn, table)
        have = set(table_columns(conn, table))
        # best available "when was this first recorded"
        seed = next((c for c in ("scraped_at", "created_at", "updated_at")
                     if c in have), None)
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        todo = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE content_hash IS NULL").fetchone()[0]
        if not apply:
            return {"rows": total, "needing_hash": todo, "hashed": 0,
                    "columns_in_hash": len(cols)}

        done = 0
        while True:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE content_hash IS NULL LIMIT {batch}"
            ).fetchall()
            if not rows:
                break
            where = " AND ".join(f"{c}=?" for c in pk_cols)
            for r in rows:
                ts = (r[seed] if seed and r[seed] else None) or time.time()
                conn.execute(
                    f"UPDATE {table} SET content_hash=?, "
                    f"first_seen_at=COALESCE(first_seen_at,?), "
                    f"last_seen_at=COALESCE(last_seen_at,?), "
                    f"last_changed_at=COALESCE(last_changed_at,?) "
                    f"WHERE {where}",
                    [row_hash(r, cols), ts, ts, ts] + [r[c] for c in pk_cols])
            conn.commit()
            done += len(rows)
            if len(rows) < batch:
                break
        return {"rows": total, "needing_hash": todo, "hashed": done,
                "columns_in_hash": len(cols)}
    finally:
        conn.commit()
        conn.close()


# ---------------------------------------------------------------------------
# Reading the log
# ---------------------------------------------------------------------------
def recent_changes(db_path: str, since: Optional[float] = None,
                   table: Optional[str] = None, change_type: Optional[str] = None,
                   limit: int = 200) -> List[Dict[str, Any]]:
    conn = connect(db_path)
    try:
        conn.executescript(CHANGE_LOG_DDL)
        where, args = ["1=1"], []
        if since:
            where.append("changed_at >= ?"); args.append(since)
        if table:
            where.append("table_name = ?"); args.append(table)
        if change_type:
            where.append("change_type = ?"); args.append(change_type)
        rows = conn.execute(
            "SELECT * FROM data_changes WHERE " + " AND ".join(where) +
            f" ORDER BY changed_at DESC, id DESC LIMIT {int(limit)}", args)
        return [dict(r) for r in rows]
    finally:
        conn.close()


def change_summary(db_path: str, since: Optional[float] = None) -> Dict[str, Any]:
    conn = connect(db_path)
    try:
        conn.executescript(CHANGE_LOG_DDL)
        w, a = ("WHERE changed_at >= ?", [since]) if since else ("", [])
        by_type = {r[0]: r[1] for r in conn.execute(
            f"SELECT change_type, COUNT(*) FROM data_changes {w} "
            f"GROUP BY change_type", a)}
        by_table = {r[0]: r[1] for r in conn.execute(
            f"SELECT table_name, COUNT(*) FROM data_changes {w} "
            f"GROUP BY table_name ORDER BY 2 DESC", a)}
        top_fields = [(r[0], r[1]) for r in conn.execute(
            f"SELECT field, COUNT(*) FROM data_changes {w} "
            f"GROUP BY field ORDER BY 2 DESC LIMIT 15", a)]
        return {"by_type": by_type, "by_table": by_table,
                "top_fields": top_fields,
                "total": sum(by_type.values())}
    finally:
        conn.close()
