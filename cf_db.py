"""
Course Finder — data layer. SELF-CONTAINED and isolated from every other module.

Own `cf_`-prefixed tables, own jobs/logs/progress. It does not read or write a
single domestic (`courses`, `colleges`, `offerings`) or Study Abroad (`sa_`)
table, and nothing else reads its tables. Deleting every cf_ table would leave
the rest of the system untouched.

Reuses ONLY generic infrastructure from `db`: the WAL connection helper and the
credential redaction used when persisting a job config.

Two tables:
  cf_courses    one row per course in collegedunia.com/course-finder (~21,689),
                including `colleges_count` — how many colleges offer it, which
                the listing gives away for free and which lets Phase B be costed
                and prioritised BEFORE spending a request.
  cf_offerings  one row per (course_id, college_id) from
                /course-finder?course_id=<id>, with fees, ranking, cutoff,
                admission dates and rating.
"""
from __future__ import annotations

BUILD = "2026-08-29a"

import json
import os
import time
from typing import Any, Dict, List, Optional

import db as _core  # generic WAL connection + redact_secrets only


def _redact(config):
    try:
        return _core.redact_secrets(config)
    except Exception:  # noqa: BLE001
        return config


# Shares the same SQLite file by default (one disk, one backup), but only ever
# touches cf_-prefixed tables. Override to split it into its own file entirely.
CF_DB_PATH = os.environ.get("CD_CF_DB_PATH") or _core.DB_PATH
PROMOTE_CHUNK = int(os.environ.get("CD_PROMOTE_CHUNK", "2500"))


def connect(db_path: str = CF_DB_PATH):
    return _core.connect(db_path)


SCHEMA = """
-- The course catalogue as the course-finder lists it.
CREATE TABLE IF NOT EXISTS cf_courses (
    course_id       INTEGER PRIMARY KEY,
    name            TEXT,
    course_link     TEXT,          -- /courses/<slug>
    listing_link    TEXT,          -- /course-finder?course_id=<id>  (Phase B entry point)
    description     TEXT,
    eligibility     TEXT,
    duration        TEXT,
    level           TEXT,
    course_type     TEXT,
    course_could_be TEXT,
    degree_could_be TEXT,
    fees            TEXT,
    avg_salary      TEXT,
    exam_name       TEXT,
    exam_url        TEXT,
    job_roles       TEXT,
    topics_covered  TEXT,
    stream_id       TEXT,
    course_tag      TEXT,
    course_tag_id   TEXT,
    colleges_count  INTEGER,       -- colleges_data.count — free in the listing
    colleges_link   TEXT,
    raw_json        TEXT, scraped_at REAL, source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS cf_idx_courses_tag    ON cf_courses(course_tag_id);
CREATE INDEX IF NOT EXISTS cf_idx_courses_stream ON cf_courses(stream_id);
CREATE INDEX IF NOT EXISTS cf_idx_courses_count  ON cf_courses(colleges_count DESC);

-- One row per college offering a course.
CREATE TABLE IF NOT EXISTS cf_offerings (
    course_id        INTEGER,
    college_id       INTEGER,
    course_name      TEXT,
    college_name     TEXT,
    college_short    TEXT,
    city             TEXT,
    state_id         INTEGER,
    college_link     TEXT,
    logo             TEXT,
    offering_link    TEXT,
    fees_amount      INTEGER,
    fees_text        TEXT,
    eligibility      TEXT,
    duration         TEXT,
    level            TEXT,
    course_type      TEXT,
    course_could_be  TEXT,
    degree_could_be  TEXT,
    exam_name        TEXT,
    exam_url         TEXT,
    ranking_agency   TEXT,
    ranking_rank     INTEGER,
    ranking_total    INTEGER,
    ranking_stream   TEXT,
    ranking_url      TEXT,
    cutoff_exam      TEXT,
    cutoff_value     REAL,
    admission_start  TEXT,
    admission_end    TEXT,
    course_rating    REAL,
    reviews_count    INTEGER,
    avg_salary       TEXT,
    job_roles        TEXT,
    major_stream_rating REAL,
    stream_id        TEXT,
    course_tag       TEXT,
    course_tag_id    TEXT,
    raw_json         TEXT, scraped_at REAL, source_job_id INTEGER,
    PRIMARY KEY (course_id, college_id)
);
CREATE INDEX IF NOT EXISTS cf_idx_off_college ON cf_offerings(college_id);
CREATE INDEX IF NOT EXISTS cf_idx_off_course  ON cf_offerings(course_id);

-- Phase A partition progress (listing sliced by course_tag_id / stream).
CREATE TABLE IF NOT EXISTS cf_partition_progress (
    partition_key TEXT PRIMARY KEY,
    status        TEXT,            -- 'partial' | 'done'
    last_page     INTEGER DEFAULT 0,
    found         INTEGER DEFAULT 0,
    updated_at    REAL
);

-- Phase B per-course progress. This IS the resume queue: a course drops out of
-- courses_pending() the moment it is marked done.
CREATE TABLE IF NOT EXISTS cf_course_progress (
    course_id   INTEGER PRIMARY KEY,
    status      TEXT,              -- 'done' | 'partial' | 'empty' | 'error'
    last_page   INTEGER DEFAULT 0,
    found       INTEGER DEFAULT 0,
    expected    INTEGER DEFAULT 0,
    updated_at  REAL
);
CREATE INDEX IF NOT EXISTS cf_idx_cprog_status ON cf_course_progress(status);

CREATE TABLE IF NOT EXISTS cf_jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    vertical      TEXT,
    phase         TEXT,
    status        TEXT,
    config_json   TEXT,
    total_units   INTEGER DEFAULT 0,
    done_units    INTEGER DEFAULT 0,
    items_written INTEGER DEFAULT 0,
    req_count     INTEGER DEFAULT 0,
    bytes_count   INTEGER DEFAULT 0,
    message       TEXT,
    stop_requested INTEGER DEFAULT 0,
    pid           INTEGER,
    started_at    REAL, updated_at REAL, finished_at REAL
);
CREATE TABLE IF NOT EXISTS cf_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, ts REAL, message TEXT
);
CREATE INDEX IF NOT EXISTS cf_idx_logs_job ON cf_logs(job_id, id);

CREATE TABLE IF NOT EXISTS cf_settings (key TEXT PRIMARY KEY, value TEXT);
"""


def init_db(db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)


# ---------------------------------------------------------------------------
# Writes — all upserts are non-destructive by default: a blank incoming value
# never overwrites something already stored.
# ---------------------------------------------------------------------------
COURSE_COLS = ["course_id", "name", "course_link", "listing_link", "description",
               "eligibility", "duration", "level", "course_type", "course_could_be",
               "degree_could_be", "fees", "avg_salary", "exam_name", "exam_url",
               "job_roles", "topics_covered", "stream_id", "course_tag",
               "course_tag_id", "colleges_count", "colleges_link", "raw_json",
               "scraped_at", "source_job_id"]

OFFERING_COLS = ["course_id", "college_id", "course_name", "college_name",
                 "college_short", "city", "state_id", "college_link", "logo",
                 "offering_link", "fees_amount", "fees_text", "eligibility",
                 "duration", "level", "course_type", "course_could_be",
                 "degree_could_be", "exam_name", "exam_url", "ranking_agency",
                 "ranking_rank", "ranking_total", "ranking_stream", "ranking_url",
                 "cutoff_exam", "cutoff_value", "admission_start", "admission_end",
                 "course_rating", "reviews_count", "avg_salary", "job_roles",
                 "major_stream_rating", "stream_id", "course_tag", "course_tag_id",
                 "raw_json", "scraped_at", "source_job_id"]


def _upsert(conn, table: str, cols: List[str], key_cols: List[str],
            rows: List[Dict[str, Any]], preserve_nonempty: bool = True) -> int:
    """Non-destructive by default — an incoming NULL/'' never replaces a stored
    value. Phase A and Phase B both write course-level fields, so without this
    the second pass would blank what the first captured."""
    if not rows:
        return 0
    ph = ",".join("?" for _ in cols)
    if preserve_nonempty:
        setc = ",".join(
            f"{c}=CASE WHEN excluded.{c} IS NULL OR CAST(excluded.{c} AS TEXT)='' "
            f"THEN {table}.{c} ELSE excluded.{c} END"
            for c in cols if c not in key_cols)
    else:
        setc = ",".join(f"{c}=excluded.{c}" for c in cols if c not in key_cols)
    conn.executemany(
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
        f"ON CONFLICT({','.join(key_cols)}) DO UPDATE SET {setc}",
        [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def upsert_courses(rows, db_path: str = CF_DB_PATH) -> int:
    rows = [r for r in rows if r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "cf_courses", COURSE_COLS, ["course_id"], rows)


def upsert_offerings(rows, db_path: str = CF_DB_PATH) -> int:
    rows = [r for r in rows
            if r.get("course_id") is not None and r.get("college_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "cf_offerings", OFFERING_COLS,
                       ["course_id", "college_id"], rows)


# ---------------------------------------------------------------------------
# Progress / resume
# ---------------------------------------------------------------------------
def set_partition(key: str, status: str, last_page: int, found: int,
                  db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cf_partition_progress(partition_key,status,last_page,found,updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(partition_key) DO UPDATE SET "
            "status=excluded.status,last_page=excluded.last_page,"
            "found=excluded.found,updated_at=excluded.updated_at",
            (key, status, int(last_page), int(found), time.time()))


def done_partitions(db_path: str = CF_DB_PATH) -> set:
    with connect(db_path) as conn:
        return {r[0] for r in conn.execute(
            "SELECT partition_key FROM cf_partition_progress WHERE status='done'")}


def set_course_progress(course_id: int, status: str, last_page: int, found: int,
                        expected: int = 0, db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cf_course_progress(course_id,status,last_page,found,expected,updated_at) "
            "VALUES(?,?,?,?,?,?) ON CONFLICT(course_id) DO UPDATE SET "
            "status=excluded.status,last_page=excluded.last_page,found=excluded.found,"
            "expected=excluded.expected,updated_at=excluded.updated_at",
            (int(course_id), status, int(last_page), int(found), int(expected), time.time()))


def courses_pending(limit: int = 0, min_colleges: int = 0, order: str = "value",
                    db_path: str = CF_DB_PATH) -> List[Dict[str, Any]]:
    """Self-draining Phase-B queue: every course without a 'done'/'empty' progress
    row. `order='value'` returns the courses with the most colleges first, so a
    budget-limited run captures the most data per request."""
    order_sql = ("COALESCE(c.colleges_count,0) DESC" if order == "value"
                 else "c.course_id ASC")
    sql = ("SELECT c.course_id, c.name, c.colleges_count FROM cf_courses c "
           "LEFT JOIN cf_course_progress p ON p.course_id=c.course_id "
           "WHERE (p.course_id IS NULL OR p.status NOT IN ('done','empty')) "
           "AND COALESCE(c.colleges_count,0) >= ? "
           f"ORDER BY {order_sql}")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql, (int(min_colleges),))]


def phase_b_forecast(db_path: str = CF_DB_PATH) -> Dict[str, Any]:
    """Cost the whole of Phase B BEFORE running it, from colleges_count alone."""
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS courses, "
            "COALESCE(SUM(colleges_count),0) AS offerings, "
            "COALESCE(SUM(MAX(1,(colleges_count+9)/10)),0) AS pages "
            "FROM cf_courses").fetchone()
        done = conn.execute(
            "SELECT COUNT(*) FROM cf_course_progress WHERE status IN ('done','empty')"
        ).fetchone()[0]
        left = conn.execute(
            "SELECT COUNT(*) AS courses, COALESCE(SUM(c.colleges_count),0) AS offerings, "
            "COALESCE(SUM(MAX(1,(c.colleges_count+9)/10)),0) AS pages "
            "FROM cf_courses c LEFT JOIN cf_course_progress p ON p.course_id=c.course_id "
            "WHERE p.course_id IS NULL OR p.status NOT IN ('done','empty')").fetchone()
    return {"courses": row["courses"], "expected_offerings": row["offerings"],
            "expected_pages": row["pages"], "courses_done": done,
            "courses_left": left["courses"], "offerings_left": left["offerings"],
            "pages_left": left["pages"]}


def counts(db_path: str = CF_DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        def one(sql):
            try:
                return conn.execute(sql).fetchone()[0]
            except Exception:  # noqa: BLE001
                return 0
        return {
            "courses": one("SELECT COUNT(*) FROM cf_courses"),
            "courses_with_count": one("SELECT COUNT(*) FROM cf_courses "
                                      "WHERE COALESCE(colleges_count,0)>0"),
            "offerings": one("SELECT COUNT(*) FROM cf_offerings"),
            "distinct_colleges": one("SELECT COUNT(DISTINCT college_id) FROM cf_offerings"),
            "courses_scraped": one("SELECT COUNT(*) FROM cf_course_progress "
                                   "WHERE status IN ('done','empty')"),
            "partitions_done": one("SELECT COUNT(*) FROM cf_partition_progress "
                                   "WHERE status='done'"),
        }


# ---------------------------------------------------------------------------
# Jobs / logs (own bookkeeping — never touches the domestic `jobs` table)
# ---------------------------------------------------------------------------
def create_job(phase: str, config: Dict[str, Any], db_path: str = CF_DB_PATH) -> int:
    now = time.time()
    with connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO cf_jobs(vertical,phase,status,config_json,started_at,updated_at) "
            "VALUES('coursefinder',?,?,?,?,?)",
            (phase, "queued", json.dumps(_redact(config)), now, now))
        return cur.lastrowid


def update_job(job_id: int, db_path: str = CF_DB_PATH, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    sets = ",".join(f"{k}=?" for k in fields)
    with connect(db_path) as conn:
        conn.execute(f"UPDATE cf_jobs SET {sets} WHERE id=?",
                     (*fields.values(), job_id))


def get_job(job_id: int, db_path: str = CF_DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        r = conn.execute("SELECT * FROM cf_jobs WHERE id=?", (job_id,)).fetchone()
        return dict(r) if r else None


def list_jobs(limit: int = 50, db_path: str = CF_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM cf_jobs ORDER BY id DESC LIMIT ?", (int(limit),))]


def stop_requested(job_id: int, db_path: str = CF_DB_PATH) -> bool:
    with connect(db_path) as conn:
        r = conn.execute("SELECT stop_requested FROM cf_jobs WHERE id=?",
                         (job_id,)).fetchone()
        return bool(r and r[0])


def request_stop(job_id: int, db_path: str = CF_DB_PATH) -> None:
    update_job(job_id, stop_requested=1, db_path=db_path)


def add_log(job_id: int, message: str, db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO cf_logs(job_id,ts,message) VALUES(?,?,?)",
                     (job_id, time.time(), message))


def get_logs(job_id: int, limit: int = 400, db_path: str = CF_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM cf_logs WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, int(limit)))]


def prune_logs(keep: int = 8000, db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM cf_logs WHERE id NOT IN "
                     "(SELECT id FROM cf_logs ORDER BY id DESC LIMIT ?)", (int(keep),))


def get_setting(key: str, default: Any = None, db_path: str = CF_DB_PATH) -> Any:
    with connect(db_path) as conn:
        r = conn.execute("SELECT value FROM cf_settings WHERE key=?", (key,)).fetchone()
    if r is None:
        return default
    try:
        return json.loads(r["value"])
    except Exception:  # noqa: BLE001
        return r["value"]


def set_setting(key: str, value: Any, db_path: str = CF_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO cf_settings(key,value) VALUES(?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, json.dumps(value)))
