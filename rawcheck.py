"""READ-ONLY payload audit. Writes nothing, deletes nothing.

Answers the question the completeness report can't: when a column is 0% filled,
is the field MISSING FROM THE API, or is the parser looking under the wrong key?

Every parser stores the untouched source object in raw_json, so we can compare
what the API actually sends against what the parser reads. It also lists keys the
API sends that NOTHING currently maps — i.e. free fields available with no extra
requests.

    python rawcheck.py [db_path] [sample_size]
"""
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

# Source keys each parser reads (see scraper.parse_* / sa_scraper.sa_parse_program).
MAPPED = {
    "courses": {
        "name", "course_link", "link", "duration", "course_type", "level",
        "eligibility", "courses_could_be", "degree_could_be", "exam", "fees",
        "avg_salary", "colleges_data", "job_roles", "topics_covered",
        "description", "lead_params",
    },
    "offerings": {
        "name", "link", "college", "exam", "fees_data", "ranking_data", "cutoff",
        "admission", "course_rating", "reviews_count", "eligibility", "duration",
        "course_type", "level", "course_could_be", "degree_could_be", "job_roles",
        "topics_covered", "description", "lead_params",
    },
    "colleges_directory": {
        "college_id", "college_name", "college_short_form", "college_city",
        "city_id", "state", "state_id", "url", "rating", "naac_grading", "fees",
        "courseCount", "approvals",
    },
    "sa_programs": {
        "course_id", "head_one", "head_two", "course_tags", "program_type",
        "course_duration", "course_languages", "is_stem", "is_partner",
        "default_fee_per_year", "total_fee_per_year", "application_end_date",
        "lead_params", "college_name", "college_url", "course_link",
        "college_logo", "ranking", "course_description", "exams_data",
    },
}

# Columns the completeness report flagged as suspiciously empty -> the source key
# the parser reads for them. Reported explicitly.
SUSPECT = {
    "courses": {"fees": "fees", "avg_salary": "avg_salary",
                "topics_covered": "topics_covered"},
    "offerings": {"topics_covered": "topics_covered", "description": "description"},
    "colleges_directory": {"top_course_fees": "fees"},
    "sa_programs": {"application_end_date": "application_end_date"},
}

NESTED = {  # table -> (source key holding a list of dicts, label)
    "sa_programs": ("exams_data", "sa_program_exams"),
}


def _nonempty(v):
    if v is None:
        return False
    if isinstance(v, str):
        return v.strip() != ""
    if isinstance(v, (list, dict)):
        return len(v) > 0
    return True


def audit(conn, table, sample):
    try:
        rows = conn.execute(
            f"SELECT raw_json FROM {table} WHERE raw_json IS NOT NULL AND raw_json<>'' "
            f"LIMIT ?", (sample,)).fetchall()
    except Exception as e:
        print(f"\n{table}: cannot read ({e})")
        return
    objs = []
    for (rj,) in rows:
        try:
            o = json.loads(rj)
            if isinstance(o, dict):
                objs.append(o)
        except Exception:
            continue
    if not objs:
        print(f"\n{table}: no usable raw_json in the sample "
              f"({len(rows)} rows read) — nothing to compare against")
        return

    n = len(objs)
    present, filled = Counter(), Counter()
    for o in objs:
        for k, v in o.items():
            present[k] += 1
            if _nonempty(v):
                filled[k] += 1

    mapped = MAPPED.get(table, set())
    print(f"\n{'='*78}\n{table}  —  {n} raw payloads sampled\n{'='*78}")

    sus = SUSPECT.get(table, {})
    if sus:
        print("  SUSPECT COLUMNS — api payload vs stored column:")
        total_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] or 1
        for col, key in sus.items():
            p, f = present.get(key, 0), filled.get(key, 0)
            try:
                col_filled = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL "
                    f"AND CAST({col} AS TEXT)<>''").fetchone()[0]
            except Exception:
                col_filled = None
            col_pct = (100.0 * col_filled / total_rows) if col_filled is not None else None
            if p == 0:
                verdict = "KEY ABSENT FROM THE API — the field is gone; no parser change can recover it"
            elif f == 0:
                verdict = "key present but ALWAYS EMPTY — the API sends it blank"
            elif col_pct is not None and col_pct < 1.0:
                verdict = ("API SENDS IT, COLUMN IS EMPTY -> EXTRACTION BUG, "
                           "and it is recoverable from raw_json with no re-scrape")
            else:
                verdict = "populated in both — no problem here"
            colstr = "n/a" if col_pct is None else f"{col_pct:.1f}%"
            print(f"    {col:22} (reads '{key}')  api: present {p}/{n}, non-empty {f}/{n}"
                  f"   |  column filled: {colstr}")
            print(f"      -> {verdict}")

    unmapped = [(k, filled[k], present[k]) for k in present if k not in mapped]
    unmapped.sort(key=lambda x: -x[1])
    print(f"\n  UNMAPPED KEYS the API sends but nothing captures  ({len(unmapped)} keys):")
    if not unmapped:
        print("    (none — every key is already mapped)")
    for k, f, p in unmapped[:40]:
        ex = ""
        for o in objs:
            if _nonempty(o.get(k)):
                ex = str(o[k])[:60].replace("\n", " ")
                break
        print(f"    {k:28} non-empty {f:>4}/{n:<4} e.g. {ex}")

    key, label = NESTED.get(table, (None, None))
    if key:
        sub_present, sub_filled, m = Counter(), Counter(), 0
        for o in objs:
            for item in (o.get(key) or []):
                if not isinstance(item, dict):
                    continue
                m += 1
                for k, v in item.items():
                    sub_present[k] += 1
                    if _nonempty(v):
                        sub_filled[k] += 1
        if m:
            print(f"\n  NESTED '{key}' -> {label}  ({m} items in sample):")
            for k in sorted(sub_present, key=lambda x: -sub_filled[x]):
                print(f"    {k:28} non-empty {sub_filled[k]:>5}/{m}")


def coverage_sanity(conn):
    print(f"\n{'='*78}\nCOVERAGE SANITY\n{'='*78}")
    def one(q):
        try:
            return conn.execute(q).fetchone()[0]
        except Exception:
            return None
    checks = [
        ("rows in courses", "SELECT COUNT(*) FROM courses"),
        ("DISTINCT course_id present in offerings",
         "SELECT COUNT(DISTINCT course_id) FROM offerings"),
        ("offerings whose course_id is NOT in courses",
         "SELECT COUNT(*) FROM offerings o LEFT JOIN courses c "
         "ON c.course_id=o.course_id WHERE c.course_id IS NULL"),
        ("offering_progress rows total", "SELECT COUNT(*) FROM offering_progress"),
        ("  ...status='done'", "SELECT COUNT(*) FROM offering_progress WHERE status='done'"),
        ("  ...status='partial'", "SELECT COUNT(*) FROM offering_progress WHERE status='partial'"),
        ("distinct course_tag_id values seen in courses",
         "SELECT COUNT(DISTINCT course_tag_id) FROM courses WHERE COALESCE(course_tag_id,'')<>''"),
        ("distinct stream_id values seen in courses",
         "SELECT COUNT(DISTINCT stream_id) FROM courses WHERE COALESCE(stream_id,'')<>''"),
        ("courses rows with a source_job_id",
         "SELECT COUNT(*) FROM courses WHERE source_job_id IS NOT NULL"),
        ("jobs of type 'courses' ever completed",
         "SELECT COUNT(*) FROM jobs WHERE type='courses' AND status='completed'"),
        ("last courses job message",
         "SELECT message FROM jobs WHERE type='courses' ORDER BY id DESC LIMIT 1"),
        ("last pipeline job message",
         "SELECT message FROM jobs WHERE type='pipeline' ORDER BY id DESC LIMIT 1"),
    ]
    for label, q in checks:
        v = one(q)
        v = "-" if v is None else (f"{v:,}" if isinstance(v, int) else str(v)[:90])
        print(f"   {label:52} {v}")


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else db.DB_PATH
    sample = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    print(f"DB: {path}   sample: {sample} rows/table")
    with db.connect(path) as conn:
        for t in ("courses", "offerings", "colleges_directory", "sa_programs"):
            audit(conn, t, sample)
        coverage_sanity(conn)


if __name__ == "__main__":
    main()
