"""Zero-network backfills. Everything here is derived from data that is ALREADY
in the database — no HTTP requests are made, nothing is re-scraped.

  1. course_enrichment   rebuild the per-course aggregate table from `offerings`
  2. fees_inr            parse college_courses.total_fees -> integer rupees
  3. sa_countries        real country names + facet ids, matched from sa_facets

Nothing source is deleted. (1) rebuilds a *derived* cache table that is a pure
function of `offerings`; (2) and (3) only ever fill columns that are currently
empty.

  python freefix.py            report only, writes nothing
  python freefix.py --apply    commit
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db  # noqa: E402

try:
    import sa_db
except Exception:                                   # pragma: no cover
    sa_db = None


# --- country_code ('uk', 'usa') <-> facet label ('United Kingdom') ----------
ALIASES = {
    "usa": ("united states", "united states of america", "us", "usa", "america"),
    "uk": ("united kingdom", "uk", "great britain", "britain", "england"),
    "uae": ("united arab emirates", "uae"),
    "nz": ("new zealand",),
    "hongkong": ("hong kong",),
    "southkorea": ("south korea", "korea"),
    "srilanka": ("sri lanka",),
    "southafrica": ("south africa",),
    "czechrepublic": ("czech republic", "czechia"),
}


def _norm(s):
    return re.sub(r"[^a-z]", "", str(s or "").lower())


def _country_map(conn):
    """{normalised label -> (value_id, label)} from the country facet."""
    out = {}
    try:
        rows = conn.execute(
            "SELECT filter_name, value_id, label FROM sa_facets "
            "WHERE lower(filter_name) LIKE '%countr%'").fetchall()
    except Exception:
        return out
    for r in rows:
        lab = (r["label"] or "").strip()
        if lab:
            out[_norm(lab)] = (r["value_id"], lab)
    return out


def _match(code, cmap):
    code = (code or "").strip().lower()
    if not code:
        return None
    hit = cmap.get(_norm(code))
    if hit:
        return hit
    for alias in ALIASES.get(_norm(code), ()):
        hit = cmap.get(_norm(alias))
        if hit:
            return hit
    # last resort: a facet label whose normalised form starts with the code
    n = _norm(code)
    if len(n) >= 4:
        for k, v in cmap.items():
            if k.startswith(n):
                return v
    return None


# ---------------------------------------------------------------------------
def task_enrichment(apply_changes, db_path):
    print("\n[1] course_enrichment  (derived from `offerings`)")
    with db.connect(db_path) as conn:
        have = conn.execute("SELECT COUNT(*) FROM course_enrichment").fetchone()[0]
        src = conn.execute("SELECT COUNT(DISTINCT course_id) FROM offerings").fetchone()[0]
    print(f"    rows now: {have:,}   distinct course_id in offerings: {src:,}")
    if not src:
        print("    offerings is empty — nothing to build. skipped.")
        return
    if not apply_changes:
        print(f"    would rebuild ~{src:,} rows.")
        return
    n = db.enrich_courses(db_path)
    print(f"    rebuilt {n:,} rows.")


def task_fees(apply_changes, db_path):
    print("\n[2] college_courses.fees_inr  (parsed from total_fees)")
    with db.connect(db_path) as conn:
        q = lambda s: conn.execute(s).fetchone()[0]
        tot = q("SELECT COUNT(*) FROM college_courses")
        miss = q("SELECT COUNT(*) FROM college_courses WHERE fees_inr IS NULL "
                 "AND COALESCE(total_fees,'')<>''")
        filled = q("SELECT COUNT(*) FROM college_courses WHERE fees_inr IS NOT NULL")
        sample = conn.execute(
            "SELECT total_fees FROM college_courses WHERE fees_inr IS NULL "
            "AND COALESCE(total_fees,'')<>'' LIMIT 5").fetchall()
    print(f"    rows {tot:,} | fees_inr set {filled:,} | parseable but unset {miss:,}")
    for s in sample:
        print(f"      e.g. {s[0]!r} -> {db.fee_to_inr(s[0])}")
    if not miss:
        print("    nothing to do.")
        return
    if not apply_changes:
        print(f"    would fill up to {miss:,} rows (only where fees_inr IS NULL).")
        return
    n = db.normalize_fees(db_path)
    print(f"    filled {n:,} rows.")


def task_countries(apply_changes, sa_path):
    print("\n[3] sa_countries.name / country_id  (matched from sa_facets)")
    if sa_db is None:
        print("    sa_db unavailable — skipped.")
        return
    with sa_db.connect(sa_path) as conn:
        try:
            rows = conn.execute(
                "SELECT country_code, country_id, name FROM sa_countries "
                "ORDER BY country_code").fetchall()
        except Exception as e:
            print(f"    sa_countries not present ({e}) — skipped.")
            return
        cmap = _country_map(conn)
        print(f"    countries: {len(rows):,} | country facet values: {len(cmap):,}")
        if not cmap:
            print("    no country facet captured yet — run the SA 'facets' phase "
                  "first, then re-run this. skipped.")
            return

        updates, unmatched, already = [], [], 0
        for r in rows:
            code = r["country_code"]
            cur_name = (r["name"] or "").strip()
            hit = _match(code, cmap)
            if not hit:
                unmatched.append(code)
                continue
            vid, label = hit
            # only fill what is empty or still the placeholder (code.upper())
            new_name = label if (not cur_name or cur_name == code.upper()) else None
            new_id = vid if r["country_id"] is None else None
            if new_name is None and new_id is None:
                already += 1
                continue
            updates.append((new_name, new_id, code, label, vid))

        print(f"    to fill: {len(updates):,} | already good: {already:,} | "
              f"no facet match: {len(unmatched):,}")
        for u in updates[:8]:
            print(f"      {u[2]:<14} -> name={u[3]!r} id={u[4]}")
        if unmatched:
            print(f"    unmatched codes (left untouched): {', '.join(unmatched[:12])}"
                  + (" …" if len(unmatched) > 12 else ""))
        if not apply_changes or not updates:
            if not apply_changes:
                print("    nothing written.")
            return
        conn.executemany(
            "UPDATE sa_countries SET name=COALESCE(?, name), "
            "country_id=COALESCE(?, country_id) WHERE country_code=?",
            [(u[0], u[1], u[2]) for u in updates])
        conn.commit()
        print(f"    wrote {len(updates):,} rows.")


def task_report_network_gaps(db_path):
    """Things that LOOK like free wins but are not — they need requests."""
    print("\n[i] still needs the network (reported, not attempted)")
    with db.connect(db_path) as conn:
        try:
            st = db.course_url_backfill_status(db_path)
            print(f"    college_courses.course_url missing on {st['rows_missing']:,} "
                  f"rows across {st['colleges_missing']:,} colleges")
            print("      -> not derivable offline; needs a Phase-4 re-scrape of "
                  "those colleges.")
        except Exception:
            pass
        try:
            n = conn.execute("SELECT COUNT(*) FROM colleges_directory d "
                             "LEFT JOIN colleges k ON k.college_id=d.college_id "
                             "WHERE k.college_id IS NULL").fetchone()[0]
            print(f"    directory colleges never enriched: {n:,}  "
                  "-> Phase 3 with 'include directory' ticked.")
        except Exception:
            pass


def main(apply_changes=False, db_path=None, sa_path=None):
    db_path = db_path or db.DB_PATH
    sa_path = sa_path or (getattr(sa_db, "DB_PATH", db_path) if sa_db else db_path)
    print(f"DB: {db_path}")
    if sa_path != db_path:
        print(f"SA DB: {sa_path}")
    print(f"mode: {'APPLY' if apply_changes else 'REPORT ONLY'}   (0 HTTP requests)")

    task_enrichment(apply_changes, db_path)
    task_fees(apply_changes, db_path)
    task_countries(apply_changes, sa_path)
    task_report_network_gaps(db_path)

    if not apply_changes:
        print("\nnothing written. re-run with --apply to commit.")
    return 0


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))
