"""READ-ONLY pending-attribute report. Writes nothing, deletes nothing.
Run from the app directory:  python gaps.py
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db

TABLES = ["courses", "colleges", "offerings", "college_courses", "colleges_directory",
          "sa_programs", "sa_universities", "sa_countries", "sa_program_exams"]


def main(db_path=None):
    db_path = db_path or db.DB_PATH
    print(f"DB: {db_path}\n")
    with db.connect(db_path) as c:
        def one(q, p=()):
            try:
                return c.execute(q, p).fetchone()[0]
            except Exception:
                return None

        print("=" * 78)
        print("PER-COLUMN COMPLETENESS  (empty = NULL or '')")
        print("=" * 78)
        for t in TABLES:
            cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})")]
            if not cols:
                continue
            n = one(f"SELECT COUNT(*) FROM {t}") or 0
            if not n:
                print(f"\n{t}: (empty table)")
                continue
            print(f"\n{t}  —  {n:,} rows")
            gaps = []
            for col in cols:
                filled = one(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL "
                             f"AND CAST({col} AS TEXT)<>''") or 0
                pct = 100.0 * filled / n
                if pct < 99.5:
                    gaps.append((col, filled, pct))
            if not gaps:
                print("   all columns >=99.5% filled")
            for col, filled, pct in sorted(gaps, key=lambda x: x[2]):
                bar = "#" * int(pct / 5)
                print(f"   {col:22} {pct:5.1f}%  {filled:>9,}/{n:,}  {bar}")

        print()
        print("=" * 78)
        print("CROSS-TABLE GAPS")
        print("=" * 78)
        rows = [
            ("directory colleges with NO row in `colleges` (unreachable by Phase 3)",
             "SELECT COUNT(*) FROM colleges_directory d LEFT JOIN colleges k "
             "ON k.college_id=d.college_id WHERE k.college_id IS NULL"),
            ("colleges never enriched (Phase 3 pending)",
             "SELECT COUNT(*) FROM colleges WHERE enriched_at IS NULL"),
            ("colleges enriched but with NO website/email/phone",
             "SELECT COUNT(*) FROM colleges WHERE enriched_at IS NOT NULL AND "
             "COALESCE(website,'')='' AND COALESCE(email,'')='' AND COALESCE(phone,'')=''"),
            ("college_courses rows with no course_url",
             "SELECT COUNT(*) FROM college_courses WHERE COALESCE(course_url,'')=''"),
            ("college_courses rows with no hostel_fees",
             "SELECT COUNT(*) FROM college_courses WHERE COALESCE(hostel_fees,'')=''"),
            ("college_courses rows with unparsed fee (fees_inr NULL, total_fees set)",
             "SELECT COUNT(*) FROM college_courses WHERE fees_inr IS NULL "
             "AND COALESCE(total_fees,'')<>''"),
            ("courses never expanded by Phase 2",
             "SELECT COUNT(*) FROM courses c LEFT JOIN offering_progress p "
             "ON p.course_id=c.course_id WHERE p.course_id IS NULL OR p.status<>'done'"),
            ("colleges with courses-fees NOT scraped (Phase 4 pending)",
             "SELECT COUNT(*) FROM colleges k LEFT JOIN cc_progress p "
             "ON p.college_id=k.college_id WHERE p.college_id IS NULL "
             "OR p.status NOT IN ('done','empty')"),
            ("offerings with admission date sentinel '0000-00-00'",
             "SELECT COUNT(*) FROM offerings WHERE admission_start='0000-00-00' "
             "OR admission_end='0000-00-00'"),
            ("SA programmes with a program_url that has never been fetched",
             "SELECT COUNT(*) FROM sa_programs WHERE COALESCE(program_url,'')<>''"),
            ("SA universities with no city",
             "SELECT COUNT(*) FROM sa_universities WHERE COALESCE(city,'')=''"),
            ("SA countries with no facet id",
             "SELECT COUNT(*) FROM sa_countries WHERE country_id IS NULL"),
            ("rows sitting unpromoted in staging",
             "SELECT COUNT(*) FROM staging"),
        ]
        for label, q in rows:
            v = one(q)
            print(f"   {label:66} {'-' if v is None else format(v, ',')}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
