"""
Shiksha discovery — read-only verification. No network, no writes.

    python sk_verify.py

Discovery reported:

    57,751 colleges · 55,539 alias slugs · 1,248 universities
    317,892 courses · 317,907 offerings

Two of those numbers need explaining before anything is built on top of them, and
guessing at either would be worse than asking the database.

**1. Courses ≈ offerings, 1:1.** 317,892 courses against 317,907 offerings means
almost every "course" is offered by exactly one college. A real course catalogue
does not look like that — Collegedunia's 21,689 courses carry far more offerings
than courses. The likely explanation is that Shiksha's `courseId` in
`/college/<slug>-<id>/course-<cslug>-<courseId>` is a **per-college page id, not
a shared catalogue id**. If so, `sk_courses` is not a catalogue at all, and the
side-by-side comparison cannot join Shiksha to Collegedunia on course id — it
has to match on name. That is a design decision, so it should be established, not
assumed. Sections 2 and 3 below test it directly: if the id were global, the same
course slug would map to ONE id; if it is per-college, one slug maps to thousands.

**2. Fewer alias slugs (55,539) than colleges (57,751).** The 2026-09-24 recon
counted 84,193 "college home" URLs for 57,695 distinct ids and concluded there
were ~26k alias slugs. This run found fewer distinct home slugs than colleges,
which points the other way: roughly one home URL per college, and some colleges
discovered only from tab or offering URLs. Either the recon's definition of a
"home" URL was looser than `_COLLEGE_HOME`, or something is being dropped.
Section 4 shows which.
"""
from __future__ import annotations

BUILD = "2026-09-25a"

import sk_db


def q(conn, sql, args=()):
    return conn.execute(sql, args).fetchall()


def one(conn, sql, args=()):
    r = conn.execute(sql, args).fetchone()
    return r[0] if r else 0


def main() -> None:
    print(f"Shiksha discovery verification [BUILD {BUILD}]")
    print(f"db: {sk_db.SK_DB_PATH}\n")

    with sk_db.connect() as conn:
        # ------------------------------------------------------------ 1
        print("1. Headline counts")
        for k, v in sk_db.counts().items():
            print(f"   {k:<22} {v:>12,}")
        print()

        # ------------------------------------------------------------ 2
        print("2. Is `course_id` a shared catalogue id, or per-college?")
        multi = one(conn, "SELECT COUNT(*) FROM sk_courses WHERE colleges_count > 1")
        total = one(conn, "SELECT COUNT(*) FROM sk_courses")
        print(f"   courses offered by >1 college : {multi:,} of {total:,} "
              f"({100.0*multi/max(1,total):.1f}%)")
        print("   colleges_count distribution:")
        for row in q(conn,
                     "SELECT CASE WHEN colleges_count>=10 THEN '10+' "
                     "ELSE CAST(colleges_count AS TEXT) END AS bucket, COUNT(*) c "
                     "FROM sk_courses GROUP BY bucket "
                     "ORDER BY CAST(REPLACE(bucket,'+','') AS INTEGER) LIMIT 12"):
            print(f"     offered by {row[0]:>3} college(s): {row[1]:>10,}")
        print()
        print("   Same slug, many ids?  (a global id would give exactly one)")
        for row in q(conn,
                     "SELECT slug, COUNT(*) n, MIN(course_id), MAX(course_id) "
                     "FROM sk_courses WHERE slug<>'' GROUP BY slug "
                     "ORDER BY n DESC LIMIT 10"):
            print(f"     {row[0][:44]:<44} {row[1]:>8,} ids  "
                  f"({row[2]}…{row[3]})")
        distinct_slugs = one(conn, "SELECT COUNT(DISTINCT slug) FROM sk_courses")
        print(f"\n   distinct course slugs : {distinct_slugs:,}")
        print(f"   distinct course ids   : {total:,}")
        print("   -> ids >> slugs means the id is PER-COLLEGE, and `sk_courses`")
        print("      is a page index, not a catalogue. Comparison must match on")
        print("      name/slug, not on id.")
        print()

        # ------------------------------------------------------------ 3
        print("3. Do two colleges ever share one course_id?")
        shared = one(conn,
                     "SELECT COUNT(*) FROM (SELECT course_id FROM sk_offerings "
                     "GROUP BY course_id HAVING COUNT(DISTINCT college_id) > 1)")
        print(f"   course_ids seen at more than one college: {shared:,}")
        if shared:
            for row in q(conn,
                         "SELECT course_id, COUNT(DISTINCT college_id) n FROM sk_offerings "
                         "GROUP BY course_id HAVING n>1 ORDER BY n DESC LIMIT 5"):
                print(f"     course {row[0]}: {row[1]} colleges")
        print()

        # ------------------------------------------------------------ 4
        print("4. Aliases — fewer slugs than colleges?")
        n_alias = one(conn, "SELECT COUNT(*) FROM sk_college_aliases")
        n_col = one(conn, "SELECT COUNT(*) FROM sk_colleges")
        no_home = one(conn, "SELECT COUNT(*) FROM sk_colleges c WHERE NOT EXISTS "
                            "(SELECT 1 FROM sk_college_aliases a "
                            "WHERE a.college_id=c.college_id)")
        print(f"   alias slug rows          : {n_alias:,}")
        print(f"   colleges                 : {n_col:,}")
        print(f"   colleges with NO home URL: {no_home:,}  "
              f"(found only via tab/offering URLs)")
        print("   alias_count distribution:")
        # COALESCE: a college discovered without a home URL has alias_count
        # NULL, and the format string crashed on it. It did not surface on the
        # live database because discovery always sets the column there — a bug
        # that only appears on data the report has not met yet.
        for row in q(conn, "SELECT COALESCE(alias_count,0), COUNT(*) "
                           "FROM sk_colleges GROUP BY 1 ORDER BY 1 LIMIT 8"):
            print(f"     {row[0]:>3} slug(s): {row[1]:>10,}")
        print()

        # ------------------------------------------------------------ 5
        print("5. Coverage per college")
        no_tabs = one(conn, "SELECT COUNT(*) FROM sk_colleges "
                            "WHERE COALESCE(tabs,'')=''")
        no_off = one(conn, "SELECT COUNT(*) FROM sk_colleges c WHERE NOT EXISTS "
                           "(SELECT 1 FROM sk_offerings o "
                           "WHERE o.college_id=c.college_id)")
        print(f"   colleges with no tabs recorded : {no_tabs:,}")
        print(f"   colleges with no offerings     : {no_off:,}")
        row = conn.execute(
            "SELECT AVG(n), MAX(n) FROM (SELECT COUNT(*) n FROM sk_offerings "
            "GROUP BY college_id)").fetchone()
        print(f"   offerings per college          : avg {row[0] or 0:.1f}, "
              f"max {row[1] or 0:,}")
        print("   most common tab sets:")
        for r in q(conn, "SELECT tabs, COUNT(*) c FROM sk_colleges "
                         "GROUP BY tabs ORDER BY c DESC LIMIT 6"):
            print(f"     {(r[0] or '(none)')[:60]:<60} {r[1]:>8,}")
        print()

        # ------------------------------------------------------------ 6
        print("6. Sitemaps")
        r = conn.execute(
            "SELECT COUNT(*), SUM(urls), SUM(colleges), SUM(offerings), "
            "SUM(universities), SUM(bytes) FROM sk_sitemap_progress "
            "WHERE status='done'").fetchone()
        print(f"   done: {r[0] or 0} files · {r[1] or 0:,} URLs · "
              f"{r[2] or 0:,} college homes · {r[3] or 0:,} offerings · "
              f"{r[4] or 0:,} universities · {(r[5] or 0)/1048576:.1f} MB")
        bad = q(conn, "SELECT sitemap_url, status, message FROM sk_sitemap_progress "
                      "WHERE status<>'done'")
        print(f"   not done: {len(bad)}")
        for b in bad[:5]:
            print(f"     {b[0].rsplit('/',1)[-1]} [{b[1]}] {(b[2] or '')[:70]}")
        print()

        # ------------------------------------------------------------ 7
        print("7. Change tracking")
        try:
            print(f"   data_changes rows: "
                  f"{one(conn, 'SELECT COUNT(*) FROM data_changes'):,}")
            for r in q(conn, "SELECT table_name, change_type, COUNT(*) FROM "
                             "data_changes GROUP BY 1,2 ORDER BY 3 DESC LIMIT 6"):
                print(f"     {r[0]:<22} {r[1]:<10} {r[2]:>10,}")
        except Exception as err:  # noqa: BLE001
            print(f"   (no change log: {err})")

        # ------------------------------------------------------------ 7b
        print("\n7b. Phase Ⓑ — detail coverage and the coarseness question")
        done = one(conn, "SELECT COUNT(*) FROM sk_colleges "
                         "WHERE detail_scraped_at IS NOT NULL")
        print(f"   colleges with detail : {done:,}")
        if done:
            for col in ("name", "city", "website", "phone", "email", "latitude",
                        "rating", "reviews_count", "facilities", "recruiters",
                        "highlights", "admission_text", "description"):
                try:
                    n = one(conn, f"SELECT COUNT(*) FROM sk_colleges WHERE "
                                  f"detail_scraped_at IS NOT NULL AND "
                                  f"COALESCE(CAST({col} AS TEXT),'')<>''")
                    print(f"     {col:<16} filled for {n:>7,} "
                          f"({100.0*n/done:5.1f}%)")
                except Exception as err:  # noqa: BLE001
                    print(f"     {col:<16} ? {str(err)[:50]}")

            # THE question for the comparison: is a base-course fee range one
            # course or twenty-four? Measured across the slice, not argued about.
            print("\n   base-course rows, by how many actual courses they cover:")
            for row in q(conn,
                         "SELECT CASE WHEN COALESCE(course_count,0)<=1 THEN '1' "
                         "WHEN course_count<=3 THEN '2-3' "
                         "WHEN course_count<=10 THEN '4-10' ELSE '11+' END b, "
                         "COUNT(*) n FROM sk_college_base_courses GROUP BY b "
                         "ORDER BY n DESC"):
                print(f"     covers {row[0]:<5} {row[1]:>9,} rows")
            tot = one(conn, "SELECT COUNT(*) FROM sk_college_base_courses")
            nofee = one(conn, "SELECT COUNT(*) FROM sk_college_base_courses "
                              "WHERE min_fees IS NULL AND max_fees IS NULL")
            wide = one(conn, "SELECT COUNT(*) FROM sk_college_base_courses "
                             "WHERE COALESCE(course_count,0) > 3")
            covered = one(conn, "SELECT COALESCE(SUM(course_count),0) "
                                "FROM sk_college_base_courses")
            if tot:
                print(f"\n   rows with NO fee range : {nofee:,}/{tot:,} "
                      f"({100.0*nofee/tot:.1f}%)")
                print(f"   rows covering >3       : {wide:,}/{tot:,} "
                      f"({100.0*wide/tot:.1f}%)")
                print(f"   actual courses behind them: {covered:,} "
                      f"(so ~{covered/max(1,tot):.1f} courses per fee range)")
                print("   -> a high figure here means base-course fees are too "
                      "coarse to compare on, and the /courses tab pass is "
                      "needed. A low one means phase Ⓑ v1 is enough.")

            print("\n   widest fee ranges (a big spread = a range worth little):")
            for row in q(conn,
                         "SELECT name, min_fees, max_fees, course_count "
                         "FROM sk_college_base_courses "
                         "WHERE min_fees IS NOT NULL AND max_fees > min_fees "
                         "ORDER BY (max_fees-min_fees) DESC LIMIT 8"):
                print(f"     {row[0][:26]:<26} {row[1]:>10,} – {row[2]:>10,} "
                      f"over {row[3]} course(s)")

            print("\n   catalogue, most widely offered:")
            for row in q(conn, "SELECT base_course_id, name, colleges_count "
                               "FROM sk_base_courses ORDER BY colleges_count "
                               "DESC LIMIT 12"):
                print(f"     [{row[0]:>5}] {row[1][:34]:<34} {row[2]:>7,} colleges")

            bad = q(conn, "SELECT college_id, recruiters FROM sk_colleges "
                          "WHERE recruiters LIKE '%  %' LIMIT 3")
            print(f"\n   rows with a double space in `recruiters` "
                  f"(the H&M question): {len(bad)}")
            for b in bad:
                print(f"     {b[0]}: {(b[1] or '')[:90]}")

        # ------------------------------------------------------------ 8
        print("\n8. Sample rows")
        for r in q(conn, "SELECT college_id, slug, alias_count, tabs FROM sk_colleges "
                         "ORDER BY alias_count DESC, college_id LIMIT 5"):
            print(f"   {r[0]:>8}  {(r[1] or '')[:38]:<38} aliases={r[2]} tabs={r[3]}")
        for r in q(conn, "SELECT college_id, course_id, course_slug FROM sk_offerings "
                         "LIMIT 5"):
            print(f"   offering: college {r[0]} × course {r[1]}  {r[2][:40]}")


if __name__ == "__main__":
    main()
