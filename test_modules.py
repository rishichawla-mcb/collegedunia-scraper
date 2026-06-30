"""
Fresh-DB module test for the Collegedunia scraper.

Exercises every module end-to-end against a throwaway SQLite DB (no network):
db schema + upserts, all parsers, the staging->validate->promote governance
flow, diff, exports, the wipe/reset functions, and the live-scraper helpers.

Run:  CD_DB_PATH=/tmp/cd_test.db python test_modules.py
Exits non-zero if any check fails (so it doubles as a CI test).
"""
import json
import os
import tempfile

os.environ.setdefault("CD_DB_PATH", os.path.join(tempfile.gettempdir(), "cd_modtest.db"))
DB = os.environ["CD_DB_PATH"]
if os.path.exists(DB):
    os.remove(DB)

import db
import scraper
import export

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  {'✅' if cond else '❌'} {name}" + (f"  — {detail}" if detail and not cond else ""))


print("MODULE 1 — DB schema + counts")
db.init_db(DB)
check("init_db builds schema", db.counts(DB) == {"courses": 0, "colleges": 0,
                                                 "offerings": 0, "courses_done_phase2": 0})

print("MODULE 2 — Phase 1 parser + course upsert")
sample_course = {"name": "Bachelor of Science [B.Sc]", "lead_params": {"course_id": 3989, "stream_id": 6},
                 "duration": "3 Years", "course_type": "Degree", "level": "Graduation",
                 "colleges_data": {"count": 960}, "fees": "1.2 Lakhs"}
pc = scraper.parse_course(sample_course)
check("parse_course extracts id+name+stream", pc["course_id"] == 3989 and pc["stream_name"] == "Computer Applications")
db.upsert_courses([pc], DB)
check("upsert_courses persists", db.counts(DB)["courses"] == 1)

print("MODULE 3 — Phase 2 parsers + governance staging")
offering_api = {"name": "B.Sc", "college": {"name": "Test College", "college_id": 555, "city": "Pune", "state_id": 17},
                "lead_params": {"college_id": 555}, "fees_data": {"amount": 89676, "text": "Total Fees"},
                "course_rating": 3.9, "eligibility": "10+2"}
off = scraper.parse_offering(3989, offering_api)
col = scraper.parse_college(offering_api)
check("parse_offering fee+college", off["fees_amount"] == 89676 and off["college_id"] == 555)
check("parse_college id+city", col and col["college_id"] == 555 and col["city"] == "Pune")
jid = db.create_job("offerings", {"staging": True})
ns = db.stage_records(jid, "colleges", [col], DB) + db.stage_records(jid, "offerings", [off], DB)
check("stage_records writes staging (not master)", ns == 2 and db.counts(DB)["offerings"] == 0)
v = db.validate_job(jid, {}, DB)
check("validate_job scores staged data", v["passed"] and v["total"] == 2, str(v))
summ = db.promote_job(jid, DB)
check("promote_job merges to master", db.counts(DB)["offerings"] == 1 and db.counts(DB)["colleges"] == 1)
with db.connect(DB) as conn:
    sj = conn.execute("SELECT source_job_id FROM offerings WHERE course_id=3989").fetchone()[0]
check("promotion stamps source_job_id", sj == jid, f"got {sj}")
dj = db.diff_job(jid, DB)
check("diff_job runs", isinstance(dj, dict))

print("MODULE 4 — Phase 3 JSON-LD parser")
ld_html = ('<script type="application/ld+json">' +
           json.dumps({"@type": "CollegeOrUniversity", "url": "http://x.edu", "email": "a@x.edu",
                       "telephone": "+91-11-100", "aggregateRating": {"ratingValue": "4.2", "ratingCount": "50"}}) +
           "</script>")
ld = scraper.parse_college_ld(ld_html)
check("parse_college_ld extracts contact+rating", ld["email"] == "a@x.edu" and ld["rating_value"] == 4.2)
db.update_college_details(555, ld, DB)
with db.connect(DB) as conn:
    em = conn.execute("SELECT email FROM colleges WHERE college_id=555").fetchone()[0]
check("update_college_details persists enrichment", em == "a@x.edu")

print("MODULE 5 — Phase 4 courses-fees (__NEXT_DATA__) parser")
nd = {"props": {"initialProps": {"pageProps": {"data": {
    "college_name": "Test College",
    "course_data": {"course_count": 1, "total_pages": 1, "courses": [
        {"display_name": "Bachelor of Science [B.Sc]", "short_head": "B.Sc", "duration": "3 Years ",
         "level": "Graduation", "course_type": "Degree", "type": "Full Time", "eligibility": "10+2",
         "course_rating": 3.9, "reviews_count": 76,
         "streams": [{"name": "Biotechnology", "fees_data": {"amount": 446000, "amount_formatted": "4.46 Lakhs"},
                      "admission": {"admission_start_date": "2026-10-30", "admission_end_date": "2026-12-01"}}]}]}}}}}}
page = ('<script id="__NEXT_DATA__" type="application/json">' + json.dumps(nd) + "</script>"
        '<table><tr><th>Course</th><th>Total Fees</th><th>Hostel Fees</th></tr>'
        '<tr><td>B.Sc</td><td>₹ 4.46 Lakhs</td><td>₹ 1,20,000</td></tr></table>')
parsed = scraper.parse_courses_fees(page)
row = parsed["courses"][0]
check("parse_courses_fees JSON: name+specialization",
      row["course_name"] == "Bachelor of Science [B.Sc] (Biotechnology)" and row["specialization"] == "Biotechnology")
check("parse_courses_fees JSON: fee+hostel+dates",
      row["fees_inr"] == 446000 and row["hostel_fees"] == "₹1,20,000" and row["application_end"] == "2026-12-01")
db.upsert_college_courses([{**row, "college_id": 555, "college_name": "Test College",
                            "source_url": "x", "scraped_at": 0, "source_job_id": jid}], DB)
with db.connect(DB) as conn:
    ccrow = conn.execute("SELECT duration, rating, application_start FROM college_courses LIMIT 1").fetchone()
check("college_courses stores rich columns", ccrow[0] == "3 Years" and ccrow[2] == "2026-10-30")

print("MODULE 6 — Live scraper helpers")
html = ('<title>X</title><div class="card"><a class="name" href="/c/1">Alpha</a>'
        '<span class="fee">2.1 Lakhs</span></div>')
info = scraper.analyze_page(html)
check("analyze_page lists classes", {x["class"] for x in info["classes"]} >= {"card", "name", "fee"})
rows = scraper.extract_by_classes(html, ["name"], mode="links")
check("extract_by_classes links absolutized", rows and rows[0]["href"].endswith("/c/1"))
rows2 = scraper.extract_by_selector(html, "span.fee", mode="text")
check("extract_by_selector text", rows2 and rows2[0]["text"] == "2.1 Lakhs")

print("MODULE 7 — Exports")
check("to_csv", b"course_id" in export.to_csv("courses", DB))
check("to_json", b"3989" in export.to_json("offerings", DB))
xb = export.to_xlsx(("courses", "colleges", "offerings", "college_courses"), DB)
check("to_xlsx (xlsx magic bytes)", xb[:2] == b"PK")

print("MODULE 8 — Reset / wipe scopes")
# keep courses + colleges
d1 = db.wipe_data(keep_colleges=True, db_path=DB)
cnt = db.counts(DB)
check("wipe keep colleges: courses+colleges kept, offerings cleared",
      cnt["courses"] == 1 and cnt["colleges"] == 1 and cnt["offerings"] == 0)
with db.connect(DB) as conn:
    cc_left = conn.execute("SELECT COUNT(*) FROM college_courses").fetchone()[0]
    jobs_left = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
check("wipe clears college_courses + jobs", cc_left == 0 and jobs_left == 0)
# Phase-1-only: also clears colleges
d2 = db.wipe_data(keep_colleges=False, db_path=DB)
cnt = db.counts(DB)
check("wipe Phase-1-only: courses kept, colleges cleared", cnt["courses"] == 1 and cnt["colleges"] == 0)

print()
print(f"RESULT: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", ", ".join(FAIL))
    raise SystemExit(1)
print("ALL MODULES OK")
