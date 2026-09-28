"""
Shiksha phase Ⓑ (college detail) — test harness.

Drives the REAL runner (`sk_scraper.run_detail`) with the transport monkeypatched
at `sk_scraper._session_for`, the same seam the discovery suite uses. The parser,
the brace scanner, the route policy, the upserts, the freshness tracking, the
progress queue and the job bookkeeping all run for real.

Every load-bearing guard is mutation-tested: the guard is broken on purpose and
the test must then fail.

    python t_shiksha_detail.py
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="skdet_")
os.environ["CD_DB_PATH"] = os.path.join(_TMP, "data.db")
os.environ["CD_SK_DB_PATH"] = os.path.join(_TMP, "shiksha.db")

import requests  # noqa: E402

import sk_db      # noqa: E402
import sk_parse   # noqa: E402
import sk_scraper  # noqa: E402

SITE = sk_scraper.SITE
FAILURES, PASSES = [], []


def check(name, cond, detail=""):
    (PASSES if cond else FAILURES).append(name)
    print(("  ok   " if cond else "  FAIL ") + name +
          (f"  — {detail}" if detail and not cond else ""))
    return cond


# ---------------------------------------------------------------------------
# A college page, shaped like the real one: the college node sits one level
# below the root, and the root's OWN `instituteData` key is an empty dict.
# ---------------------------------------------------------------------------
def node_for(cid: int, name: str, tuples):
    return {
        "instituteId": cid, "listingId": cid, "listingName": name,
        "ownership": "Private", "instituteTier": "top", "countryId": 2,
        "totalBaseCourseCount": len(tuples), "reviewCount": 6,
        "anaCountString": "75", "flagshipCourseId": 306967,
        "description": "<p>Admission is &amp; merit based</p>",
        "instituteTopCardData": {
            "instituteName": name, "instituteShortName": name,
            "instituteSpecificationType": "College",
            "logoImageUrl": "https://img/logo.jpg",
            "photoCount": 24, "videoCount": 23,
            "reviewDetails": {"averageRating": 4.518181822516701,
                              "totalCount": 24, "verifiedCount": 22},
            "affiliationData": [{"name": "University of Rajasthan"}],
            "parentUniversityData": None, "rankingData": [],
        },
        "currentLocation": {"city_name": "Jaipur", "state_name": "Rajasthan",
                            "city_id": 109, "state_id": 121,
                            # the REAL key spellings, read off the live payload
                            # 2026-09-28 — the fixture must not test a shape the
                            # site does not actually send
                            "contact_details": {"address": "9, Govind Marg",
                                                "website_url": "https://archedu.org/",
                                                "admission_contact_number": "9414070678",
                                                "admission_email": "a@archedu.org",
                                                "generic_email": "",
                                                "latitude": "26.85",
                                                "longitude": "75.80"}},
        "seoData": {"metaTitle": "T", "metaDescription": "D",
                    "canonicalUrl": f"/college/x-{cid}"},
        "facilityInfo": [{"facilityName": "Design Studio"}],
        "recruitmentCompanies": [{"companyName": "Future Group"}],
        "highlightsInfo": {"highlights": [{"title": "Established", "value": "2000"}]},
        "streamObjects": [{"name": "Design"}],
        "acceptedExams": [{"name": "AIEED"}],
        "admissionData": {"admissionDetails": "<p>merit</p>",
                          "admissionPostedDate": "May 28, 2026", "examList": []},
        "childPageToUrls": {"fees": "/f", "courses": "/c"},
        "baseCourseTuples": tuples,
        # must be ignored — a byte-for-byte duplicate upstream
        "filteredBaseCourseTuples": [dict(t, name="DUPLICATE") for t in tuples],
        # must never be stored — third-party personal data
        "usersInfo": {"11614203": {"name": "Anangsha Patra",
                                   "profilePageURL": "/userprofile/x",
                                   "city": 74, "workExperience": 1}},
    }


TUPLE_BDES = {"id": 9, "courseId": 306967, "name": "B.Des", "courseLevel": "UG",
              "minFees": 1940000, "maxFees": 1940000, "minDuration": 4,
              "maxDuration": 4, "minDurationUnit": "years", "totalSeats": 100,
              "courseCount": 5, "courseRating": 4.5875, "ratingCount": 16,
              "minEligibilityXII": 50, "scholarshipsCount": 0,
              "exams": [{"name": "AIEED"}], "url": "/x"}
TUPLE_MDES = {"id": 12, "courseId": 306970, "name": "M.Des", "courseLevel": "PG",
              "minFees": 900000, "maxFees": 1100000, "minDuration": 2,
              "maxDuration": 2, "minDurationUnit": "years", "totalSeats": 30,
              "courseCount": 1, "exams": [], "url": "/y"}


def page_for(cid: int, name: str, tuples) -> bytes:
    state = {"gnbLinks": {"config": {"child": []}},
             "instituteData": {},          # the decoy: empty, and named right
             "courseData": {},
             "iulpPageData": node_for(cid, name, tuples)}
    return (
        "<!doctype html><html><head><title>x</title></head><body>"
        "<script>window.__PRELOADED_STATE__ = " + json.dumps(state) +
        ";window.__OTHER__={\"a\":\"}\"};</script></body></html>"
    ).encode()


COLLEGES = {72: ("Arch College", [TUPLE_BDES, TUPLE_MDES]),
            99: ("NIT Trichy", [TUPLE_BDES]),
            101: ("Gone College", None)}       # None -> page without state

PAGES = {}
for _cid, (_nm, _tp) in COLLEGES.items():
    url = f"{SITE}/college/slug-{_cid}"
    PAGES[url] = (page_for(_cid, _nm, _tp) if _tp is not None
                  else b"<html><body>no state here</body></html>")

REQUESTS = []       # (url, proxy_or_None)


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self.status_code = status
        self._body = body
        self.headers = {"Content-Length": str(len(body))}
        self.download_size = len(body)
        self.header_size = 100

    @property
    def content(self):
        return self._body

    @property
    def text(self):
        return self._body.decode("utf-8", "replace")

    def json(self):
        return json.loads(self._body.decode())

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, refuse_direct=False, transport_error=False):
        self.refuse_direct = refuse_direct
        self.transport_error = transport_error

    def get(self, url, proxies=None, timeout=None, allow_redirects=True, **kw):
        prox = (proxies or {}).get("https")
        REQUESTS.append((url, prox))
        if url.startswith(sk_scraper.IP_ECHO.split("?")[0]):
            ip = "198.51.100.44" if prox else "203.0.113.9"
            return FakeResponse(('{"ip":"%s"}' % ip).encode())
        if self.transport_error and prox is None:
            raise OSError("tunnel died")            # NOT a site refusal
        if self.refuse_direct and prox is None:
            return FakeResponse(b"nope", 403)       # a site refusal
        body = PAGES.get(url)
        return FakeResponse(body if body is not None else b"missing",
                            200 if body is not None else 404)


def install(**kw):
    def _session_for(client):
        s = getattr(client, "_sk_curl", None)
        if s is None:
            s = FakeSession(**kw)
            client._sk_curl = s
        return s
    sk_scraper._session_for = _session_for


install()


def fresh_db(with_discovery=True):
    p = os.path.join(_TMP, "shiksha.db")
    for suf in ("", "-wal", "-shm"):
        try:
            os.remove(p + suf)
        except OSError:
            pass
    import freshness as _fr
    _fr._SCHEMA_READY.clear()
    sk_db.init_db()
    if with_discovery:
        now = 1.0
        sk_db.upsert_colleges([
            {"college_id": cid, "slug": f"slug-{cid}",
             "url": f"{SITE}/college/slug-{cid}", "tabs": "courses,fees",
             "alias_count": 3, "lastmod": "2026-09-01", "discovered_at": now,
             "scraped_at": now, "source_job_id": 1}
            for cid in COLLEGES])
    REQUESTS.clear()


CFG = {"proxy_mode": "gateway", "proxy_gateway": "http://u:p@gw.example.com:7777",
       "concurrency": 2, "delay": 0, "adaptive": False, "max_retries": 2,
       "backoff": 0.01}
LOGS = []


def run(cfg=None):
    del LOGS[:]
    jid = sk_db.create_job("detail", {})
    sk_scraper.run_detail(jid, {**CFG, **(cfg or {})}, log=LOGS.append)
    return sk_db.get_job(jid)


# ---------------------------------------------------------------------------
print("\n== 1. extract_state — brace matching, not regex ==")
cases = [
    ('window.__PRELOADED_STATE__ = {"a":1,"b":"}"};x=1;', '{"a":1,"b":"}"}'),
    ('window.__PRELOADED_STATE__={"s":"he said \\"}\\" ok","n":3};z()',
     '{"s":"he said \\"}\\" ok","n":3}'),
    ('window.__PRELOADED_STATE__={"a":{"b":[1,{"c":"\\\\"}]}};more',
     '{"a":{"b":[1,{"c":"\\\\"}]}}'),
    ("nothing here", None),
]
for src, want in cases:
    got = sk_scraper.extract_state(src)
    check(f"extract {src[:34]!r}", got == want, f"got {got!r}")
check("and the extracted text is valid JSON",
      json.loads(sk_scraper.extract_state(cases[0][0]))["b"] == "}")

print("\n== 2. parser ==")
state = json.loads(sk_scraper.extract_state(
    PAGES[f"{SITE}/college/slug-72"].decode()))
col = sk_parse.parse_college(state)
bcs = sk_parse.parse_base_courses(state)
cat = sk_parse.parse_catalogue(state)
check("node found by SHAPE, past the empty `instituteData` decoy",
      col is not None and col["college_id"] == 72, col)
check("contact details read from the real key names",
      (col["website"], col["phone"], col["email"])
      == ("https://archedu.org/", "9414070678", "a@archedu.org"),
      (col["website"], col["phone"], col["email"]))
check("geo coordinates captured",
      (col["latitude"], col["longitude"]) == (26.85, 75.80), col["latitude"])
check("review count is totalCount (24), not the narrower reviewCount (6)",
      col["reviews_count"] == 24, col["reviews_count"])
check("rating rounded to 2dp, not stored at full float precision",
      col["rating"] == 4.52, col["rating"])
check("name, city, state, ownership picked up",
      (col["name"], col["city"], col["state"], col["ownership"])
      == ("Arch College", "Jaipur", "Rajasthan", "Private"), col)
check("HTML entities decoded and tags stripped",
      col["description"] == "Admission is & merit based", col["description"])
check("lists stored as compact JSON",
      json.loads(col["facilities"]) == ["Design Studio"], col["facilities"])
check("2 base courses, NOT 3 — filteredBaseCourseTuples ignored",
      len(bcs) == 2, [b["name"] for b in bcs])
check("no 'DUPLICATE' row leaked in",
      all(b["name"] != "DUPLICATE" for b in bcs))
check("BOTH ids captured: shared base_course_id and per-college page id",
      (bcs[0]["base_course_id"], bcs[0]["course_page_id"]) == (9, 306967), bcs[0])
check("fee range and duration parsed",
      (bcs[0]["min_fees"], bcs[0]["max_fees"], bcs[0]["duration"])
      == (1940000, 1940000, "4 years"), bcs[0])
check("fee range across several courses keeps its course_count",
      bcs[0]["course_count"] == 5, bcs[0]["course_count"])
check("catalogue rows are the shared ids",
      sorted(c["base_course_id"] for c in cat) == [9, 12], cat)
blob = json.dumps(col)
check("NO third-party personal data stored",
      "Anangsha" not in blob and "userprofile" not in blob
      and "workExperience" not in blob)
check("no review text or author fields in the column set",
      not any(k in col for k in ("reviews", "review_text", "users", "authors")))
check("stored size is ~2 KB, not 616 KB",
      len(blob) < 6000, f"{len(blob)} bytes")

print("\n== 3. run_detail, end to end ==")
fresh_db()
job = run()
c = sk_db.counts()
check("job completed", job["status"] == "completed", job["message"])
check("2 colleges got detail (the third has no state)",
      c["colleges_with_detail"] == 2, c)
check("catalogue built from the pages", c["base_courses"] == 2, c)
check("college x base-course rows written",
      c["college_base_courses"] == 3, c)      # 72 has 2, 99 has 1
with sk_db.connect() as conn:
    r72 = dict(conn.execute("SELECT * FROM sk_colleges WHERE college_id=72").fetchone())
    prog = {x[0]: x[1] for x in conn.execute(
        "SELECT college_id, status FROM sk_college_progress")}
    bc9 = dict(conn.execute("SELECT * FROM sk_base_courses WHERE base_course_id=9").fetchone())
check("detail_scraped_at stamped", r72["detail_scraped_at"], r72["detail_scraped_at"])
check("DISCOVERY's columns survived the detail write",
      (r72["slug"], r72["tabs"], r72["alias_count"]) == ("slug-72", "courses,fees", 3),
      (r72["slug"], r72["tabs"], r72["alias_count"]))
check("a page with no state is marked 'gone', not 'error'",
      prog.get(101) == "gone", prog)
check("colleges with state are 'done'",
      prog.get(72) == "done" and prog.get(99) == "done", prog)
check("catalogue colleges_count computed, not guessed",
      bc9["colleges_count"] == 2, bc9["colleges_count"])

print("\n== 4. the queue drains — a second run does no work ==")
REQUESTS.clear()
job2 = run()
check("nothing pending on the second run",
      "nothing pending" in (job2["message"] or ""), job2["message"])
check("and no page was re-fetched",
      not [u for u, _ in REQUESTS if "/college/" in u],
      [u for u, _ in REQUESTS if "/college/" in u])

print("\n== 5. route: direct by default, proxy after a refusal ==")
fresh_db()
install()
job3 = run()
site_reqs = [(u, p) for u, p in REQUESTS if "/college/" in u]
check("every college page went out DIRECT", all(p is None for _, p in site_reqs),
      [p for _, p in site_reqs if p])
check("the job message says which route was used",
      "direct" in (job3["message"] or ""), job3["message"])

fresh_db()
install(refuse_direct=True)
job4 = run()
site_reqs = [(u, p) for u, p in REQUESTS if "/college/" in u]
check("a 403 on direct switches to the proxy",
      any(p for _, p in site_reqs), "nothing went via proxy")
check("the switch is announced in the log",
      any("goes through the proxy" in m for m in LOGS),
      [m for m in LOGS if "⇄" in m])
check("and it is one-way — no request goes direct after the switch",
      [p for _, p in site_reqs][-1] is not None,
      [p for _, p in site_reqs])
check("colleges still landed despite the refusals",
      sk_db.counts()["colleges_with_detail"] == 2, sk_db.counts())

print("\n== 6. a dead tunnel is NOT a site refusal ==")
fresh_db()
install(transport_error=True)
job5 = run()
check("a transport error does not flip the route to proxy",
      not any("goes through the proxy" in m for m in LOGS),
      [m for m in LOGS if "⇄" in m])
install()

print("\n== 7. budget ==")
fresh_db()
job6 = run({"budget_requests": 1})
check("request budget halts the run",
      job6["status"] == "stopped" and "budget" in (job6["message"] or ""),
      job6["message"])

print("\n== 8. mutation tests ==")


def mutate(name, patch, restore, assertion):
    try:
        patch()
        ok = assertion()
    except Exception as e:  # noqa: BLE001
        ok = f"raised {type(e).__name__}: {e}"
    finally:
        restore()
    check(f"MUTANT CAUGHT: {name}", ok is not True, f"mutant survived ({ok})")


# (a) find the node by NAME instead of by shape — the decoy is empty, so this
#     writes nothing and errors nowhere
_real_find = sk_parse.find_node


def _assert_detail_lands():
    """Content, not row count. A parser that finds nothing still produces a row
    with the college_id the caller passed in, so counting rows proves nothing."""
    fresh_db()
    install()
    run()
    with sk_db.connect() as conn:
        r = conn.execute("SELECT name, city FROM sk_colleges "
                         "WHERE college_id=72").fetchone()
    return bool(r) and r[0] == "Arch College" and r[1] == "Jaipur"


mutate("node located by name (`instituteData`) instead of shape",
       lambda: setattr(sk_parse, "find_node",
                       lambda s, d=0: (s or {}).get("instituteData")
                       if isinstance(s, dict) else None),
       lambda: setattr(sk_parse, "find_node", _real_find),
       _assert_detail_lands)

# (b) the route flips back to direct on success -> it oscillates
_real_on_refusal = sk_scraper.Route.on_refusal


def _flapping(self, label):
    _real_on_refusal(self, label)
    self.direct = True          # the bug: reset on the next call
    return True


def _assert_one_way():
    fresh_db()
    install(refuse_direct=True)
    run()
    site = [p for u, p in REQUESTS if "/college/" in u]
    return bool(site) and site[-1] is not None


mutate("route switch is not one-way",
       lambda: setattr(sk_scraper.Route, "on_refusal", _flapping),
       lambda: setattr(sk_scraper.Route, "on_refusal", _real_on_refusal),
       _assert_one_way)

# (c) a stateless page counted as an error -> it never leaves the queue, and the
#     crawl re-fetches it at 171 KB on every run, forever
def _assert_gone_is_terminal():
    fresh_db()
    install()
    run()
    with sk_db.connect() as conn:
        row = conn.execute("SELECT status FROM sk_college_progress "
                           "WHERE college_id=101").fetchone()
    if not row or row[0] != "gone":
        return False
    return len(sk_db.colleges_pending()) == 0


# Mutating the exception CLASS proves nothing — both the raise and the except
# resolve it from the same module global, so the swap is invisible. What matters
# is the status written: 'error' keeps the college in the queue and the crawl
# re-fetches it at 171 KB on every run, forever.
_real_progress = sk_db.set_college_progress


def _gone_as_error(college_id, status, found=0, message="", db_path=sk_db.SK_DB_PATH):
    return _real_progress(college_id, "error" if status == "gone" else status,
                          found, message, db_path)


mutate("a page with no state recorded as 'error', so it never leaves the queue",
       lambda: setattr(sk_db, "set_college_progress", _gone_as_error),
       lambda: setattr(sk_db, "set_college_progress", _real_progress),
       _assert_gone_is_terminal)

# (d) a re-fetch whose page has LOST a field must not blank the stored value.
#     (Discovery's own columns are structurally safe — they are not in
#     COLLEGE_DETAIL_COLS at all — so testing those would prove nothing.)
_real_upsert = sk_db.upsert_college_detail


def _destructive(rows, db_path=sk_db.SK_DB_PATH):
    with sk_db.connect(db_path) as conn:
        return sk_db._upsert(conn, "sk_colleges", sk_db.COLLEGE_DETAIL_COLS,
                             ["college_id"], rows, preserve_nonempty=False)


def _assert_refetch_keeps_city():
    fresh_db()
    install()
    run()
    # the page comes back a second time with the location block gone
    url = f"{SITE}/college/slug-72"
    keep = PAGES[url]
    node = node_for(72, "Arch College", [TUPLE_BDES, TUPLE_MDES])
    node.pop("currentLocation")
    st = {"instituteData": {}, "iulpPageData": node}
    PAGES[url] = ("<script>window.__PRELOADED_STATE__ = " + json.dumps(st)
                  + ";</script>").encode()
    try:
        with sk_db.connect() as conn:
            conn.execute("DELETE FROM sk_college_progress WHERE college_id=72")
        run()
        with sk_db.connect() as conn:
            r = conn.execute("SELECT city FROM sk_colleges "
                             "WHERE college_id=72").fetchone()
        return bool(r) and r[0] == "Jaipur"
    finally:
        PAGES[url] = keep


mutate("a re-fetch missing a field blanks the stored value",
       lambda: setattr(sk_db, "upsert_college_detail", _destructive),
       lambda: setattr(sk_db, "upsert_college_detail", _real_upsert),
       _assert_refetch_keeps_city)

# ---------------------------------------------------------------------------
print("\n" + "=" * 64)
print(f"{len(PASSES)} passed, {len(FAILURES)} failed")
for f in FAILURES:
    print("  FAILED:", f)
shutil.rmtree(_TMP, ignore_errors=True)
sys.exit(1 if FAILURES else 0)
