"""
Shiksha — parse a college page's `__PRELOADED_STATE__` into stored columns.

Pure functions. No network, no database, no globals. That is deliberate: the
parser is the part most likely to be wrong, and keeping it pure means it can be
run against a saved payload (`python sk_parse.py /data/sk_sample_72.json`) and
checked against real data before 57,751 pages are fetched on its say-so.

What it keeps, and what it refuses
----------------------------------
A college page carries 616 KB of state, of which roughly half is navigation and
footer boilerplate identical on every page (`gnbLinks` alone is 186.8 KB). The
extract is ~2 KB per college, and three things are left out ON PURPOSE:

* **`usersInfo`** — review authors' real names, profile URLs, cities, work
  experience and profile-completion scores. Third-party personal data with no
  bearing on comparing colleges. Not collected.
* **reviews** — the owner's standing instruction: *"i dont want reviews as of
  now"*. Aggregate rating and review COUNT are kept; no review text, no authors.
* **`gnbLinks` / `saGnbLinks` / `soGnbLinks` / footers / tracking config** —
  identical on every page and worth nothing per college.

`filteredBaseCourseTuples` is a byte-for-byte duplicate of `baseCourseTuples`
and is ignored.

The two course ids
------------------
Each base-course tuple carries BOTH:

    courseId : 306967   the per-college course page id (the one in the URL)
    id       : 9        "B.Des" — Shiksha's SHARED base-course id

The shared id never appears in a URL, which is why sitemap discovery could not
see it and concluded there was no catalogue. There is one, and it is the join
key the Collegedunia comparison needs.
"""
from __future__ import annotations

BUILD = "2026-09-25a"

import html as _html
import json
import re
import sys
from typing import Any, Dict, List, Optional

# Branches that mark the node holding a college's data. The node's KEY differs
# between page types, so it is found by shape rather than by name — a rename
# upstream then costs nothing, and a wrong guess cannot silently produce empty
# rows.
NODE_MARKERS = ("instituteTopCardData", "baseCourseTuples")

# Caps, tuned against the real payload on 2026-09-28. The first pass stored
# 6.7 KB per college — 379 MB over 57,751, against ~900 MB free on a disk the
# Collegedunia data is still growing into. Most of the excess was prose:
# `description` and `admission_text` came back as near-duplicates of each other,
# and a "highlight" turned out to be a paragraph about institutional memberships.
MAX_TEXT = 1500          # cap on any single stored text field
MAX_LIST = 30            # cap on any stored list
MAX_ITEM = 200           # cap on one item inside a stored list


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def text(v: Any, limit: int = MAX_TEXT) -> str:
    """HTML to plain text: unescape entities, drop tags, collapse whitespace."""
    if v is None:
        return ""
    s = _TAG_RE.sub(" ", str(v))
    s = _html.unescape(s)
    return _WS_RE.sub(" ", s).strip()[:limit]


def as_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(str(v).replace(",", "").strip()))
    except Exception:  # noqa: BLE001
        return None


def as_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(str(v).replace(",", "").strip())
    except Exception:  # noqa: BLE001
        return None


def dget(obj: Any, *keys, default=None):
    """Nested get that tolerates a non-dict anywhere along the path."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return default if cur is None else cur


def jlist(values: List[Any], limit: int = MAX_LIST,
          item_cap: int = MAX_ITEM) -> str:
    """A compact JSON list, capped per item and in length. Empty list stores as
    '' so the preserve_nonempty upsert treats it as absent, not as a value."""
    vals = []
    for v in values:
        if v in (None, ""):
            continue
        vals.append(str(v)[:item_cap] if isinstance(v, str) else v)
        if len(vals) >= limit:
            break
    return json.dumps(vals, ensure_ascii=False, separators=(",", ":")) if vals else ""


def first_text(obj: Any, *keys, limit: int = MAX_TEXT) -> str:
    """The first of several candidate keys that holds something.

    Contact details are a 15-key bag whose spelling is not documented, so the
    parser tries the plausible names rather than betting on one. The CLI prints
    the raw bag, so a key we do not yet know shows up as data instead of as a
    silently empty column."""
    if not isinstance(obj, dict):
        return ""
    for k in keys:
        v = obj.get(k)
        if v not in (None, "", [], {}):
            return text(v, limit)
    return ""


def _same_prose(a: str, b: str, n: int = 60) -> bool:
    """True when two text fields are the same piece of prose.

    60 characters, not 120: on the college sampled, `description` and
    `admissionDetails` share an opening of ~78 characters and then diverge
    ("...UCEED 2027 exami" vs "...Find other lates"). A 120-char window missed
    the duplicate entirely and the drop never fired. Both texts open with the
    college's own name, so 60 characters is still specific enough not to
    collide across colleges."""
    if not a or not b:
        return False
    return a[:n].strip().lower() == b[:n].strip().lower()


def find_node(state: Any, depth: int = 0) -> Optional[Dict[str, Any]]:
    """The dict holding this college's data, located by the branches it carries.

    On the page sampled it sits one level below the root; the root's own
    `instituteData` key is an EMPTY dict, so keying off the obvious name would
    have produced 57,751 blank rows without erroring once."""
    if isinstance(state, dict):
        if any(m in state for m in NODE_MARKERS):
            return state
        if depth < 3:
            for v in state.values():
                got = find_node(v, depth + 1)
                if got is not None:
                    return got
    return None


# ---------------------------------------------------------------------------
# college
# ---------------------------------------------------------------------------
def parse_college(state: Any, college_id: Any = None,
                  job_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """`sk_colleges` columns from one page's state, or None if it is not a
    college page."""
    node = find_node(state)
    if node is None:
        return None

    top = node.get("instituteTopCardData") if isinstance(
        node.get("instituteTopCardData"), dict) else {}
    loc = node.get("currentLocation") if isinstance(
        node.get("currentLocation"), dict) else {}
    contact = loc.get("contact_details") if isinstance(
        loc.get("contact_details"), dict) else {}
    seo = node.get("seoData") if isinstance(node.get("seoData"), dict) else {}
    rev = top.get("reviewDetails") if isinstance(
        top.get("reviewDetails"), dict) else {}

    cid = (as_int(node.get("instituteId")) or as_int(node.get("listingId"))
           or as_int(node.get("sdListingId")) or as_int(college_id))
    if cid is None:
        return None

    # Affiliation and parent university are shaped differently (a list vs a
    # dict), so both are reduced to names rather than stored structurally.
    aff = node.get("instituteTopCardData") or {}
    affiliations = []
    for a in (aff.get("affiliationData") or []):
        if isinstance(a, dict):
            nm = a.get("name") or a.get("universityName") or a.get("displayName")
            if nm:
                affiliations.append(str(nm))
    parent = dget(top, "parentUniversityData", "name") or \
        dget(top, "parentUniversityData", "displayName") or ""

    facilities = [f.get("facilityName") for f in (node.get("facilityInfo") or [])
                  if isinstance(f, dict) and f.get("facilityName")]
    recruiters = [c.get("companyName") for c in (node.get("recruitmentCompanies") or [])
                  if isinstance(c, dict) and c.get("companyName")]
    highlights = []
    for h in dget(node, "highlightsInfo", "highlights", default=[]) or []:
        if isinstance(h, dict):
            k = h.get("title") or h.get("name") or h.get("key") or ""
            v = h.get("value") or h.get("description") or ""
            if k or v:
                highlights.append(f"{text(k, 80)}: {text(v, 160)}".strip(": "))
    streams = [s.get("name") for s in (node.get("streamObjects") or [])
               if isinstance(s, dict) and s.get("name")]
    exams = []
    for e in (node.get("acceptedExams") or []) + \
             (dget(node, "admissionData", "examList", default=[]) or []):
        if isinstance(e, dict):
            nm = e.get("name") or e.get("examName") or e.get("fullName")
            if nm and nm not in exams:
                exams.append(str(nm))

    admission = text(dget(node, "admissionData", "admissionDetails"))
    desc = text(node.get("description"))

    rankings = []
    for r in (top.get("rankingData") or []):
        if isinstance(r, dict):
            rankings.append(text(r.get("rankingString") or r.get("name") or "", 120))

    return {
        "college_id": cid,
        "name": text(top.get("instituteName") or node.get("listingName")
                     or node.get("shortName"), 300),
        "short_name": text(top.get("instituteShortName")
                           or node.get("shortName"), 200),
        "college_type": text(top.get("instituteSpecificationType"), 60),
        "ownership": text(node.get("ownership"), 40),
        "tier": text(node.get("instituteTier"), 40),
        "city": text(loc.get("city_name"), 120),
        "state": text(loc.get("state_name"), 120),
        "city_id": as_int(loc.get("city_id")),
        "state_id": as_int(loc.get("state_id")),
        "locality": text(loc.get("locality_name"), 120),
        "country_id": as_int(node.get("countryId")),
        # Confirmed against the live payload 2026-09-28. The admission contact
        # is preferred over the generic one because the generic pair came back
        # empty on the college sampled; both are tried, in that order.
        "address": first_text(contact, "address", "full_address", limit=600),
        "website": first_text(contact, "website_url", "website", limit=300),
        "phone": first_text(contact, "admission_contact_number",
                            "generic_contact_number", limit=120),
        "email": first_text(contact, "admission_email", "generic_email",
                            limit=200),
        "latitude": as_float(contact.get("latitude")),
        "longitude": as_float(contact.get("longitude")),
        "logo": text(top.get("logoImageUrl"), 400),
        # 4.518181822516701 is false precision — two decimals is all the site
        # itself displays, and it keeps the content hash from churning on
        # floating-point noise between refreshes.
        "rating": (lambda v: round(v, 2) if v is not None else None)(
            as_float(rev.get("averageRating") or rev.get("rating"))),
        # `reviewDetails.totalCount` (24) is the real figure — it matches the
        # page's own "Read 24 Student Reviews". `node.reviewCount` (6) counts
        # something narrower and reading it as the review count understated
        # every college by 4x. Aggregate counts only; no review text, no authors.
        "reviews_count": as_int(rev.get("totalCount")) or as_int(rev.get("count")),
        "reviews_verified": as_int(rev.get("verifiedCount")),
        "questions_count": as_int(node.get("anaCountString")),
        "photo_count": as_int(top.get("photoCount")),
        "video_count": as_int(top.get("videoCount")),
        "base_course_count": as_int(node.get("totalBaseCourseCount")),
        "flagship_course_id": as_int(node.get("flagshipCourseId")),
        "parent_university": text(parent, 300),
        "affiliations": jlist(affiliations),
        "facilities": jlist(facilities),
        "recruiters": jlist(recruiters),
        "highlights": jlist(highlights),
        "streams": jlist(streams),
        "accepted_exams": jlist(exams),
        "rankings": jlist([r for r in rankings if r]),
        "admission_text": admission,
        "admission_updated": text(dget(node, "admissionData", "admissionPostedDate"), 40),
        # `description` and `admissionDetails` come back as near-duplicates —
        # same opening sentence, same content. Storing both doubles the biggest
        # text column for nothing, so the description is dropped when it merely
        # repeats the admission text.
        "description": "" if _same_prose(desc, admission) else desc,
        "meta_title": text(seo.get("metaTitle"), 200),
        "meta_description": text(seo.get("metaDescription"), 400),
        "canonical_url": text(seo.get("canonicalUrl") or node.get("seoUrl"), 400),
        "tab_urls": jlist(sorted((node.get("childPageToUrls") or {}).keys())
                          if isinstance(node.get("childPageToUrls"), dict) else []),
        "detail_scraped_at": None,        # set by the writer, not the parser
        "source_job_id": job_id,
    }


# ---------------------------------------------------------------------------
# base courses
# ---------------------------------------------------------------------------
def _duration(t: Dict[str, Any]) -> str:
    lo, hi = as_int(t.get("minDuration")), as_int(t.get("maxDuration"))
    unit = t.get("minDurationUnit") or t.get("maxDurationUnit") or ""
    if lo is None and hi is None:
        return ""
    if lo == hi or hi is None:
        return f"{lo} {unit}".strip()
    return f"{lo}-{hi} {unit}".strip()


def parse_base_courses(state: Any, college_id: Any = None,
                       job_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """`sk_college_base_courses` rows — the fees table, one per base course.

    `baseCourseTuples` is grouped BY BASE COURSE, so a tuple's fee range can span
    several actual courses (`courseCount` says how many). That is a known
    coarseness, recorded rather than hidden: `course_count` is stored so a
    consumer can tell a single-course range from an eight-course one."""
    node = find_node(state)
    if node is None:
        return []
    cid = (as_int(node.get("instituteId")) or as_int(node.get("listingId"))
           or as_int(college_id))
    out: List[Dict[str, Any]] = []
    seen = set()
    for t in (node.get("baseCourseTuples") or []):
        if not isinstance(t, dict):
            continue
        bid = as_int(t.get("id"))
        if bid is None or (cid, bid) in seen:
            continue
        seen.add((cid, bid))
        exams = [e.get("name") or e.get("examName")
                 for e in (t.get("exams") or []) if isinstance(e, dict)]
        out.append({
            "college_id": cid,
            "base_course_id": bid,
            "name": text(t.get("name"), 200),
            "course_page_id": as_int(t.get("courseId")),
            "level": text(t.get("courseLevel"), 40),
            "min_fees": as_int(t.get("minFees")),
            "max_fees": as_int(t.get("maxFees")),
            "duration": _duration(t),
            "total_seats": as_int(t.get("totalSeats")),
            "course_count": as_int(t.get("courseCount")),
            "rating": as_float(t.get("courseRating")),
            "rating_count": as_int(t.get("ratingCount")),
            "money_rating": as_float(t.get("averageMoneyRating")),
            "placement_rating": as_float(t.get("averagePlacementRating")),
            "min_salary": as_int(t.get("minMedianSalary")),
            "max_salary": as_int(t.get("maxMedianSalary")),
            "eligibility_xii": as_int(t.get("minEligibilityXII")),
            "eligibility_grad": as_int(t.get("minEligibilityGraduation")),
            "scholarships_count": as_int(t.get("scholarshipsCount")),
            "exams": jlist([e for e in exams if e]),
            "ranking": text(t.get("rankingString"), 200),
            "url": text(t.get("url"), 400),
            "source_job_id": job_id,
        })
    return out


def parse_catalogue(state: Any, job_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """`sk_base_courses` rows — the shared catalogue. A few hundred values in
    total across the whole site, accumulated one college at a time."""
    rows, seen = [], set()
    for t in (find_node(state) or {}).get("baseCourseTuples") or []:
        if not isinstance(t, dict):
            continue
        bid = as_int(t.get("id"))
        if bid is None or bid in seen:
            continue
        seen.add(bid)
        rows.append({"base_course_id": bid, "name": text(t.get("name"), 200),
                     "level": text(t.get("courseLevel"), 40),
                     "source_job_id": job_id})
    return rows


def parse_all(state: Any, college_id: Any = None,
              job_id: Optional[int] = None) -> Dict[str, Any]:
    return {"college": parse_college(state, college_id, job_id),
            "base_courses": parse_base_courses(state, college_id, job_id),
            "catalogue": parse_catalogue(state, job_id)}


# ---------------------------------------------------------------------------
# Ⓒ  per-course rows
#
# Where this came from, and why the earlier reading was wrong
# ----------------------------------------------------------
# sk_api_probe.py said the /courses tab carried "the same tuples plus
# totalCourses: 42, pageSize: 12, totalCourseCount: 0 — the actual course list
# is fetched client-side AFTER hydration and is not in the HTML."
#
# That was an inference from a summary field, and it was WRONG. Measured in a
# real browser on 2026-09-28 (college 72, /courses):
#
#   * the page issues ZERO requests to any api host. The only network calls are
#     the document itself, Google Analytics, and a tracking beacon — before and
#     after scrolling the page to the bottom. There is nothing to hydrate from.
#   * __PRELOADED_STATE__.childPageData.courseTuples is a list of 12 REAL course
#     rows — 50 keys each, `fees` a plain integer — and totalCourses is 42.
#     `totalCourseCount: 0` is a filtered-result counter, not the list length;
#     reading it as the list length is what produced the wrong conclusion.
#   * childPageData.paginationData.nextUrls gives /courses-2 … /courses-4.
#     Page 4 returns 6 rows: 12+12+12+6 = 42. The pages are plain server-
#     rendered URLs, so no endpoint has to be found at all.
#   * config.API_SERVER is "apis.shiksha.jsb9.net" — an internal host the
#     browser never calls. There is no public API to piggyback on, which is why
#     the probe's constructed candidates were never going to land.
#
# Grain: these rows are the ACTUAL courses, not the base-course groups in
# sk_college_base_courses. "B.Des" there is one row with a fee range; here it is
# five rows each with its own fee, seats and eligibility.
COURSE_TUPLE_MARKER = "courseTuples"


def find_listing(state: Any, depth: int = 0) -> Optional[Dict[str, Any]]:
    """The node carrying `courseTuples`. Located by shape, like find_node, and
    for the same reason: the obvious key is not where the data lives."""
    if isinstance(state, dict):
        if isinstance(state.get(COURSE_TUPLE_MARKER), list):
            return state
        if depth < 3:
            for v in state.values():
                got = find_listing(v, depth + 1)
                if got is not None:
                    return got
    return None


def _course_duration(t: Dict[str, Any]) -> str:
    lo, hi = as_int(t.get("duration")), as_int(t.get("maxDuration"))
    unit = t.get("durationUnit") or t.get("maxDurationUnit") or ""
    if lo is None and hi is None:
        return ""
    if hi is None or hi == lo:
        return f"{lo} {unit}".strip()
    return f"{lo}-{hi} {unit}".strip()


def parse_course_listing(state: Any, college_id: Any = None,
                         job_id: Optional[int] = None) -> Dict[str, Any]:
    """One /courses (or /courses-N) page → offering rows + the next page paths.

    Returns {'offerings', 'courses', 'next_paths', 'total', 'page'}. `next_paths`
    is what the runner follows; it is taken from the site's own paginationData
    rather than constructed, so a college whose pagination stops early stops
    here too instead of being probed for pages that do not exist."""
    node = find_listing(state)
    if node is None:
        return {"offerings": [], "courses": [], "next_paths": [],
                "total": None, "page": None}
    # `listingId` first. The obvious-looking instituteTopCardData.instituteId
    # does NOT exist — checked against NIT Trichy's listing, whose top card
    # carries instituteName, h1, logoImageUrl … and no id at all. Reading it
    # first worked only because the fallback caught it; leaving it there would
    # have been a lookup that never fires pretending to be the primary source.
    cid = as_int(node.get("listingId")) or as_int(college_id)
    offerings: List[Dict[str, Any]] = []
    courses: List[Dict[str, Any]] = []
    seen = set()
    for t in (node.get(COURSE_TUPLE_MARKER) or []):
        if not isinstance(t, dict):
            continue
        course_id = as_int(t.get("courseId"))
        # A tuple carrying another college's id is a cross-sell card, not an
        # offering of THIS college. Dropping it here keeps the edge table
        # honest; the listing page does carry such cards.
        row_cid = as_int(t.get("instituteId")) or cid
        if course_id is None or row_cid != cid or (cid, course_id) in seen:
            continue
        seen.add((cid, course_id))
        exams = [e.get("name") or e.get("examName")
                 for e in (t.get("exams") or []) if isinstance(e, dict)]
        skills = [s.get("name") if isinstance(s, dict) else s
                  for s in (t.get("skills") or [])]
        url = text(t.get("url"), 400)
        offerings.append({
            "college_id": cid,
            "course_id": course_id,
            "url": url,
            "course_slug": url.rsplit("/", 1)[-1] if url else "",
            "course_name": text(t.get("name"), 300),
            "fees_amount": as_int(t.get("fees")),
            "fees_text": "",
            "duration": _course_duration(t),
            "level": text(dget(t, "courseLevel", "name")
                          or t.get("courseLevel"), 40),
            "exams": jlist([e for e in exams if e]),
            "base_course_id": as_int(t.get("baseCourseId")),
            "base_course_name": text(t.get("baseCourseName"), 200),
            "total_seats": as_int(t.get("totalSeats")),
            "median_salary": as_int(t.get("courseMedianSalary")),
            "rating": as_float(t.get("courseRating")),
            "rating_count": as_int(t.get("ratingCount")),
            "money_rating": as_float(t.get("moneyRating")),
            "scholarships_count": as_int(t.get("scholarshipsCount")),
            # Percentages, not prose: the live tuple carries eligibilityXII=50
            # and eligibilityGraduation=0. Stored as integers, matching
            # sk_college_base_courses, so the two grains stay comparable.
            "eligibility_x": as_int(t.get("eligibilityX")),
            "eligibility_xii": as_int(t.get("eligibilityXII")),
            "eligibility_grad": as_int(t.get("eligibilityGraduation")),
            "eligibility_pg": as_int(t.get("eligibilityPostGraduation")),
            "work_experience": text(t.get("workExpString"), 200),
            "difficulty_level": text(t.get("difficultyLevel"), 60),
            "skills": jlist([s for s in skills if s]),
            "credential": text(dget(t, "credential", "name")
                               or t.get("credential"), 60),
            "is_online": 1 if t.get("isOnline") else 0,
            "curriculum_pdf": text(t.get("curricullumPDF"), 400),
            "intake_dates": jlist(t.get("intakeDates") or []),
            "commencement_dates": jlist(t.get("courseCommencementDates") or []),
            "admission_status": text(t.get("courseAdmissionStatus"), 60),
            "shiksha_rank": as_int(t.get("shikshaRank")),
            "exams_count": as_int(t.get("totalExamsCount")),
            "institute_grade": text(t.get("instituteGrade"), 40),
            "ranking": text(t.get("rankingString"), 200),
            "source_job_id": job_id,
        })
        courses.append({
            "course_id": course_id,
            "slug": url.rsplit("/", 1)[-1] if url else "",
            "name": text(t.get("name"), 300),
            "level": text(dget(t, "courseLevel", "name") or t.get("courseLevel"), 40),
            "duration": _course_duration(t),
            "source_job_id": job_id,
        })
    pag = node.get("paginationData") or {}
    nxt = [text(u.get("url"), 400) for u in (pag.get("nextUrls") or [])
           if isinstance(u, dict) and u.get("url")]
    return {"offerings": offerings, "courses": courses, "next_paths": nxt,
            "total": as_int(node.get("totalCourses")),
            "page": as_int(pag.get("currentPageNUmber"))}


def parse_course_detail(state: Any, job_id: Optional[int] = None
                        ) -> Optional[Dict[str, Any]]:
    """One /course-<slug>-<id> page → the fields the LISTING cannot give.

    Measured on college 72 / course 306967: the listing has no specialization,
    no course level, no delivery method and only a single `fees` number, while
    courseData here carries specializationName ("Fashion Design"),
    entryCourseTypeInformation.hierarchy[].specialization_id (183),
    course_level ({id:14,name:'UG'}), credential, educationType ("Full Time"),
    deliveryMethod ("Classroom"), mediumOfInstruction, and a fee BREAKDOWN
    (totalFees 19,40,000 + oneTimePayment 75,000, with a currency and a year).

    This costs one request per course — 144 KB measured — so it is a separate
    pass, not part of the listing crawl."""
    cd = state.get("courseData") if isinstance(state, dict) else None
    if not isinstance(cd, dict) or as_int(cd.get("courseId")) is None:
        return None
    ent = cd.get("entryCourseTypeInformation") or {}
    hier = (ent.get("hierarchy") or [{}])[0] if isinstance(ent, dict) else {}
    hier = hier if isinstance(hier, dict) else {}
    fees = cd.get("courseFees") or {}
    fblock = dget(fees, "fees", default={}) or {}
    elig = cd.get("eligibility") or {}
    xii = elig.get("twelthDetails") or {}
    struct = cd.get("courseStructure") or {}
    seats = cd.get("seatsData") or {}
    plc = cd.get("placements") or {}
    aff = cd.get("affiliationData") or {}
    media = [m.get("name") if isinstance(m, dict) else m
             for m in (cd.get("mediumOfInstruction") or [])]
    return {
        "college_id": as_int(cd.get("instituteId")),
        "course_id": as_int(cd.get("courseId")),
        "course_name": text(cd.get("courseName"), 300),
        "base_course_name": text(cd.get("baseCourseName"), 200),
        # ---- taxonomy -----------------------------------------------------
        "specialization": text(cd.get("specializationName"), 200),
        "specialization_id": as_int(hier.get("specialization_id")),
        "substream": text(cd.get("substreamName"), 200),
        "substream_id": as_int(hier.get("substream_id")),
        "stream_id": as_int(hier.get("stream_id")),
        "course_level": text(dget(ent, "course_level", "name"), 40),
        "credential": text(dget(ent, "credential", "name"), 60),
        "education_type": text(dget(cd, "educationType", "name"), 60),
        "delivery_method": text(dget(cd, "deliveryMethod", "name"), 60),
        "medium": jlist([m for m in media if m]),
        "duration": (f"{as_int(cd.get('durationValue'))} "
                     f"{cd.get('durationUnit') or ''}".strip()
                     if as_int(cd.get("durationValue")) is not None else ""),
        "course_type": text(cd.get("courseType"), 60),
        "institute_type": text(cd.get("instituteType"), 60),
        "course_variant": as_int(cd.get("courseVariant")),
        "is_course_paid": 1 if cd.get("isCoursePaid") else 0,
        "nzqf": text(dget(cd, "nzqfCategorization", "name"), 120),
        # ---- fees ---------------------------------------------------------
        # Every money figure is a CATEGORY-KEYED map ({general: {value,…}}), so
        # the map is stored whole and `general` is broken out for querying.
        # Measured across a private design college, a private B-school and NIT
        # Trichy: only `general` is ever populated — and NIT's own description
        # says that one number covers "OPEN/OPEN-PWD/OPEN-EWS/OBC-NCL/OBC-PWD/
        # ICCR/DASA(CIWG)". The prose is therefore not decoration, it is the
        # qualification the number lacks, which is why every *_note is stored.
        "fees_total": as_int(dget(fblock, "totalFees", "general", "value")),
        "fees_onetime": as_int(dget(fblock, "oneTimePayment", "general", "value")),
        "fees_hostel": as_int(dget(fblock, "hostelFees", "general", "value")),
        "fees_deposit": as_int(dget(fblock, "deposit", "general", "value")),
        "fees_other": as_int(fblock.get("otherFees")),
        "fees_total_json": _jmap(fblock.get("totalFees")),
        "fees_onetime_json": _jmap(fblock.get("oneTimePayment")),
        "fees_hostel_json": _jmap(fblock.get("hostelFees")),
        "fees_deposit_json": _jmap(fblock.get("deposit")),
        "fees_period_json": _jmap(fblock.get("fees")),
        "fees_period_type": text(fblock.get("periodType"), 40),
        "fees_includes": jlist([t_ for t_ in (fblock.get("totalIncludes") or [])
                                if t_]),
        "fees_categories": _jmap(fees.get("categoryNameMapping")),
        "fees_location_json": _jmap(fees.get("locationWiseFees")),
        "fees_year": as_int(fees.get("year")),
        "fees_currency": text(fees.get("feesUnitName"), 8),
        "fees_note": text(fees.get("description"), 600),
        "fees_hostel_note": text(fees.get("hostelDescription"), 600),
        "fees_onetime_note": text(fees.get("otpDescription"), 600),
        "fees_deposit_note": text(fees.get("depositDescription"), 600),
        "fees_brochure_url": text(fees.get("feesBrochureUrl"), 400),
        # ---- eligibility --------------------------------------------------
        # categoryWiseScores DOES populate: NIT Trichy asks 75% general, 65% SC,
        # 65% ST. So the category-keyed shape is real and used — it is the fee
        # maps specifically that collapse to `general`, not the schema.
        "elig_year": as_int(elig.get("year")),
        "elig_x_json": _jmap(elig.get("tenthDetails")),
        "elig_xii_json": _jmap(xii),
        "elig_grad_json": _jmap(elig.get("graduationDetails")),
        "elig_pg_json": _jmap(elig.get("postGraduationDetails")),
        "elig_xii_general": as_int(dget(xii, "categoryWiseScores", "general",
                                        "score")),
        "elig_xii_scores": _jmap(xii.get("categoryWiseScores")),
        "elig_xii_score_type": text(xii.get("scoreType"), 40),
        "elig_exams_json": _exams(elig.get("exams")),
        "elig_min_workex": as_int(elig.get("minWorkEx")),
        "elig_max_workex": as_int(elig.get("maxWorkEx")),
        "elig_min_age": as_int(elig.get("minAge")),
        "elig_max_age": as_int(elig.get("maxAge")),
        "elig_backlogs": as_int(elig.get("numberofBacklog")),
        "elig_note": text(elig.get("description"), 2000),
        "elig_intl_note": text(elig.get("internationalDescription"), 1000),
        # ---- structure, admission, seats ----------------------------------
        "curriculum_pdf": text(struct.get("curriculumPdfUrl"), 400),
        "course_period": text(struct.get("period"), 60),
        "period_courses_json": _jmap(struct.get("periodWiseCourses")),
        "admission_steps": _steps(cd.get("admissionProcess")),
        "seats_total": as_int(seats.get("totalSeats")),
        "seats_category_json": _jmap(seats.get("categoryWiseSeats")),
        "seats_exam_json": _jmap(seats.get("examWiseSeats")),
        "seats_domicile_json": _jmap(seats.get("domicileWiseSeats")),
        # ---- placements ---------------------------------------------------
        # `course_type` says WHAT the figure describes. At college 72 it is
        # "substreamId" — the salary belongs to the substream, not to this one
        # course. Storing the grain beside the number is the difference between
        # a usable figure and a misleading one.
        "placement_grain": text(plc.get("course_type"), 40),
        "placement_batch_year": as_int(plc.get("batch_year")),
        "placement_pct": as_float(plc.get("percentage_batch_placed")),
        "salary_avg": as_int(plc.get("avg_salary")),
        "salary_median": as_int(plc.get("median_salary")),
        "salary_max": as_int(plc.get("max_salary")),
        "salary_min": as_int(plc.get("min_salary")),
        "salary_currency": text(plc.get("salary_unit_name"), 8),
        "placement_report_url": text(plc.get("report_url"), 400),
        "internships_available": (1 if plc.get("is_internship_available")
                                  else (0 if plc.get("is_internship_available")
                                        is not None else None)),
        "recruiters": jlist([r.get("companyName") for r
                             in (cd.get("recruitmentCompanies") or [])
                             if isinstance(r, dict) and r.get("companyName")],
                            limit=40),
        # ---- affiliation, highlights, dates --------------------------------
        "affiliation_university_id": as_int(aff.get("universityId")),
        "affiliation_name": text(aff.get("name"), 200),
        "affiliation_url": text(aff.get("url"), 400),
        "affiliation_scope": text(aff.get("scope"), 40),
        "highlights": jlist([h.get("description") if isinstance(h, dict) else h
                             for h in (cd.get("highlights") or [])], limit=20,
                            item_cap=400),
        "important_dates_json": _dates(cd.get("importantDates")),
        "source_job_id": job_id,
    }


def _jmap(o: Any, limit: int = 4000) -> str:
    """Store a nested block whole, or '' when it is empty.

    '' rather than '{}' on purpose: the upsert preserves a stored value when the
    incoming one is blank, so an empty block must read as "nothing to say" and
    not overwrite what an earlier run captured."""
    if o in (None, {}, []):
        return ""
    try:
        return json.dumps(o, ensure_ascii=False, separators=(",", ":"))[:limit]
    except Exception:  # noqa: BLE001
        return ""


def _exams(exams: Any) -> str:
    """The accepted-exam block, keyed `exam:<id>`, reduced to what is useful:
    the exam, and the cutoffs it carries per category."""
    if not isinstance(exams, dict):
        return ""
    out = []
    for v in exams.values():
        if not isinstance(v, dict):
            continue
        out.append({"id": as_int(v.get("examId")),
                    "name": text(v.get("examName"), 120),
                    "scoreType": text(v.get("scoreType"), 40),
                    "scores": v.get("categoryWiseScores") or {},
                    "cutoff": v.get("cutOffData") or v.get("cutoff") or {}})
    return _jmap(out)


def _steps(proc: Any) -> str:
    """`admissionProcess` is keyed "1","2","3" — an ORDERED list wearing a dict.
    Sorting numerically keeps the steps in the order a candidate follows."""
    if not isinstance(proc, dict):
        return ""
    try:
        keys = sorted(proc, key=lambda k: int(k))
    except Exception:  # noqa: BLE001
        keys = sorted(proc)
    out = [{"step": k, "name": text(proc[k].get("admissionName"), 120),
            "description": text(proc[k].get("description"), 800)}
           for k in keys if isinstance(proc.get(k), dict)]
    return _jmap(out)


def _dates(blk: Any) -> str:
    """The dated events, flattened out of `entityWiseDates`'s bucket keys."""
    if not isinstance(blk, dict):
        return ""
    out = []
    for bucket in (blk.get("entityWiseDates") or {}).values():
        for ev in (bucket or []):
            if not isinstance(ev, dict):
                continue
            out.append({"name": text(ev.get("eventName"), 160),
                        "type": text(ev.get("type"), 40),
                        "examId": as_int(ev.get("examId")),
                        "start": [as_int(ev.get("startYear")),
                                  as_int(ev.get("startMonth")),
                                  as_int(ev.get("startDate"))],
                        "end": [as_int(ev.get("endYear")),
                                as_int(ev.get("endMonth")),
                                as_int(ev.get("endDate"))]})
    return _jmap(out)


# ---------------------------------------------------------------------------
# CLI — check the parser against a real saved payload before trusting it
# ---------------------------------------------------------------------------
def _main(path: str) -> int:
    with open(path, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    got = parse_all(state)
    col, bcs, cat = got["college"], got["base_courses"], got["catalogue"]
    if col is None:
        print("NO COLLEGE NODE FOUND — the parser would write nothing.")
        print("top-level keys:", list(state)[:20] if isinstance(state, dict) else "?")
        return 1
    filled = [(k, v) for k, v in col.items() if v not in (None, "")]
    empty = [k for k, v in col.items() if v in (None, "")]
    print(f"college row: {len(filled)}/{len(col)} fields filled, "
          f"{len(json.dumps(col, ensure_ascii=False))/1024:,.1f} KB")
    for k, v in filled:
        s = str(v).replace("\n", " ")
        print(f"   {k:<22} {s[:96]}")
    print(f"\n   EMPTY ({len(empty)}): {', '.join(empty)}")
    print(f"\nbase courses: {len(bcs)}")
    for b in bcs[:8]:
        print(f"   [{b['base_course_id']:>5}] {b['name'][:28]:<28} "
              f"fees {b['min_fees']}–{b['max_fees']}  {b['duration']:<10} "
              f"{b['level']:<4} seats={b['total_seats']} covers={b['course_count']} "
              f"exams={b['exams'][:40]}")
    node = find_node(state) or {}
    contact = dget(node, "currentLocation", "contact_details", default={})
    print(f"\nraw contact_details keys: {sorted(contact) if isinstance(contact, dict) else contact}")
    if isinstance(contact, dict):
        for k, v in list(contact.items())[:15]:
            print(f"   {k:<22} {str(v)[:70]}")
    rev = dget(node, "instituteTopCardData", "reviewDetails", default={})
    print(f"raw reviewDetails: {rev}")

    nofee = [b for b in bcs if b["min_fees"] is None and b["max_fees"] is None]
    print(f"\nbase courses with NO fee range: {len(nofee)}/{len(bcs)}"
          + (f"  ({', '.join(b['name'] for b in nofee)})" if nofee else ""))
    wide = [b for b in bcs if (b.get("course_count") or 0) > 3]
    if wide:
        names = ", ".join("{}x{}".format(b["name"], b["course_count"])
                          for b in wide)
        print("base courses covering >3 actual courses: "
              "{}/{}  ({})".format(len(wide), len(bcs), names))
    else:
        print("base courses covering >3 actual courses: 0/{}".format(len(bcs)))

    print(f"\ncatalogue rows: {len(cat)} -> "
          f"{[(c['base_course_id'], c['name']) for c in cat][:10]}")
    def _values_bytes(d):
        return sum(len(str(v)) for v in d.values() if v not in (None, ""))

    total = _values_bytes(col) + sum(_values_bytes(b) for b in bcs)
    with_keys = len(json.dumps(col, ensure_ascii=False)) + \
        sum(len(json.dumps(b, ensure_ascii=False)) for b in bcs)
    print(f"\nstored size for this college: {total/1024:,.1f} KB of VALUES "
          f"-> {total*57751/1024/1024:,.0f} MB for 57,751 colleges")
    print(f"   (as JSON with key names it is {with_keys/1024:,.1f} KB, but "
          f"SQLite stores columns, not key names — the earlier 378 MB figure "
          f"was that inflated number.)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python sk_parse.py /data/sk_sample_72.json")
        sys.exit(2)
    sys.exit(_main(sys.argv[1]))
