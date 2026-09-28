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


def _same_prose(a: str, b: str, n: int = 120) -> bool:
    """True when two text fields are the same piece of prose."""
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
        "address": first_text(contact, "address", "full_address", "address1",
                              "street_address", "addressLine1", limit=600),
        "website": first_text(contact, "website", "web_site", "websiteUrl",
                              "url", "site_url", "homepage", "web", limit=300),
        "phone": first_text(contact, "phone", "phone_no", "phoneNumber",
                            "mobile", "contact_no", "contactNumber", "telephone",
                            "landline", limit=120),
        "email": first_text(contact, "email", "email_id", "emailId",
                            "email_address", limit=200),
        "logo": text(top.get("logoImageUrl"), 400),
        # 4.518181822516701 is false precision — two decimals is all the site
        # itself displays, and it keeps the content hash from churning on
        # floating-point noise between refreshes.
        "rating": (lambda v: round(v, 2) if v is not None else None)(
            as_float(rev.get("rating") or rev.get("averageRating"))),
        # The two counts disagree (`reviewCount` 6 vs "24 Student Reviews" in the
        # meta description), so keep the larger: a count that undercounts is
        # worse than one that includes every review type.
        "reviews_count": max([x for x in (as_int(rev.get("count")),
                                          as_int(rev.get("reviewCount")),
                                          as_int(node.get("reviewCount")))
                              if x is not None] or [None],
                             key=lambda x: (x is not None, x)),
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
    total = len(json.dumps(col, ensure_ascii=False)) + \
        sum(len(json.dumps(b, ensure_ascii=False)) for b in bcs)
    print(f"\nstored size for this college: {total/1024:,.1f} KB "
          f"-> {total*57751/1024/1024:,.0f} MB for 57,751 colleges")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python sk_parse.py /data/sk_sample_72.json")
        sys.exit(2)
    sys.exit(_main(sys.argv[1]))
