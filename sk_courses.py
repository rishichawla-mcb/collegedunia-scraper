"""
Shiksha phase Ⓒ — the per-course rows, and phase Ⓓ — per-course detail.

What this fixes
---------------
`sk_offerings` held 317,907 college x course edges whose `fees_amount`,
`duration`, `level` and `exams` columns were shaped for real data and were
EMPTY, because nothing we fetched carried per-course values. Phase Ⓑ fetches the
college home page, whose `baseCourseTuples` are grouped by BASE course: "B.Des"
is one row with a fee RANGE that can span eight actual courses.

`sk_api_probe.py` concluded the real list was "fetched client-side AFTER
hydration and is not in the HTML", and went looking for an endpoint. That was an
inference from `totalCourseCount: 0`, and it was wrong. Measured in a real
browser on 2026-09-28 against college 72:

  * loading /courses and scrolling it to the bottom produces ZERO requests to
    any api host — only the document, Google Analytics and a tracking beacon.
    There is nothing to hydrate from, so there was never an endpoint to find.
  * __PRELOADED_STATE__.childPageData.courseTuples already holds 12 real course
    rows, 50 keys each, `fees` a plain integer. `totalCourses` is 42;
    `totalCourseCount` is a filtered counter and is 0 on an unfiltered page.
  * paginationData.nextUrls gives /courses-2, /courses-3, /courses-4, and page 4
    returns the last 6 rows. 12+12+12+6 = 42.
  * config.API_SERVER is "apis.shiksha.jsb9.net" — internal, never called by the
    browser. The probe's constructed apis.shiksha.com candidates could not have
    worked.

So phase Ⓒ needs no endpoint: it walks ⌈courses/12⌉ ordinary pages per college.

Two passes, priced separately
-----------------------------
Ⓒ listing   /college/<slug>/courses[-N]   ~26,500 pages for 317,907 courses
            Gives fees, duration, seats, exams, eligibility, salary, skills,
            credential, ranking, admission status — and the per-course URL.
            Measured 155 KB/page → ≈3.8 GB.

Ⓓ deep      /college/<slug>/course-<cslug>-<id>   one page PER COURSE
            Adds what the listing has not got: specializationName,
            specialization/substream/stream ids, course_level, educationType,
            deliveryMethod, mediumOfInstruction, and the fee BREAKDOWN
            (totalFees + oneTimePayment, with currency and year).
            Measured 144 KB/page → ≈45.8 GB for all 317,907. Off by default.

Run Ⓒ first — Ⓓ's URL comes from it.
"""
from __future__ import annotations

BUILD = "2026-09-28a"

import queue as _queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import sk_db
import sk_parse
from sk_scraper import (SITE, Route, _client, _proxy_cfg, NoStateError,
                        ParseEmptyError, fetch_college_state)
from scraper import AdaptiveDelay, PageGoneError, ProxyManager, Stats

# Measured, not assumed. Browser navigation timing on 2026-09-28, college 72.
KB_PER_LISTING_PAGE = 155.0
KB_PER_COURSE_PAGE = 144.0
PAGE_SIZE = 12          # childPageData.pageSize


def _listing_url(col: Dict[str, Any], page: int = 1) -> str:
    base = col.get("url") or f"{SITE}/college/{col.get('slug')}-{col['college_id']}"
    base = base.rstrip("/")
    return f"{base}/courses" if page <= 1 else f"{base}/courses-{page}"


def _abs(path: str) -> str:
    return path if path.startswith("http") else SITE + path


def _budget_guard(stats, budget_requests: int, budget_bytes: int):
    def check() -> Optional[str]:
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs:,})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:,.0f} MB)"
        return None
    return check


# ---------------------------------------------------------------------------
# Ⓒ  listing
# ---------------------------------------------------------------------------
def run_course_listing(job_id: int, cfg: Dict[str, Any],
                       log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    merged = {**_proxy_cfg(), **cfg}
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    concurrency = max(1, int(merged.get("concurrency", 4)))
    delay = float(merged.get("delay", 1.0))
    max_colleges = int(merged.get("max_colleges", 0))
    # A backstop against a runaway, nothing else. It was 25, written on the
    # assumption that "the busiest college in the database has nowhere near
    # 300" courses. That assumption was wrong and the first live run proved it:
    # colleges 36321, 60197 and 182205 advertise 444, 473 and 319 courses and
    # all three stopped at exactly 25 pages, keeping 300 and silently dropping
    # the rest — while being recorded `done`.
    #
    # Measured afterwards: the busiest college holds 473 courses (40 pages), and
    # only 3 colleges exceed 300. 100 pages is 1,200 courses, ~2.5x the largest
    # real case. The walk is ended by the site's own paginationData; this only
    # stops a loop, and the seen-URL guard below already covers the loop the
    # site could cause. So the cap should be far out of the way of real data,
    # which is the mistake the first value taught.
    max_pages = max(1, int(merged.get("max_pages", 100)))
    order = str(merged.get("order", "value"))
    budget_check = _budget_guard(stats, int(merged.get("budget_requests", 0)),
                                 int(float(merged.get("budget_mb", 0)) * 1048576))

    route = Route(pm, log, start_direct=bool(merged.get("start_direct", True)))
    sk_db.init_db()
    sk_db.update_job(job_id, status="running", message="building the queue…")
    log(f"Shiksha · Phase Ⓒ course listing [BUILD {BUILD}] "
        f"concurrency={concurrency} route={route.describe()}")

    pending = sk_db.colleges_pending_courses(limit=max_colleges, order=order)
    total = len(pending)
    known = sk_db.counts().get("offerings", 0)
    est_pages = max(total, int(known / PAGE_SIZE) + total)
    sk_db.update_job(job_id, total_units=total,
                     message=f"{total:,} colleges to list")
    log(f"  {total:,} colleges pending · ≈{est_pages:,} pages · "
        f"≈{est_pages*KB_PER_LISTING_PAGE/1048576:,.2f} GB at the measured "
        f"{KB_PER_LISTING_PAGE:.0f} KB/page")
    if not total:
        sk_db.update_job(job_id, status="completed", finished_at=time.time(),
                         message="nothing pending — every college already listed")
        log("  nothing pending. Phase Ⓑ must mark a college 'done' before "
            "phase Ⓒ will ask for its courses.")
        return

    q: "_queue.Queue" = _queue.Queue()
    for c in pending:
        q.put(c)
    stop = threading.Event()
    lock = threading.Lock()
    counts = {"done": 0, "colleges": 0, "rows": 0, "pages": 0,
              "gone": 0, "error": 0, "short": 0, "redirect": 0}
    halt = {"reason": None}

    def push():
        reqs, byts, _ = stats.snapshot()
        with lock:
            s = dict(counts)
        sk_db.update_job(job_id, done_units=s["done"], items_written=s["rows"],
                         req_count=reqs, bytes_count=byts,
                         message=f"{s['done']:,}/{total:,} colleges · "
                                 f"{s['rows']:,} course rows · "
                                 f"{s['pages']:,} pages · "
                                 f"{byts/1048576:,.0f} MB · {route.describe()}")

    def worker(idx: int):
        client = _client(pm, merged, log, stats, adaptive)
        while not stop.is_set():
            try:
                col = q.get_nowait()
            except _queue.Empty:
                return
            if sk_db.stop_requested(job_id):
                halt["reason"] = "stopped by user"
                stop.set()
                return
            bh = budget_check()
            if bh:
                log(f"  ⏸ {bh}")
                halt["reason"] = bh
                stop.set()
                return
            cid = col["college_id"]
            client.session_id = f"skc{idx}_{cid}"
            rows: List[Dict[str, Any]] = []
            cat: Dict[int, Dict[str, Any]] = {}
            expected: Optional[int] = None
            pages = 0
            url = _listing_url(col)
            try:
                seen_urls = set()
                while url and pages < max_pages:
                    if url in seen_urls:
                        break            # paginationData pointing at itself
                    seen_urls.add(url)
                    state = fetch_college_state(client, url, cid, route=route)
                    got = sk_parse.parse_course_listing(state, cid, job_id)
                    pages += 1
                    if expected is None:
                        expected = got["total"]
                    rows.extend(got["offerings"])
                    for c_ in got["courses"]:
                        cat[c_["course_id"]] = c_
                    nxt = got["next_paths"]
                    url = _abs(nxt[0]) if nxt else ""
                    if delay:
                        time.sleep(adaptive.value() if adaptive else delay)
                # The mutation-testing lesson from phase Ⓑ, applied here: a page
                # that fetched fine but parsed to nothing is OUR failure, not the
                # site's. Recording it 'done' would bury a parser bug under a
                # table of empty rows, so it is an error and stays in the queue.
                if pages and not rows and (expected or 0) > 0:
                    raise ParseEmptyError(
                        f"college {cid}: {expected} courses advertised, 0 parsed "
                        f"from {pages} page(s). Not marking it done.")
                # A page whose own `listingId` is not the college we asked for is
                # a redirect — expected, since discovery found ~26k alias slugs
                # resolving to fewer ids. The rows are written under the id the
                # PAGE claims (the alternative corrupts the edge table), and the
                # redirect is recorded rather than passed over in silence.
                landed = {r["college_id"] for r in rows if r.get("college_id")}
                redirect = landed and cid not in landed
                now = time.time()
                for r in rows:
                    r["listed_at"] = now
                    r["scraped_at"] = now
                if cat:
                    sk_db.upsert_courses(list(cat.values()))
                if rows:
                    sk_db.upsert_offering_listing(rows)
                # 'short' is recorded, not hidden: if the site says 42 and we
                # wrote 30, that is a fact a later audit needs to see.
                short = expected is not None and len(rows) < expected
                # Short because WE stopped, or short because the site's own
                # count disagrees with what it served? Only the first is our
                # bug, and only the first must stay in the queue. Recording a
                # truncated college as `done` is how three colleges lost 144,
                # 173 and 19 courses apiece on the first live run and left the
                # queue for good.
                if short and pages >= max_pages:
                    raise ParseEmptyError(
                        f"college {cid}: stopped at the {max_pages}-page cap "
                        f"with {len(rows)} of {expected} courses. Not marking "
                        f"it done — raise max_pages and re-run.")
                notes = []
                if short:
                    notes.append(f"{len(rows)} of {expected} advertised")
                if redirect:
                    notes.append("redirected to "
                                 + ",".join(str(x) for x in sorted(landed)))
                sk_db.set_course_progress(
                    cid, "done", pages=pages, found=len(rows), expected=expected,
                    message="; ".join(notes))
                with lock:
                    counts["colleges"] += 1
                    counts["rows"] += len(rows)
                    counts["pages"] += pages
                    if short:
                        counts["short"] += 1
                    if redirect:
                        counts["redirect"] += 1
            except (NoStateError, PageGoneError) as err:
                sk_db.set_course_progress(cid, "gone", pages=pages,
                                          message=str(err)[:300])
                with lock:
                    counts["gone"] += 1
            except Exception as err:  # noqa: BLE001
                sk_db.set_course_progress(cid, "error", pages=pages,
                                          message=str(err)[:300])
                with lock:
                    counts["error"] += 1
                log(f"  ! college {cid} courses failed: {str(err)[:160]}")
            with lock:
                counts["done"] += 1
                n = counts["done"]
            if n % 25 == 0:
                push()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    sk_db.recount_course_colleges()
    push()
    reqs, byts, _ = stats.snapshot()
    msg = (f"{halt['reason'] + ' — ' if halt['reason'] else ''}"
           f"phase Ⓒ: {counts['rows']:,} course rows across "
           f"{counts['colleges']:,} colleges ({counts['pages']:,} pages) · "
           f"{counts['short']:,} short of the advertised count, "
           f"{counts['redirect']:,} redirected · "
           f"{counts['gone']:,} gone, {counts['error']:,} errors · "
           f"{byts/1048576:,.0f} MB via {route.describe()}")
    sk_db.update_job(job_id, status="stopped" if halt["reason"] else "completed",
                     message=msg, finished_at=time.time())
    log(msg)


# ---------------------------------------------------------------------------
# Ⓓ  per-course detail — specialization, level, delivery, fee breakdown
# ---------------------------------------------------------------------------
def run_course_detail(job_id: int, cfg: Dict[str, Any],
                      log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    merged = {**_proxy_cfg(), **cfg}
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    concurrency = max(1, int(merged.get("concurrency", 4)))
    delay = float(merged.get("delay", 1.0))
    max_courses = int(merged.get("max_courses", 0))
    budget_check = _budget_guard(stats, int(merged.get("budget_requests", 0)),
                                 int(float(merged.get("budget_mb", 0)) * 1048576))

    route = Route(pm, log, start_direct=bool(merged.get("start_direct", True)))
    sk_db.init_db()
    sk_db.update_job(job_id, status="running", message="building the queue…")
    log(f"Shiksha · Phase Ⓓ course detail [BUILD {BUILD}] "
        f"concurrency={concurrency} route={route.describe()}")

    pending = sk_db.offerings_pending_deep(limit=max_courses)
    total = len(pending)
    sk_db.update_job(job_id, total_units=total,
                     message=f"{total:,} courses to fetch")
    log(f"  {total:,} courses pending · "
        f"≈{total*KB_PER_COURSE_PAGE/1048576:,.2f} GB at the measured "
        f"{KB_PER_COURSE_PAGE:.0f} KB each")
    if not total:
        sk_db.update_job(job_id, status="completed", finished_at=time.time(),
                         message="nothing pending — run phase Ⓒ first, it is "
                                 "what supplies the per-course URL")
        log("  nothing pending")
        return

    q: "_queue.Queue" = _queue.Queue()
    for c in pending:
        q.put(c)
    stop = threading.Event()
    lock = threading.Lock()
    counts = {"done": 0, "rows": 0, "spec": 0, "gone": 0, "error": 0}
    halt = {"reason": None}

    def push():
        reqs, byts, _ = stats.snapshot()
        with lock:
            s = dict(counts)
        sk_db.update_job(job_id, done_units=s["done"], items_written=s["rows"],
                         req_count=reqs, bytes_count=byts,
                         message=f"{s['done']:,}/{total:,} courses · "
                                 f"{s['spec']:,} with a specialization · "
                                 f"{byts/1048576:,.0f} MB · {route.describe()}")

    def worker(idx: int):
        client = _client(pm, merged, log, stats, adaptive)
        while not stop.is_set():
            try:
                off = q.get_nowait()
            except _queue.Empty:
                return
            if sk_db.stop_requested(job_id):
                halt["reason"] = "stopped by user"
                stop.set()
                return
            bh = budget_check()
            if bh:
                log(f"  ⏸ {bh}")
                halt["reason"] = bh
                stop.set()
                return
            cid, kid = off["college_id"], off["course_id"]
            client.session_id = f"skk{idx}_{cid}"
            try:
                state = fetch_college_state(client, _abs(off["url"]),
                                            f"{cid}/{kid}", route=route)
                row = sk_parse.parse_course_detail(state, job_id)
                if row is None:
                    raise ParseEmptyError(
                        f"course {kid}: page carries no courseData")
                # Trust the queue's ids over the page's own, so a redirect to a
                # different course cannot overwrite the wrong row.
                row["college_id"], row["course_id"] = cid, kid
                row["deep_scraped_at"] = time.time()
                sk_db.upsert_offering_deep([row])
                with lock:
                    counts["rows"] += 1
                    if row.get("specialization"):
                        counts["spec"] += 1
            except (NoStateError, PageGoneError):
                # Stamp it so the queue drains: a course page the site says is
                # gone will still be gone next run.
                sk_db.upsert_offering_deep([{"college_id": cid, "course_id": kid,
                                             "deep_scraped_at": time.time(),
                                             "source_job_id": job_id}])
                with lock:
                    counts["gone"] += 1
            except Exception as err:  # noqa: BLE001
                with lock:
                    counts["error"] += 1
                if counts["error"] <= 20:
                    log(f"  ! course {kid} failed: {str(err)[:160]}")
            with lock:
                counts["done"] += 1
                n = counts["done"]
            if n % 100 == 0:
                push()
            if delay:
                time.sleep(adaptive.value() if adaptive else delay)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    push()
    reqs, byts, _ = stats.snapshot()
    msg = (f"{halt['reason'] + ' — ' if halt['reason'] else ''}"
           f"phase Ⓓ: {counts['rows']:,} courses enriched, "
           f"{counts['spec']:,} with a specialization · "
           f"{counts['gone']:,} gone, {counts['error']:,} errors · "
           f"{byts/1048576:,.0f} MB via {route.describe()}")
    sk_db.update_job(job_id, status="stopped" if halt["reason"] else "completed",
                     message=msg, finished_at=time.time())
    log(msg)
