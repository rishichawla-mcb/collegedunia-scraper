"""
Shiksha vertical — test harness.

Drives the REAL runner (`sk_scraper.run_discovery`) with the transport
monkeypatched at `requests.Session.get`, exactly as the other twelve suites do.
Nothing is stubbed above the socket: the parsers, the accumulator, the upserts,
the freshness tracking, the resume logic and the job bookkeeping all run.

Every load-bearing guard is mutation-tested — the guard is broken on purpose and
the test must then fail. A test that passes both ways is not testing anything.

    python t_shiksha.py
"""
from __future__ import annotations

import gzip
import io
import os
import shutil
import sys
import tempfile

_TMP = tempfile.mkdtemp(prefix="sktest_")
os.environ["CD_DB_PATH"] = os.path.join(_TMP, "data.db")
os.environ["CD_SK_DB_PATH"] = os.path.join(_TMP, "shiksha.db")

import requests  # noqa: E402

import sk_db      # noqa: E402
import sk_scraper  # noqa: E402

SITE = "https://www.shiksha.com"

FAILURES = []
PASSES = []


def check(name, cond, detail=""):
    (PASSES if cond else FAILURES).append(name)
    print(("  ok   " if cond else "  FAIL ") + name + (f"  — {detail}" if detail and not cond else ""))
    return cond


# ---------------------------------------------------------------------------
# A fake site: a sitemap index plus five sitemap files.
# ---------------------------------------------------------------------------
def _urlset(entries):
    body = "".join(
        f"<url><loc>{loc}</loc>" + (f"<lastmod>{lm}</lastmod>" if lm else "") + "</url>"
        for loc, lm in entries)
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            + body + "</urlset>")


def _index(names):
    body = "".join(f"<sitemap><loc>{SITE}/{n}</loc>"
                   f"<lastmod>2026-09-20</lastmod></sitemap>" for n in names)
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            + body + "</sitemapindex>")


def _gz(s: str) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(s.encode())
    return buf.getvalue()


SITEMAP_NAMES = ["www_sd_college_SiteMap_f1.xml.gz",
                 "www_sd_college_SiteMap_f2.xml.gz",
                 "www_sd_university_SiteMap_f1.xml.gz",
                 "www_listing_SiteMap_f1.xml.gz",
                 "www_news_SiteMap_f1.xml.gz"]

# college 72 appears under THREE home slugs (the alias problem, in miniature:
# 3 home URLs -> 1 id), college 99 under one.
COLLEGE_F1 = _urlset([
    (f"{SITE}/college/iim-ahmedabad-72", "2026-09-01"),
    (f"{SITE}/college/indian-institute-of-management-ahmedabad-72", "2026-09-02"),
    (f"{SITE}/college/iim-a-72", ""),
    (f"{SITE}/college/iim-ahmedabad-72/fees", "2026-09-03"),
    (f"{SITE}/college/nit-trichy-2-99", "2026-08-15"),      # digits inside the slug
    (f"{SITE}/college/nit-trichy-2-99/courses", ""),
])
COLLEGE_F2 = _urlset([
    (f"{SITE}/college/iim-ahmedabad-72/placement", ""),
    (f"{SITE}/college/iim-ahmedabad-72/course-mba-101", "2026-09-05"),
    (f"{SITE}/college/nit-trichy-2-99/course-mba-101", ""),
    (f"{SITE}/college/nit-trichy-2-99/course-btech-computer-science-202", ""),
    (f"{SITE}/college/nit-trichy-2-99/some-unknown-page", ""),
])
UNIV_F1 = _urlset([
    (f"{SITE}/university/anna-university-5", "2026-07-01"),
    (f"{SITE}/university/anna-university-5/courses", ""),
])
LISTING_F1 = _urlset([
    (f"{SITE}/college/iim-ahmedabad-72/course-pgp-303", ""),
])
# Must never be read: it is not in SITEMAP_GROUPS.
NEWS_F1 = _urlset([(f"{SITE}/college/should-never-be-seen-4242", "")])

BODIES = {
    f"{SITE}/sitemap_index.xml": _index(SITEMAP_NAMES).encode(),
    f"{SITE}/www_sd_college_SiteMap_f1.xml.gz": _gz(COLLEGE_F1),
    f"{SITE}/www_sd_college_SiteMap_f2.xml.gz": _gz(COLLEGE_F2),
    f"{SITE}/www_sd_university_SiteMap_f1.xml.gz": _gz(UNIV_F1),
    f"{SITE}/www_listing_SiteMap_f1.xml.gz": _gz(LISTING_F1),
    f"{SITE}/www_news_SiteMap_f1.xml.gz": _gz(NEWS_F1),
}

REQUESTS = []          # (url, session_id_in_proxy_or_None)


HOST_IP = "203.0.113.9"          # this host's own public IP, in the fake world
PROXY_IP = "198.51.100.44"       # what the gateway's exit looks like


class FakeResponse:
    """Shaped like a curl_cffi response: no `raw`, but curl's own counters."""

    def __init__(self, body: bytes, status: int = 200):
        self.status_code = status
        self._body = body
        self.headers = {"Content-Length": str(len(body)),
                        "Content-Type": "application/xml"}
        self.download_size = len(body)
        self.header_size = 120
        self.encoding = "utf-8"

    @property
    def content(self):
        return self._body

    @property
    def text(self):
        return self._body.decode("utf-8", "replace")

    def json(self):
        import json as _j
        return _j.loads(self._body.decode("utf-8"))

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Stands in for `curl_cffi.requests.Session`. Installed by replacing
    `sk_scraper._session_for`, which is the seam the real code opens for it —
    so the runner, the retry loop and the rotation logic are all still real."""

    def __init__(self, fail_urls, missing, honour_proxy=True):
        self.fail_urls, self.missing = fail_urls, missing
        self.honour_proxy = honour_proxy

    def get(self, url, proxies=None, timeout=None, allow_redirects=True, **kw):
        prox = (proxies or {}).get("https")
        REQUESTS.append((url, prox))
        if url.startswith(sk_scraper.IP_ECHO.split("?")[0]):
            # A proxy that is honoured changes the exit IP; one that is silently
            # ignored does not. That is exactly what the guard measures.
            ip = PROXY_IP if (prox and self.honour_proxy) else HOST_IP
            return FakeResponse(('{"ip": "%s"}' % ip).encode())
        if url in self.fail_urls and self.fail_urls[url] > 0:
            self.fail_urls[url] -= 1
            return FakeResponse(b"blocked", 403)
        if url in self.missing:
            return FakeResponse(b"nope", 404)
        body = BODIES.get(url)
        if body is None:
            return FakeResponse(b"not found", 404)
        return FakeResponse(body)


def install_transport(fail_urls=None, missing=(), honour_proxy=True):
    fail_urls = fail_urls or {}

    def _session_for(client):
        s = getattr(client, "_sk_curl", None)
        if s is None:
            s = FakeSession(fail_urls, missing, honour_proxy)
            client._sk_curl = s
        return s

    sk_scraper._session_for = _session_for


install_transport()


def fresh_db():
    p = os.path.join(_TMP, "shiksha.db")
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(p + suffix)
        except OSError:
            pass
    # freshness caches "this table is ready" per PROCESS, keyed by table name
    # alone. Deleting the file underneath it would otherwise leave the cache
    # asserting a schema that no longer exists.
    import freshness as _fr
    _fr._SCHEMA_READY.clear()
    sk_db.init_db()
    REQUESTS.clear()


CFG = {"proxy_mode": "gateway",
       "proxy_gateway": "http://u:p@gw.example.com:7777",
       "concurrency": 2, "delay": 0, "adaptive": False, "max_retries": 3,
       "backoff": 0.01}


LOGS = []


def run(cfg=None, phase="discovery"):
    """Drive the real runner. Job logs are captured, not swallowed: a swallowed
    '! sitemap … failed' once turned a broken run into a silently empty one."""
    del LOGS[:]
    job = sk_db.create_job(phase, {})
    sk_scraper.run_discovery(job, {**CFG, **(cfg or {})}, log=LOGS.append)
    bad = [m for m in LOGS if m.lstrip().startswith("!")]
    if bad and os.environ.get("SK_TEST_VERBOSE", "1") != "0":
        for m in bad[:8]:
            print("       [runner]", m.strip())
    return sk_db.get_job(job)


# ---------------------------------------------------------------------------
print("\n== 1. classify() ==")
c = sk_scraper.classify(f"{SITE}/college/iim-ahmedabad-72")
check("home URL -> college_home + id", c["kind"] == "college_home" and c["college_id"] == 72, c)
c = sk_scraper.classify(f"{SITE}/college/iim-ahmedabad-72/")
check("trailing slash tolerated", c["kind"] == "college_home" and c["college_id"] == 72, c)
c = sk_scraper.classify(f"{SITE}/college/nit-trichy-2-99")
check("id is the LAST numeric segment, not the first",
      c["college_id"] == 99 and c["slug"] == "nit-trichy-2", c)
c = sk_scraper.classify(f"{SITE}/college/iim-ahmedabad-72/fees")
check("tab URL -> college_tab", c["kind"] == "college_tab" and c["tab"] == "fees", c)
c = sk_scraper.classify(f"{SITE}/college/iim-ahmedabad-72/course-btech-computer-science-202")
check("offering carries BOTH ids",
      c["kind"] == "offering" and c["college_id"] == 72 and c["course_id"] == 202
      and c["course_slug"] == "btech-computer-science", c)
c = sk_scraper.classify(f"{SITE}/college/x-72/some-unknown-page")
check("unknown sub-page -> tab 'other', never dropped silently",
      c["kind"] == "college_tab" and c["tab"] == "other", c)
c = sk_scraper.classify(f"{SITE}/university/anna-university-5")
check("university home", c["kind"] == "university_home" and c["university_id"] == 5, c)
check("news/editorial URL -> other",
      sk_scraper.classify(f"{SITE}/news/some-story")["kind"] == "other")

print("\n== 2. decompress() / parse_sitemap() ==")
check("gzip FILE body is decompressed",
      sk_scraper.decompress(_gz("<urlset/>")) == "<urlset/>")
check("already-decoded body passes through",
      sk_scraper.decompress(b"<urlset/>") == "<urlset/>")
ents = sk_scraper.parse_sitemap(COLLEGE_F1)
check("urlset parsed with namespace", len(ents) == 6, len(ents))
check("lastmod paired to its own loc",
      dict(ents)[f"{SITE}/college/iim-ahmedabad-72"] == "2026-09-01")
check("missing lastmod -> empty, not the neighbour's",
      dict(ents)[f"{SITE}/college/iim-a-72"] == "")
check("sitemap index parsed the same way",
      len(sk_scraper.parse_sitemap(_index(SITEMAP_NAMES))) == 5)
check("truncated XML still yields its <loc>s",
      len(sk_scraper.parse_sitemap(
          f"<urlset><url><loc>{SITE}/college/a-1</loc></url><url><loc>"
          f"{SITE}/college/b-2</loc>")) == 2)

print("\n== 3. proxy is mandatory ==")
fresh_db()
try:
    run({"proxy_mode": "none", "proxy_gateway": ""})
    check("no-proxy run is refused", False, "it ran")
except sk_scraper.ProxyRequired:
    check("no-proxy run is refused", True)
except Exception as e:  # noqa: BLE001
    check("no-proxy run is refused", False, f"wrong exception: {e!r}")
fresh_db()
logged = []
try:
    run({"proxy_mode": "none", "proxy_gateway": "", "allow_direct": True})
    ran = True
except Exception:  # noqa: BLE001
    ran = False
check("allow_direct is the only override, and it works", ran)

print("\n== 3b. the proxy must demonstrably carry the traffic ==")
fresh_db()
job_pc = run()
check("a working proxy passes the exit-IP check",
      job_pc["status"] == "completed", job_pc["message"])
check("the check is logged with the exit IP",
      any("proxy exit IP confirmed" in m for m in LOGS), LOGS[:4])
fresh_db()
install_transport(honour_proxy=False)     # curl silently ignores `proxies`
try:
    run()
    caught = False
except sk_scraper.ProxyIneffective:
    caught = True
except Exception as e:  # noqa: BLE001
    caught = f"wrong exception {type(e).__name__}"
check("a proxy that does NOT change the exit IP stops the crawl",
      caught is True, caught)
check("and nothing was fetched from the site itself",
      not any(u.endswith(".gz") for u, _ in REQUESTS),
      [u for u, _ in REQUESTS if u.endswith(".gz")])
install_transport()

print("\n== 4. discovery, end to end ==")
fresh_db()
job = run()
counts = sk_db.counts()
check("job completed", job["status"] == "completed", job["message"])
check("DEDUPED BY ID: 4 home URLs -> 2 colleges", counts["colleges"] == 2, counts)
check("every alias slug kept (3 for id 72, 1 for id 99)",
      counts["aliases"] == 4, counts)
check("universities found", counts["universities"] == 1, counts)
check("offerings: all 4 college x course edges", counts["offerings"] == 4, counts)
check("courses learned free from offering URLs", counts["courses"] == 3, counts)
check("the news sitemap was never fetched",
      not any("news" in u for u, _ in REQUESTS),
      [u for u, _ in REQUESTS if "news" in u])
check("college 4242 (news-only) was not created",
      not sk_db.colleges_pending(db_path=sk_db.SK_DB_PATH) or
      all(r["college_id"] != 4242 for r in sk_db.colleges_pending()))

with sk_db.connect() as conn:
    row = dict(conn.execute("SELECT * FROM sk_colleges WHERE college_id=72").fetchone())
    off = [dict(r) for r in conn.execute(
        "SELECT * FROM sk_offerings WHERE college_id=72 ORDER BY course_id")]
    crs = {r["course_id"]: dict(r) for r in conn.execute("SELECT * FROM sk_courses")}
check("tabs UNIONED across two different sitemap files",
      row["tabs"] == "fees,placement", row["tabs"])
check("canonical slug = shortest home slug seen", row["slug"] == "iim-a-72"[:-3] or
      row["slug"] == "iim-a", row["slug"])
check("alias_count recorded", row["alias_count"] == 3, row["alias_count"])
check("lastmod kept as the newest seen", row["lastmod"] == "2026-09-05", row["lastmod"])
check("offering rows carry the course slug",
      [o["course_slug"] for o in off] == ["mba", "pgp"], [o["course_slug"] for o in off])
check("colleges_count backfilled onto courses",
      crs[101]["colleges_count"] == 2 and crs[202]["colleges_count"] == 1,
      {k: v["colleges_count"] for k, v in crs.items()})

print("\n== 5. every fetch went through a sticky proxy session ==")
# The exit-IP check deliberately sends ONE request with no proxy, to learn this
# host's own IP. Every request to the site itself must still be proxied.
site_reqs = [(u, p) for u, p in REQUESTS
             if not u.startswith(sk_scraper.IP_ECHO.split("?")[0])]
check("no request to shiksha.com went out direct",
      all(p for _, p in site_reqs), [u for u, p in site_reqs if not p])
echo_direct = [u for u, p in REQUESTS
               if u.startswith(sk_scraper.IP_ECHO.split("?")[0]) and not p]
check("exactly one un-proxied request, and it is the exit-IP check",
      len(echo_direct) == 1, echo_direct)
sitemap_sessions = {p for u, p in REQUESTS if u.endswith(".gz")}
check("each sitemap got its own sticky exit IP "
      "(without this, one 403 burns the whole worker)",
      len(sitemap_sessions) >= 4, sitemap_sessions)

print("\n== 6. resume and the union guard ==")
before = REQUESTS[:]
n_before = len(REQUESTS)
REQUESTS.clear()
job2 = run()
refetched = [u for u, _ in REQUESTS if u.endswith(".gz")]
check("a completed sitemap is not re-read", refetched == [], refetched)
with sk_db.connect() as conn:
    row2 = dict(conn.execute("SELECT tabs, alias_count FROM sk_colleges "
                             "WHERE college_id=72").fetchone())
check("resumed run does not SHRINK tabs", row2["tabs"] == "fees,placement", row2)
check("resumed run does not shrink alias_count", row2["alias_count"] == 3, row2)

# Force a partial re-read: clear only f2's progress, then check the union holds
# even though this run never sees f1's 'fees' tab.
with sk_db.connect() as conn:
    conn.execute("DELETE FROM sk_sitemap_progress WHERE sitemap_url LIKE '%f2%'")
REQUESTS.clear()
run()
with sk_db.connect() as conn:
    row3 = dict(conn.execute("SELECT tabs FROM sk_colleges WHERE college_id=72").fetchone())
check("partial re-read still unions with what the db already held "
      "(the seeding guard)", row3["tabs"] == "fees,placement", row3)

print("\n== 7. a blocked sitemap does not lose the rest ==")
fresh_db()
install_transport(fail_urls={f"{SITE}/www_sd_college_SiteMap_f2.xml.gz": 99})
job3 = run()
c3 = sk_db.counts()
check("the other sitemaps still landed", c3["colleges"] == 2, c3)
with sk_db.connect() as conn:
    st = {r[0]: r[1] for r in conn.execute(
        "SELECT sitemap_url, status FROM sk_sitemap_progress")}
check("the failed sitemap is recorded as error, not done",
      any(v == "error" for v in st.values()), st)
check("a failed sitemap stays in the queue for the next run",
      sum(1 for v in st.values() if v == "done") == 3, st)
install_transport()

print("\n== 8. budget stops the run ==")
fresh_db()
job4 = run({"budget_requests": 2})
check("request budget halts and the job says so",
      job4["status"] == "stopped" and "budget" in (job4["message"] or ""),
      job4["message"])

print("\n== 9. mutation tests — break the guard, the test must fail ==")


def mutate(name, patch, restore, assertion):
    try:
        patch()
        ok = assertion()
    except Exception as e:  # noqa: BLE001
        ok = f"raised {type(e).__name__}"
    finally:
        restore()
    check(f"MUTANT CAUGHT: {name}", ok is not True, f"mutant survived ({ok})")


# (a) dedupe by URL instead of by id
_real_classify = sk_scraper.classify


def _by_url(url):
    d = _real_classify(url)
    if d.get("kind") == "college_home":
        d = dict(d)
        d["college_id"] = abs(hash(url)) % 10 ** 6   # a distinct id per URL
    return d


def _assert_two_colleges():
    fresh_db()
    run()
    return sk_db.counts()["colleges"] == 2


mutate("dedupe by URL, not by id",
       lambda: setattr(sk_scraper, "classify", _by_url),
       lambda: setattr(sk_scraper, "classify", _real_classify),
       _assert_two_colleges)

# (b) drop the sticky session id — Client._rotate() becomes a no-op
_real_require = sk_scraper.require_proxy


def _assert_sessions_distinct():
    fresh_db()
    run()
    return len({p for u, p in REQUESTS if u.endswith(".gz")}) >= 4


class _NoStick(str):
    pass


_real_Client_setattr = None


def _patch_no_session():
    global _real_Client_setattr
    from scraper import Client as _C
    _real_Client_setattr = _C.__setattr__

    def _setattr(self, k, v):
        if k == "session_id":
            v = None
        object.__setattr__(self, k, v)
    _C.__setattr__ = _setattr


def _unpatch_no_session():
    from scraper import Client as _C
    if _real_Client_setattr:
        _C.__setattr__ = _real_Client_setattr


mutate("no sticky session id per sitemap",
       _patch_no_session, _unpatch_no_session, _assert_sessions_distinct)

# (c) remove the proxy requirement
_real_req = sk_scraper.require_proxy


def _assert_refuses_direct():
    fresh_db()
    try:
        run({"proxy_mode": "none", "proxy_gateway": ""})
        return False
    except sk_scraper.ProxyRequired:
        return True


mutate("proxy requirement removed",
       lambda: setattr(sk_scraper, "require_proxy", lambda *a, **k: None),
       lambda: setattr(sk_scraper, "require_proxy", _real_req),
       _assert_refuses_direct)

# (d) forget to decompress
_real_decompress = sk_scraper.decompress


def _assert_offerings():
    fresh_db()
    run()
    return sk_db.counts()["offerings"] == 4


mutate("gzip container not decompressed",
       lambda: setattr(sk_scraper, "decompress",
                       lambda b: b.decode("utf-8", "replace") if isinstance(b, bytes) else b),
       lambda: setattr(sk_scraper, "decompress", _real_decompress),
       _assert_offerings)

# (e) seed the accumulator from nothing -> a resumed run shrinks tabs
_real_load = sk_db.load_college_index


def _assert_union_survives():
    fresh_db()
    run()
    with sk_db.connect() as conn:
        conn.execute("DELETE FROM sk_sitemap_progress WHERE sitemap_url LIKE '%f2%'")
    run()
    with sk_db.connect() as conn:
        return dict(conn.execute("SELECT tabs FROM sk_colleges "
                                 "WHERE college_id=72").fetchone())["tabs"] == "fees,placement"


mutate("accumulator not seeded from the db",
       lambda: setattr(sk_db, "load_college_index", lambda *a, **k: {}),
       lambda: setattr(sk_db, "load_college_index", _real_load),
       _assert_union_survives)

# (f) the exit-IP check removed -> a silently-ignored proxy crawls from the
#     host's own IP and nothing says so
_real_assert = sk_scraper.assert_proxy_effective


def _assert_stops_on_bypass():
    fresh_db()
    install_transport(honour_proxy=False)
    try:
        run()
        return False
    except sk_scraper.ProxyIneffective:
        return True
    finally:
        install_transport()


mutate("exit-IP check removed",
       lambda: setattr(sk_scraper, "assert_proxy_effective", lambda *a, **k: None),
       lambda: setattr(sk_scraper, "assert_proxy_effective", _real_assert),
       _assert_stops_on_bypass)

print("\n== 10. wire-byte accounting uses curl's own counter ==")
r = FakeResponse(b"x" * 5000)
check("download_size + header_size is preferred",
      sk_scraper._wire(r) == 5000 + 120, sk_scraper._wire(r))
r2 = FakeResponse(b"y" * 100)
r2.download_size = 0                      # counter unavailable
check("falls back to Content-Length", sk_scraper._wire(r2) == 100,
      sk_scraper._wire(r2))
r3 = FakeResponse(b"z" * 42)
r3.download_size = 0
r3.headers = {}
check("falls back to body length", sk_scraper._wire(r3) == 42, sk_scraper._wire(r3))

print("\n== 11. curl transport errors still rotate the exit IP ==")
from curl_cffi.requests import RequestsError as _CErr  # noqa: E402
from scraper import Client as _RealClient  # noqa: E402
_wrapped = sk_scraper._transport_error(_CErr("tunnel died"))
check("a curl_cffi error is translated to a requests ConnectionError",
      isinstance(_wrapped, requests.exceptions.ConnectionError), type(_wrapped))
check("and therefore classifies as 'proxy', which is what triggers rotation",
      _RealClient._classify(_wrapped) == "proxy",
      _RealClient._classify(_wrapped))
check("an untranslated curl error would NOT rotate (this is the bug it avoids)",
      _RealClient._classify(_CErr("tunnel died")) != "proxy")
check("a BlockedError passes through unchanged",
      isinstance(sk_scraper._transport_error(
          sk_scraper.BlockedError("403")), sk_scraper.BlockedError))

# ---------------------------------------------------------------------------
print("\n" + "=" * 64)
print(f"{len(PASSES)} passed, {len(FAILURES)} failed")
for f in FAILURES:
    print("  FAILED:", f)
shutil.rmtree(_TMP, ignore_errors=True)
sys.exit(1 if FAILURES else 0)
