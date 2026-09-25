"""
Shiksha probe #3 — can college detail be had for less than 170 KB a page?

This is the question that decides whether phase Ⓑ happens at all. Discovery says
57,751 colleges; at the measured 170 KB per home page that is **9.36 GB** through
a 5 GB proxy plan, onto a disk that already holds 4.0 GB of Collegedunia data.

The 2026-09-24 recon spotted the lead: the page calls
`apis.shiksha.com/apigateway/…` with two shapes —

    ?listingId=72&listingType=institute&pageType=iulp&reviewCount=6
    ?data=<base64 JSON>          <- the same pattern as Collegedunia's web-api

— but the endpoint paths were not readable in that session. curl_cffi now gets
200s from shiksha.com, so they can simply be read off the page and its bundles.

What this does, for about a dozen requests:

  1. Fetch one real college home page. Report wire bytes vs decompressed — the
     170 KB figure was measured in a browser, so confirm it from here.
  2. Measure `window.__PRELOADED_STATE__` and list its top-level keys, which is
     what a detail parser would actually consume.
  3. Pull every `apis.shiksha.com` URL out of the HTML and try each one, printing
     status and size. A JSON endpoint that answers by college id is the prize.
  4. If the HTML holds none, fetch the page's JS bundles and grep the gateway
     paths out of them — reported, not called, since they need parameters.

Read-only. Writes nothing, and takes the college id from the discovered database
so it is a real page rather than a guess.
"""
from __future__ import annotations

BUILD = "2026-09-25a"

import json
import re
import sys
from typing import Dict, List, Optional

import db as _core
import sk_db
from scraper import sticky_gateway, DEFAULT_SESSION_TEMPLATE, redact_proxy
from sk_scraper import IMPERSONATE, SITE, _wire

try:
    from curl_cffi import requests as _curl
except ImportError:  # pragma: no cover
    _curl = None

PRELOAD_RE = re.compile(
    r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\})\s*;?\s*</script>", re.S)
GATEWAY_RE = re.compile(r"https?://apis\.shiksha\.com/[^\"'\\\s<>)]+")
SCRIPT_RE = re.compile(r'<script[^>]+src=["\']([^"\']+)["\']', re.I)
PATH_RE = re.compile(r"[\"'](/?(?:apigateway|gateway)/[A-Za-z0-9_\-/.]{3,80})[\"']")


def _proxy() -> Optional[str]:
    gw = _core.proxy_gateway()
    if not gw or _core.get_setting("proxy_mode", "none") == "none":
        return None
    tmpl = _core.get_setting("proxy_session_template", "") or DEFAULT_SESSION_TEMPLATE
    return sticky_gateway(gw, "probe3", tmpl)


def _sess():
    if _curl is None:
        print("curl_cffi is not installed — `pip install curl_cffi`.")
        sys.exit(1)
    return _curl.Session(impersonate=IMPERSONATE)


def _get(sess, url: str, proxy: Optional[str], label: str):
    try:
        r = sess.get(url, proxies={"http": proxy, "https": proxy} if proxy else None,
                     timeout=45, allow_redirects=True)
        wire = _wire(r)
        body = r.content or b""
        print(f"   {label:<34} {r.status_code}  wire {wire/1024:>8,.1f} KB  "
              f"body {len(body)/1024:>9,.1f} KB")
        return r
    except Exception as err:  # noqa: BLE001
        print(f"   {label:<34} ERR {type(err).__name__}: {str(err)[:60]}")
        return None


def pick_college() -> Dict[str, str]:
    """A real college from the discovered inventory — one with plenty of tabs, so
    the page is representative rather than a stub."""
    with sk_db.connect() as conn:
        r = conn.execute(
            "SELECT college_id, slug, url FROM sk_colleges "
            "WHERE COALESCE(tabs,'') LIKE '%fees%' AND COALESCE(slug,'')<>'' "
            "ORDER BY college_id LIMIT 1").fetchone()
        if r:
            return dict(r)
        r = conn.execute("SELECT college_id, slug, url FROM sk_colleges "
                         "WHERE COALESCE(slug,'')<>'' LIMIT 1").fetchone()
        return dict(r) if r else {}


def main() -> None:
    print(f"Shiksha probe #3 — detail cost [BUILD {BUILD}]")
    proxy = _proxy()
    print(f"proxy: {redact_proxy(proxy) if proxy else 'direct'}\n")
    sess = _sess()

    col = pick_college()
    if not col:
        print("No colleges in the database — run discovery first.")
        return
    cid, slug = col["college_id"], col["slug"]
    url = col.get("url") or f"{SITE}/college/{slug}-{cid}"
    print(f"1. College home page  (id {cid}, {slug})")
    r = _get(sess, url, proxy, "home page")
    if r is None or r.status_code != 200:
        print("\n   Could not fetch the page; nothing further to measure.")
        return
    html = r.text

    print("\n2. __PRELOADED_STATE__")
    m = PRELOAD_RE.search(html)
    if not m:
        loose = re.search(r"window\.__PRELOADED_STATE__\s*=", html)
        print(f"   not extracted (marker present: {bool(loose)})")
        state = None
    else:
        blob = m.group(1)
        print(f"   {len(blob)/1024:,.1f} KB of JSON in the HTML")
        try:
            state = json.loads(blob)
            keys = list(state) if isinstance(state, dict) else []
            print(f"   {len(keys)} top-level keys: " + ", ".join(keys[:14]))
            for k in ("instituteData", "childPageData"):
                v = state.get(k) if isinstance(state, dict) else None
                if isinstance(v, dict):
                    print(f"     {k}: {', '.join(list(v)[:10])}")
        except Exception as err:  # noqa: BLE001
            print(f"   present but did not parse: {str(err)[:80]}")
            state = None

    print("\n3. apis.shiksha.com URLs in the page")
    found: List[str] = []
    for u in GATEWAY_RE.findall(html):
        u = u.rstrip("\\\"',;")
        if u not in found:
            found.append(u)
    if not found:
        print("   none in the HTML")
    for u in found[:8]:
        print(f"   {u[:100]}")
    for i, u in enumerate(found[:5]):
        _get(sess, u, proxy, f"gateway #{i+1}")

    print("\n4. Gateway paths inside the JS bundles")
    scripts = [s for s in SCRIPT_RE.findall(html) if s.endswith(".js")]
    scripts = [s if s.startswith("http") else
               ("https:" + s if s.startswith("//") else SITE + s) for s in scripts]
    print(f"   {len(scripts)} script tags; fetching up to 3")
    paths = set()
    for s in scripts[:3]:
        rb = _get(sess, s, proxy, s.rsplit("/", 1)[-1][:32])
        if rb is not None and rb.status_code == 200:
            for p in PATH_RE.findall(rb.text):
                paths.add(p)
    if paths:
        print(f"   {len(paths)} distinct gateway paths found:")
        for p in sorted(paths)[:25]:
            print(f"     {p}")
        print("   (not called — they need parameters; this is the map)")
    else:
        print("   none found in the bundles fetched")

    print("\n5. What this means for phase Ⓑ")
    n = 0
    try:
        n = sk_db.counts().get("colleges", 0)
    except Exception:  # noqa: BLE001
        pass
    wire_kb = _wire(r) / 1024.0
    print(f"   measured home page : {wire_kb:,.1f} KB on the wire")
    print(f"   colleges           : {n:,}")
    print(f"   one full pass      : {n * wire_kb / 1024 / 1024:,.2f} GB")
    print("   A gateway call returning the same data as JSON would replace that")
    print("   figure with its own. If nothing above answers by college id, the")
    print("   next lever is the tab pages, which are smaller than the home page.")


if __name__ == "__main__":
    main()
