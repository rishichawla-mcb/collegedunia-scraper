"""
Shiksha — map the source schema of a college page. Read-only, ~4 requests.

Probe #3 established the facts that matter:

  * A college page is reachable from every route — direct, proxy, proxy IN,
    cold or warmed. The single 403 before it was one burned exit IP.
  * 171.4 KB on the wire, 1,075.6 KB decompressed. The recon's 170 KB was right.
  * `window.__PRELOADED_STATE__` is **616 KB of JSON inside that HTML** — the
    whole dataset is already in the page.
  * The only `apis.shiksha.com` URL in the HTML is `appbeat/logJSError`, an error
    logger. The data-bearing gateway calls happen at runtime, so they are not
    readable from the static page.

That last point retires the gateway lead for now: there is no cheaper endpoint to
find from here, and there is no need for one either — the page already contains
everything. The remaining question is not *where* the data is, but *which parts
of it are worth keeping*, because the constraint has moved to disk.

Two bugs in probe #3 are fixed here:

  1. `__PRELOADED_STATE__` was extracted with a non-greedy regex, which ran past
     the end of the object ("Extra data: line 1 column 608718"). JSON is not a
     regular language; this uses a brace scanner that respects strings and
     escapes.
  2. The script-tag regex found 0 script tags in a 1 MB page, which cannot be
     right. It now reports the raw `<script` count too, so a zero is a finding
     rather than a silent miss.

Output is a key tree with the serialised size of every branch — the input to
deciding what phase Ⓑ stores. It also measures the `courses` and `fees` tabs, in
case a tab is cheaper than the home page for the fields we actually want.
"""
from __future__ import annotations

BUILD = "2026-09-25a"

import json
import os
import re
import sys
from typing import Any, Dict, Optional, Tuple

import db as _core
import sk_db
from scraper import DEFAULT_SESSION_TEMPLATE, sticky_gateway
from sk_scraper import IMPERSONATE, SITE, _wire

try:
    from curl_cffi import requests as _curl
except ImportError:  # pragma: no cover
    _curl = None

MARKER = "window.__PRELOADED_STATE__"
SCRIPT_RE = re.compile(r"<script\b[^>]*\bsrc\s*=\s*[\"']([^\"']+)[\"']", re.I)
GATEWAY_RE = re.compile(r"https?://apis\.shiksha\.com/[^\"'\\\s<>)]+")

# Where to drop the parsed sample so it can be inspected later without spending
# another 171 KB. One file, a few hundred KB.
SAMPLE_DIR = os.path.dirname(os.path.abspath(sk_db.SK_DB_PATH))


def extract_state(html: str) -> Optional[str]:
    """The JSON object assigned to __PRELOADED_STATE__, by brace matching.

    A regex cannot do this: the object contains braces inside strings, and the
    assignment is followed by more JavaScript. This walks from the opening brace
    counting depth, skipping over string literals and their escapes, and stops at
    the brace that closes it."""
    i = html.find(MARKER)
    if i < 0:
        return None
    j = html.find("{", i)
    if j < 0:
        return None
    depth, in_str, esc, quote = 0, False, False, ""
    for k in range(j, len(html)):
        c = html[k]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == quote:
                in_str = False
            continue
        if c in "\"'":
            in_str, quote = True, c
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return html[j:k + 1]
    return None


def size_of(v: Any) -> int:
    try:
        return len(json.dumps(v, ensure_ascii=False))
    except Exception:  # noqa: BLE001
        return 0


def describe(v: Any) -> str:
    if isinstance(v, dict):
        return f"dict({len(v)})"
    if isinstance(v, list):
        inner = describe(v[0]) if v else "empty"
        return f"list[{len(v)}] of {inner}"
    if isinstance(v, str):
        s = v.replace("\n", " ")[:46]
        return f'str "{s}"' + ("…" if len(v) > 46 else "")
    return f"{type(v).__name__} {v}"


def tree(node: Any, depth: int = 0, max_depth: int = 3,
         prefix: str = "", min_kb: float = 0.5) -> None:
    """Key tree with sizes. Branches under `min_kb` are summarised, not walked —
    the point is to see where the 616 KB actually sits."""
    if depth > max_depth:
        return
    if isinstance(node, dict):
        items = sorted(node.items(), key=lambda kv: -size_of(kv[1]))
        for k, v in items:
            kb = size_of(v) / 1024.0
            line = f"{prefix}{k:<28} {kb:>9,.1f} KB  {describe(v)}"
            print(line[:150])
            if kb >= min_kb and isinstance(v, (dict, list)):
                tree(v, depth + 1, max_depth, prefix + "  ", min_kb)
    elif isinstance(node, list) and node:
        # one representative element, so a 400-item list prints once
        print(f"{prefix}[0] of {len(node)}")
        tree(node[0], depth + 1, max_depth, prefix + "  ", min_kb)


def _route() -> Tuple[str, Optional[str]]:
    """Direct unless a route is forced with CD_SK_PROBE_ROUTE=proxy.

    Probe #3 showed all routes work and cost the same. Direct is the default
    here because a probe should not spend proxy quota to answer a question that
    does not depend on the proxy."""
    if os.environ.get("CD_SK_PROBE_ROUTE", "direct") != "proxy":
        return "direct", None
    gw = _core.proxy_gateway()
    if not gw:
        return "direct", None
    tmpl = _core.get_setting("proxy_session_template", "") or DEFAULT_SESSION_TEMPLATE
    return "proxy", sticky_gateway(gw, "schema1", tmpl)


def fetch(sess, url: str, proxy: Optional[str], label: str):
    try:
        r = sess.get(url, proxies={"http": proxy, "https": proxy} if proxy else None,
                     timeout=60, allow_redirects=True)
        print(f"   {label:<22} {r.status_code}  wire {_wire(r)/1024:>8,.1f} KB  "
              f"body {len(r.content or b'')/1024:>9,.1f} KB")
        return r
    except Exception as err:  # noqa: BLE001
        print(f"   {label:<22} ERR {type(err).__name__}: {str(err)[:60]}")
        return None


def pick_college() -> Dict[str, Any]:
    with sk_db.connect() as conn:
        for sql in ("SELECT college_id, slug, url, tabs FROM sk_colleges "
                    "WHERE COALESCE(tabs,'') LIKE '%fees%' "
                    "AND COALESCE(tabs,'') LIKE '%placement%' "
                    "AND COALESCE(slug,'')<>'' ORDER BY college_id LIMIT 1",
                    "SELECT college_id, slug, url, tabs FROM sk_colleges "
                    "WHERE COALESCE(slug,'')<>'' LIMIT 1"):
            r = conn.execute(sql).fetchone()
            if r:
                return dict(r)
    return {}


def main() -> None:
    if _curl is None:
        print("curl_cffi is not installed — `pip install curl_cffi`.")
        sys.exit(1)
    print(f"Shiksha source-schema map [BUILD {BUILD}]")
    rlabel, proxy = _route()
    print(f"route: {rlabel}\n")

    col = pick_college()
    if not col:
        print("No colleges in the database — run discovery first.")
        return
    cid, slug = col["college_id"], col["slug"]
    base = col.get("url") or f"{SITE}/college/{slug}-{cid}"
    sess = _curl.Session(impersonate=IMPERSONATE)

    print(f"1. Pages for college {cid} ({slug})")
    print(f"   tabs published: {col.get('tabs')}")
    home = fetch(sess, base, proxy, "home")
    if home is None or home.status_code != 200:
        print("   home page not reachable; stopping.")
        return
    html = home.text
    for tab in ("courses", "fees"):
        fetch(sess, f"{base}/{tab}", proxy, tab)

    print("\n2. Page anatomy")
    n_script = html.count("<script")
    srcs = SCRIPT_RE.findall(html)
    print(f"   '<script' occurrences : {n_script}")
    print(f"   with a src attribute  : {len(srcs)}")
    for s in srcs[:6]:
        print(f"     {s[:96]}")
    gws = sorted({g.rstrip('\\"\',;') for g in GATEWAY_RE.findall(html)})
    print(f"   apis.shiksha.com URLs : {len(gws)}")
    for g in gws[:8]:
        print(f"     {g[:96]}")

    print("\n3. __PRELOADED_STATE__")
    blob = extract_state(html)
    if not blob:
        print("   not found")
        return
    print(f"   extracted {len(blob)/1024:,.1f} KB")
    try:
        state = json.loads(blob)
    except Exception as err:  # noqa: BLE001
        print(f"   STILL did not parse: {str(err)[:120]}")
        print(f"   tail: …{blob[-160:]}")
        return
    print(f"   parsed OK — {len(state) if isinstance(state, dict) else '?'} "
          f"top-level keys\n")

    print("4. Where the 616 KB sits (branches >= 0.5 KB, depth 3)")
    tree(state, max_depth=3)

    out = os.path.join(SAMPLE_DIR, f"sk_sample_{cid}.json")
    try:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False)
        print(f"\n5. Full parsed state written to {out} "
              f"({os.path.getsize(out)/1024:,.0f} KB) — inspect it without "
              f"spending another 171 KB.")
    except Exception as err:  # noqa: BLE001
        print(f"\n5. could not write sample: {str(err)[:100]}")

    n = sk_db.counts().get("colleges", 0)
    kb = _wire(home) / 1024.0
    print("\n6. Phase Ⓑ arithmetic")
    print(f"   {n:,} colleges x {kb:,.0f} KB = {n*kb/1024/1024:,.2f} GB on the wire")
    print(f"   storing the raw state would be "
          f"{n*len(blob)/1024/1024/1024:,.1f} GB on disk — not an option on a "
          f"4.9 GB disk holding 4.0 GB already.")
    print("   storing PARSED columns only is the way: a few KB per college.")


if __name__ == "__main__":
    main()
