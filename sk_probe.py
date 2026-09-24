"""
Shiksha reachability probe — settles WHY a request is refused, for ~20 requests.

Run in the Render Shell:

    python sk_probe.py

The first discovery run got HTTP 403 on every one of five attempts, each through
a different exit IP. Two explanations fit that equally well and they lead in
opposite directions:

  A. Shiksha refuses this proxy's IPs (or datacentre IPs generally).
  B. Shiksha refuses the REQUEST — `scraper.base_headers()` was written for
     Collegedunia and sends `Referer: https://collegedunia.com/course-finder`,
     `X-Requested-With: XMLHttpRequest` and `Accept: application/json` on what is
     a plain XML file fetch. A cross-site referer plus an AJAX marker on a static
     asset is about as bot-like as a request gets.

Guessing between them is what this avoids. The probe crosses three header
profiles with two transports over two URLs and prints the status codes. Whatever
the grid shows, it is measured, not argued.

Nothing is written to any database, and no proxy URL or credential is printed —
only the host and sticky-session label, via `redact_proxy`.
"""
from __future__ import annotations

BUILD = "2026-09-24a"

import random
import sys
import time
from typing import Dict

import requests

import db as _core
from scraper import ProxyManager, base_headers, is_block_page, redact_proxy
# One source of truth for the Shiksha header set — the probe must test exactly
# what the scraper sends, or it proves nothing about the scraper.
from sk_scraper import CHROME_UA, sk_headers

SITE = "https://www.shiksha.com"

URLS = [
    ("robots.txt", f"{SITE}/robots.txt"),
    ("sitemap_index", f"{SITE}/sitemap_index.xml"),
    ("college page", f"{SITE}/college/iim-ahmedabad-72"),
]

PROFILES = {
    "collegedunia": base_headers,          # exactly what just got 403ed
    "browser": sk_headers,                 # exactly what sk_scraper now sends
    "minimal": lambda: {"User-Agent": CHROME_UA},
}


def _pm() -> ProxyManager:
    return ProxyManager.from_config({
        "proxy_mode": _core.get_setting("proxy_mode", "none"),
        "proxy_gateway": _core.proxy_gateway(),
        "proxy_list": [p.strip() for p in
                       (_core.get_setting("proxy_list_text", "") or "").splitlines()
                       if p.strip()],
        "proxy_session_template": _core.get_setting("proxy_session_template", ""),
    })


def probe(session, url, headers, proxy) -> str:
    try:
        r = session.get(url, headers=headers,
                        proxies=proxy.as_dict() if proxy else None,
                        timeout=30, stream=True, allow_redirects=True)
        body = r.content or b""
        note = ""
        if r.status_code == 200 and body[:2] != b"\x1f\x8b":
            if is_block_page(body[:4096].decode("utf-8", "replace")):
                note = " CHALLENGE-PAGE"
        return f"{r.status_code} {len(body)//1024:>5} KB{note}"
    except Exception as err:  # noqa: BLE001
        return f"ERR {type(err).__name__}: {str(err)[:60]}"


def main() -> None:
    pm = _pm()
    print(f"Shiksha probe [BUILD {BUILD}]")
    print(f"proxy mode: {pm.mode} · healthy: {pm.healthy_count()}")
    print()
    header = f"{'url':<14} {'headers':<13} {'via':<10} result"
    print(header)
    print("-" * len(header) + "-" * 20)

    for label, url in URLS:
        for pname, pfn in PROFILES.items():
            for via in ("proxy", "direct"):
                if via == "proxy" and pm.mode == "none":
                    continue
                sid = f"probe{random.randint(0, 10**6)}"
                proxy = pm.get(sid) if via == "proxy" else None
                sess = requests.Session()      # a fresh connection each time
                out = probe(sess, url, pfn(), proxy)
                where = (redact_proxy(proxy.url).split(" ")[0]
                         if proxy else "direct")
                print(f"{label:<14} {pname:<13} {where[:10]:<10} {out}")
                sess.close()
                time.sleep(1.0)
        print()

    print("How to read this grid")
    print("  browser/minimal pass, collegedunia fails  -> it was the HEADERS.")
    print("  every profile fails through the proxy but direct passes")
    print("                                            -> the proxy's IPs.")
    print("  everything fails, direct included         -> the site refuses this")
    print("     host and this TLS stack; headers will not fix it and the next")
    print("     lever is the apis.shiksha.com gateway or a different client.")
    print("  403 only on sitemap_index, 200 on robots  -> path-level rule.")


if __name__ == "__main__":
    sys.exit(main())
