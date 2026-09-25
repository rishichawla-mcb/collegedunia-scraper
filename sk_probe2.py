"""
Shiksha probe #2 — TLS fingerprint and geography.

Probe #1 settled what it could: **403 in every cell** — all three header
profiles, proxy and direct, and even `/robots.txt`, a file every crawler is
invited to read. So it is not the headers (my `base_headers()` bug was real but
was not the cause), and it is not the proxy IPs alone, since the Render host's
own IP is refused too.

What is left is what probe #1 could not vary, because every cell used the same
Python HTTP stack from the same place:

  A. **TLS fingerprint.** requests/urllib3 produce a ClientHello (JA3/JA4) that
     no browser produces. Commercial bot managers block on it before a single
     header is read — which is exactly what a 403 on `/robots.txt` looks like.
  B. **Geography.** Shiksha is an Indian site; the recon that worked came from
     the owner's residential connection in India. This host is not in India, and
     the proxy gateway is not currently asked for India either.

This probe varies both, and nothing else:

    client   : requests (the baseline that fails)  ×  curl_cffi impersonating Chrome
    route    : direct  ×  proxy (as configured)  ×  proxy pinned to India
    url      : /robots.txt  ×  /sitemap_index.xml

curl_cffi is not in requirements.txt on purpose — it goes in only if this proves
it is needed. Install it for the probe:

    pip install curl_cffi
    python sk_probe2.py

No proxy URL or credential is ever printed: the login is masked and only the
parameter block is shown, so you can see the shape that was built.
"""
from __future__ import annotations

BUILD = "2026-09-25a"

import random
import sys
import time
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests

import db as _core
from scraper import DEFAULT_SESSION_TEMPLATE, sticky_gateway
from sk_scraper import sk_headers

SITE = "https://www.shiksha.com"
URLS = [("robots.txt", f"{SITE}/robots.txt"),
        ("sitemap_index", f"{SITE}/sitemap_index.xml")]

# DataImpulse username parameters: the first is introduced by `__`, further ones
# are separated by `;`, and each is `key.value`. Confirmed against their docs
# 2026-09-25 (`login__cr.de:password@gw.dataimpulse.com:823`).
COUNTRY = "in"


def _mask_user(user: str) -> str:
    """Show the parameter block, never the login."""
    if not user:
        return "?"
    head, sep, params = user.partition("__")
    masked = (head[:2] + "…") if len(head) > 2 else "…"
    return masked + (sep + params if sep else "")


def _india_gateway(gw: str) -> Optional[str]:
    """The same gateway, pinned to India. Returns None if it can't be built."""
    try:
        p = urlparse(gw)
        if not (p.username and p.hostname):
            return None
        user = p.username
        # Drop any country already pinned, or the provider would be handed
        # `cr.us;cr.in` and pick whichever it likes.
        if "__" in user:
            head, _, params = user.partition("__")
            keep = [p_ for p_ in params.split(";") if not p_.startswith("cr.")]
            user = head + "__" + ";".join(keep + [f"cr.{COUNTRY}"]) if keep \
                else head + f"__cr.{COUNTRY}"
        else:
            user = user + f"__cr.{COUNTRY}"
        # Deliberately NOT percent-encoded: `urlparse().username` does not
        # unquote, so quoting here would leave `;` as %3B for the provider AND
        # would double-encode a password that is already stored encoded. This
        # reassembles exactly the way scraper.sticky_gateway does, so the two can
        # be composed without one undoing the other.
        auth = user + (f":{p.password}" if p.password else "")
        netloc = f"{auth}@{p.hostname}" + (f":{p.port}" if p.port else "")
        return urlunparse((p.scheme, netloc, p.path, "", "", ""))
    except Exception:  # noqa: BLE001
        return None


def _routes() -> list:
    """[(label, proxy_url_or_None, shape_for_display)]"""
    gw = _core.proxy_gateway()
    out = [("direct", None, "—")]
    if not gw:
        return out
    sid = f"probe{random.randint(0, 10**6)}"
    tmpl = _core.get_setting("proxy_session_template", "") or DEFAULT_SESSION_TEMPLATE
    base = sticky_gateway(gw, sid, tmpl)
    out.append(("proxy", base, _mask_user(urlparse(base).username or "")))
    ingw = _india_gateway(gw)
    if ingw:
        inbase = sticky_gateway(ingw, sid, tmpl)
        out.append(("proxy IN", inbase, _mask_user(urlparse(inbase).username or "")))
    return out


def _fmt(status: int, nbytes: int, note: str = "") -> str:
    return f"{status} {nbytes//1024:>5} KB{note}"


def try_requests(url: str, proxy: Optional[str]) -> str:
    s = requests.Session()
    try:
        r = s.get(url, headers=sk_headers(),
                  proxies={"http": proxy, "https": proxy} if proxy else None,
                  timeout=30, allow_redirects=True)
        return _fmt(r.status_code, len(r.content or b""))
    except Exception as err:  # noqa: BLE001
        return f"ERR {type(err).__name__}: {str(err)[:44]}"
    finally:
        s.close()


def try_curl_cffi(url: str, proxy: Optional[str]) -> str:
    try:
        from curl_cffi import requests as creq
    except ImportError:
        return "curl_cffi not installed"
    try:
        # `impersonate="chrome"` tracks the newest Chrome target the installed
        # version knows. It replaces the ClientHello, the HTTP/2 settings AND
        # the default header order — so it is NOT just another header profile.
        r = creq.get(url, impersonate="chrome",
                     proxies={"http": proxy, "https": proxy} if proxy else None,
                     timeout=30)
        return _fmt(r.status_code, len(r.content or b""))
    except Exception as err:  # noqa: BLE001
        return f"ERR {type(err).__name__}: {str(err)[:44]}"


CLIENTS = [("requests", try_requests), ("curl_cffi", try_curl_cffi)]


def main() -> None:
    print(f"Shiksha probe #2 [BUILD {BUILD}]")
    try:
        import curl_cffi
        print(f"curl_cffi: {getattr(curl_cffi, '__version__', 'installed')}")
    except ImportError:
        print("curl_cffi: NOT INSTALLED — run `pip install curl_cffi` first, or "
              "this probe only repeats what probe #1 already showed.")
    routes = _routes()
    for label, _, shape in routes:
        if label != "direct":
            print(f"route {label:<9} user: {shape}")
    print()

    head = f"{'url':<14} {'client':<10} {'route':<10} result"
    print(head)
    print("-" * (len(head) + 14))
    for ulabel, url in URLS:
        for cname, fn in CLIENTS:
            for rlabel, proxy, _ in routes:
                print(f"{ulabel:<14} {cname:<10} {rlabel:<10} {fn(url, proxy)}")
                time.sleep(1.0)
        print()

    print("How to read this grid")
    print("  curl_cffi passes where requests fails  -> TLS FINGERPRINT. Add")
    print("     curl_cffi to requirements and swap the Shiksha fetcher onto it;")
    print("     nothing else about the vertical changes.")
    print("  only 'proxy IN' passes                 -> GEOGRAPHY. Pin the")
    print("     Shiksha gateway to India; discovery then costs the same.")
    print("  both needed together                   -> both, and that is fine:")
    print("     they are independent settings.")
    print("  nothing passes at all                  -> stop probing the front")
    print("     door. Next lever is apis.shiksha.com, which recon showed takes")
    print("     the same ?data=<base64> shape as Collegedunia's web-api.")


if __name__ == "__main__":
    sys.exit(main())
