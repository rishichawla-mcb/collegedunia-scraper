"""
Do Collegedunia's colleges actually EXIST inside Shiksha's? Measured, not inferred.

    python sk_overlap.py

Why this exists
---------------
On 2026-09-28 I answered "why 20k vs 57k?" with: Collegedunia's own site claims
20,500 and we hold 20,646, therefore our crawl is complete; Shiksha holds 57,751
against an AISHE census of ~60,033, therefore Shiksha covers the whole sector;
therefore ~37,000 Shiksha colleges have no Collegedunia counterpart.

Every "therefore" there is unearned. Matching totals says nothing about set
membership: our 20,646 could contain thousands Collegedunia does not list while
missing thousands it does, and both totals would still line up. That is the exact
error recorded in `finding-college-discovery-topology-2026-09-18.md` — "a summary
statistic consistent with more than one underlying story, treated as if it
identified one" — and I repeated it.

The only thing that settles it is matching the rows. This does that.

Method, and its limits
----------------------
There is no shared id between the two sites, so matching is on normalised name
tokens. That is inexact in both directions, so this reports a STRICT and a LOOSE
measure rather than one number, and prints samples of what failed to match on
each side so the reader can judge whether the rule or the data is at fault.

Two things are validated before any conclusion is drawn:

1. **Is a Shiksha slug a usable stand-in for its name?** Phase Ⓑ has only run for
   part of the inventory, so names exist for a subset; slugs exist for all
   57,751. Section 2 measures slug-vs-name agreement on the colleges where BOTH
   are known. If that agreement is poor, every number after it is worthless and
   the report says so.
2. **Does the matcher work at all?** Section 3 matches Collegedunia against
   ITSELF. A rule that cannot find a college in its own table cannot be trusted
   to find it in someone else's.

Read-only. Opens both databases, writes nothing.
"""
from __future__ import annotations

BUILD = "2026-09-28a"

import re
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple

import db as _core
import sk_db

# Dropped because they carry no discriminating power and differ by house style.
# NOT dropped: college, university, institute, school, polytechnic — those are
# exactly the words that distinguish one kind of institution from another, and
# dropping them is how a matcher starts equating a school with a university.
STOP = {"of", "the", "and", "for", "in", "at", "a", "an", "s"}
_NON = re.compile(r"[^a-z0-9]+")
_ID_SUFFIX = re.compile(r"-\d+$")


def tokens(*parts: str) -> Set[str]:
    out: Set[str] = set()
    for p in parts:
        if not p:
            continue
        for t in _NON.split(str(p).lower()):
            if t and t not in STOP and not t.isdigit():
                out.add(t)
    return out


def slug_tokens(slug: str) -> Set[str]:
    return tokens(_ID_SUFFIX.sub("", slug or ""))


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def load_collegedunia() -> Dict[int, Tuple[str, str]]:
    """{college_id: (name, city)} from the union of both domestic tables.

    The union, not the directory: `colleges` holds 453 institutions the directory
    has never seen (IIT Bombay among them), which is the finding from
    2026-09-18. Using either table alone would under-count the left-hand side and
    flatter the match rate."""
    out: Dict[int, Tuple[str, str]] = {}
    with _core.connect() as conn:
        for table in ("colleges_directory", "colleges"):
            try:
                rows = conn.execute(
                    f"SELECT college_id, name, "
                    f"{'city' if table == 'colleges_directory' else 'city'} "
                    f"FROM {table} WHERE COALESCE(name,'')<>''")
            except Exception as err:  # noqa: BLE001
                print(f"   ! {table}: {str(err)[:80]}")
                continue
            for cid, name, city in rows:
                prev = out.get(cid)
                # keep the longer name; directory names are often abbreviated
                if prev is None or len(str(name or "")) > len(prev[0]):
                    out[cid] = (str(name or ""), str(city or ""))
    return out


def load_shiksha() -> Tuple[Dict[int, str], Dict[int, Tuple[str, str]]]:
    """({id: slug}, {id: (name, city)}) — name/city only where phase Ⓑ has run.

    The city is carried deliberately. The Collegedunia side is always
    name + city, so comparing it against a Shiksha NAME alone penalises every
    college phase Ⓑ has reached — the fixture scored a correct pair 0.80 instead
    of 1.00 purely because one side knew the city and the other did not."""
    slugs: Dict[int, str] = {}
    named: Dict[int, Tuple[str, str]] = {}
    with sk_db.connect() as conn:
        for cid, slug, name, city in conn.execute(
                "SELECT college_id, COALESCE(slug,''), COALESCE(name,''), "
                "COALESCE(city,'') FROM sk_colleges"):
            if slug:
                slugs[int(cid)] = slug
            if name:
                named[int(cid)] = (name, city)
    return slugs, named


class Matcher:
    """Inverted index on the rarest token, so 20k x 57k stays tractable.

    Brute force is 1.2 billion pairs. Blocking on each left row's RAREST token
    keeps candidate lists small while guaranteeing that any pair sharing that
    token is considered — a pair sharing no token cannot reach the similarity
    threshold anyway, so nothing reachable is skipped."""

    def __init__(self, right: Dict[int, Set[str]]):
        self.right = right
        self.index: Dict[str, List[int]] = defaultdict(list)
        self.df: Counter = Counter()
        for rid, toks in right.items():
            for t in toks:
                self.index[t].append(rid)
                self.df[t] += 1

    def best(self, toks: Set[str], min_j: float) -> Tuple[int, float]:
        if not toks:
            return (0, 0.0)
        # cheapest blocking token first, then widen only if nothing is found
        ordered = sorted(toks, key=lambda t: self.df.get(t, 0))
        best_id, best_j = 0, 0.0
        seen: Set[int] = set()
        for t in ordered[:3]:
            for rid in self.index.get(t, ()):
                if rid in seen:
                    continue
                seen.add(rid)
                j = jaccard(toks, self.right[rid])
                if j > best_j:
                    best_id, best_j = rid, j
            if best_j >= min_j and len(seen) > 400:
                break
        return (best_id, best_j)


STRICT, LOOSE = 0.99, 0.60


def main() -> int:
    print(f"Collegedunia x Shiksha overlap [BUILD {BUILD}]")
    print("Read-only. Nothing is written.\n")

    cd = load_collegedunia()
    sk_slugs, sk_names = load_shiksha()
    print("1. Populations")
    print(f"   Collegedunia colleges (union of both tables) : {len(cd):,}")
    print(f"   Shiksha colleges with a slug                 : {len(sk_slugs):,}")
    print(f"   Shiksha colleges with a scraped name         : {len(sk_names):,}")
    if not cd or not sk_slugs:
        print("\n   One side is empty — nothing to measure.")
        return 1

    # -------------------------------------------------------------- 2
    print("\n2. Is a Shiksha SLUG a usable stand-in for its NAME?")
    print("   (checked only where phase Ⓑ has supplied a real name)")
    if not sk_names:
        print("   no scraped names yet — cannot validate. Everything below is")
        print("   therefore UNVALIDATED and should not be quoted.")
        agree = None
    else:
        js = []
        for cid, (name, city) in sk_names.items():
            js.append(jaccard(tokens(name, city), slug_tokens(sk_slugs.get(cid, ""))))
        js.sort()
        agree = sum(1 for j in js if j >= 0.6) / len(js)
        print(f"   sampled {len(js):,} colleges")
        print(f"   median name-vs-slug similarity : {js[len(js)//2]:.2f}")
        print(f"   >= 0.60 similar                : {agree*100:.1f}%")
        if agree < 0.8:
            print("   -> WEAK. The slug is not a reliable stand-in, so the")
            print("      overlap numbers below understate the true match.")
        else:
            print("   -> the slug carries the name. Using it for the colleges")
            print("      phase Ⓑ has not reached yet is sound.")

    # -------------------------------------------------------------- 3
    sk_toks = {cid: (tokens(*sk_names[cid]) if cid in sk_names
                     else slug_tokens(slug))
               for cid, slug in sk_slugs.items()}
    cd_toks = {cid: tokens(n, c) for cid, (n, c) in cd.items()}

    print("\n3. Control: can the matcher find Collegedunia inside ITSELF?")
    self_m = Matcher(cd_toks)
    sample = list(cd_toks.items())[:2000]
    hits = sum(1 for cid, t in sample if self_m.best(t, STRICT)[1] >= STRICT)
    print(f"   {hits:,}/{len(sample):,} of a 2,000 sample matched themselves "
          f"({100.0*hits/max(1,len(sample)):.1f}%)")
    if hits < len(sample) * 0.98:
        print("   -> the matcher is broken. Stop reading here.")
        return 1
    print("   -> the matcher works.")

    # -------------------------------------------------------------- 4
    print("\n4. How many Collegedunia colleges appear in Shiksha?")
    m = Matcher(sk_toks)
    strict = loose = 0
    unmatched: List[Tuple[int, str, str, float]] = []
    matched_pairs: List[Tuple[str, str, float]] = []
    for cid, t in cd_toks.items():
        rid, j = m.best(t, LOOSE)
        if j >= STRICT:
            strict += 1
        if j >= LOOSE:
            loose += 1
            if len(matched_pairs) < 10:
                matched_pairs.append((cd[cid][0], sk_slugs.get(rid, ""), j))
        else:
            if len(unmatched) < 15:
                unmatched.append((cid, cd[cid][0], cd[cid][1], j))
    n = len(cd_toks)
    print(f"   exact token match  (>= {STRICT:.2f}) : {strict:,}/{n:,} "
          f"({100.0*strict/n:.1f}%)")
    print(f"   close match        (>= {LOOSE:.2f}) : {loose:,}/{n:,} "
          f"({100.0*loose/n:.1f}%)")
    print("\n   sample matches:")
    for a, b, j in matched_pairs:
        print(f"     {j:.2f}  {a[:44]:<44} -> {b[:44]}")
    print("\n   sample Collegedunia colleges with NO Shiksha match:")
    for cid, nm, city, j in unmatched:
        print(f"     [{cid}] {nm[:46]:<46} {city[:16]:<16} best={j:.2f}")

    # -------------------------------------------------------------- 4b
    print("\n4b. And the direction the original claim was actually about:")
    print("    how many SHIKSHA colleges have a Collegedunia counterpart?")
    rm = Matcher(cd_toks)
    r_strict = r_loose = 0
    sk_only: List[Tuple[int, str, float]] = []
    for cid, t in sk_toks.items():
        rid, j = rm.best(t, LOOSE)
        if j >= STRICT:
            r_strict += 1
        if j >= LOOSE:
            r_loose += 1
        elif len(sk_only) < 15:
            sk_only.append((cid, sk_slugs.get(cid, ""), j))
    sn = len(sk_toks)
    print(f"   exact token match  (>= {STRICT:.2f}) : {r_strict:,}/{sn:,} "
          f"({100.0*r_strict/sn:.1f}%)")
    print(f"   close match        (>= {LOOSE:.2f}) : {r_loose:,}/{sn:,} "
          f"({100.0*r_loose/sn:.1f}%)")
    print(f"   Shiksha-only (no Collegedunia match) : {sn - r_loose:,}")
    print("\n   sample Shiksha colleges with NO Collegedunia match:")
    for cid, slug, j in sk_only:
        print(f"     [{cid}] {slug[:60]:<60} best={j:.2f}")

    # -------------------------------------------------------------- 5
    print("\n5. What are the Shiksha colleges Collegedunia does not have?")
    with sk_db.connect() as conn:
        no_off = {r[0] for r in conn.execute(
            "SELECT c.college_id FROM sk_colleges c WHERE NOT EXISTS "
            "(SELECT 1 FROM sk_offerings o WHERE o.college_id=c.college_id)")}
    only_ids = {cid for cid, t in sk_toks.items()
                if rm.best(t, LOOSE)[1] < LOOSE}

    def profile(ids, label):
        b = Counter()
        for cid in ids:
            sl = (sk_slugs.get(cid, "") or "").lower()
            for kw in ("iti", "polytechnic", "school", "academy", "training",
                       "coaching", "university", "institute", "college"):
                if kw in sl:
                    b[kw] += 1
                    break
            else:
                b["(other)"] += 1
        tot = max(1, len(ids))
        print(f"   {label} (n={len(ids):,}):")
        for kw, cnt in b.most_common():
            print(f"     {kw:<12} {cnt:>8,}  {100.0*cnt/tot:5.1f}%")
        empty = len([c for c in ids if c in no_off])
        print(f"     no offerings {empty:>8,}  {100.0*empty/tot:5.1f}%")

    profile(set(sk_toks) - only_ids, "MATCHED to Collegedunia")
    print()
    profile(only_ids, "SHIKSHA-ONLY")
    print("\n   If the two profiles look alike, 'Shiksha covers the long tail'")
    print("   is not supported — the unmatched colleges would then be the same")
    print("   kind of institution as the matched ones, and the difference is")
    print("   something else (or the matcher is missing them).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
