"""
Why does Shiksha list 57,751 colleges and Collegedunia 20,306? Read the rows.

    python sk_gap.py              # the profile
    python sk_gap.py samples 40   # more example names per bucket

The question has been answered twice before from TOTALS — 20,306 vs 57,751, or
20,306 vs AISHE's 48,246 — and both times the answer was arithmetic dressed as a
finding. Two populations of different size tell you nothing about which rows are
in which. `finding-college-discovery-topology-2026-09-18` records the rule this
breaks: *a summary statistic consistent with more than one underlying story,
treated as if it identified one. Read the rows.*

So this splits the 57,751 into three buckets using the evidence-based match set,
and profiles them side by side.

  matched    at least one accepted pair in sk_matches (website / phone / email /
             shortform / strong name). This college IS on Collegedunia.
  pending    candidate pairs exist but none is accepted — the judgement band.
             Membership unknown.
  none       no candidate pair at all.

WHAT `none` DOES NOT MEAN
-------------------------
It does not mean "absent from Collegedunia". It means "our matcher found no
candidate", and the matcher needs a signal to work with. A Shiksha college with
no website, no phone, no email and only a slug for a name can only be reached by
the name tier, which is the weakest. So the profile below reports, for every
bucket, HOW MUCH SIGNAL its members carry. If `none` is mostly rows with no
contact details and no phase Ⓑ name, the honest reading is "we cannot tell",
not "Collegedunia is missing 37,000 colleges".

That control is the point of this tool. Without it the three counts are just
another summary statistic.

Read-only. Writes nothing.
"""
from __future__ import annotations

BUILD = "2026-09-29a"

import sys
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import sk_db

BUCKETS = ("matched", "pending", "none")


def _cut() -> float:
    """Only the latest build's rows count. A pair an earlier build accepted and
    this one dropped must not make a college look matched."""
    return float(sk_db.get_setting("last_match_build_at") or 0)


def load() -> Tuple[Dict[int, str], Dict[int, Dict[str, Any]]]:
    cut = _cut()
    where = "decided_at >= ?" if cut else "1=1"
    args: Tuple = (cut,) if cut else ()
    bucket: Dict[int, str] = {}
    rows: Dict[int, Dict[str, Any]] = {}
    with sk_db.connect() as conn:
        for r in conn.execute(
                "SELECT college_id, name, slug, city, state, college_type, "
                "       ownership, established, website, email, phone, rating, "
                "       reviews_count, base_course_count, detail_scraped_at "
                "FROM sk_colleges"):
            rows[int(r["college_id"])] = dict(r)
            bucket[int(r["college_id"])] = "none"
        for (ski,) in conn.execute(
                "SELECT DISTINCT sk_college_id FROM sk_matches "
                f"WHERE verdict='pending' AND {where}", args):
            if int(ski) in bucket:
                bucket[int(ski)] = "pending"
        # 'yes' last, so it wins over 'pending' for a college holding both
        for (ski,) in conn.execute(
                "SELECT DISTINCT sk_college_id FROM sk_matches "
                f"WHERE verdict='yes' AND {where}", args):
            if int(ski) in bucket:
                bucket[int(ski)] = "matched"
    return bucket, rows


def pct(n: int, d: int) -> str:
    return "%5.1f%%" % (100.0 * n / d) if d else "    —"


def table(title: str, counts: Dict[str, Counter], totals: Dict[str, int],
          keys: List[str], key_label: str = "") -> None:
    if title:
        print("\n%s" % title)
    print("   %-34s %18s %18s %18s" % (key_label, "matched", "pending", "none"))
    for k in keys:
        cells = []
        for b in BUCKETS:
            n = counts[b].get(k, 0)
            cells.append("%7s %s" % (f"{n:,}", pct(n, totals[b])))
        print("   %-34s %s" % (str(k)[:34], " ".join("%18s" % c for c in cells)))


def main(argv: List[str]) -> int:
    sk_db.init_db()
    n_samples = int(argv[1]) if len(argv) > 1 and argv[0] == "samples" else 8
    bucket, rows = load()
    totals = Counter(bucket.values())
    n = len(rows)

    print("Shiksha vs Collegedunia — where the 57,751 sit [BUILD %s]" % BUILD)
    if not _cut():
        print("  ! no match build recorded. Run `python sk_match.py build` first;")
        print("    without it every college reads as 'none'.")
    print("\n1. the three buckets")
    for b in BUCKETS:
        print("   %-9s %8s  %s" % (b, f"{totals[b]:,}", pct(totals[b], n)))
    print("   %-9s %8s" % ("TOTAL", f"{n:,}"))
    if sum(totals.values()) != n:
        print("   ! buckets do not sum to the inventory — do not trust the rest")
        return 1

    # ---------------------------------------------------------------- control
    # Before any story about WHICH colleges are missing, ask whether the matcher
    # could have seen them at all.
    print("\n2. CONTROL — how much identifying signal each bucket carries")
    print("   A college with no website, phone or email can only be reached by")
    print("   the weakest tier. If 'none' is mostly signal-less rows, the honest")
    print("   reading is 'we cannot tell', not 'Collegedunia is missing them'.")
    sig: Dict[str, Counter] = {b: Counter() for b in BUCKETS}
    for cid, b in bucket.items():
        r = rows[cid]
        sig[b]["has website"] += 1 if (r.get("website") or "").strip() else 0
        sig[b]["has phone"] += 1 if (r.get("phone") or "").strip() else 0
        sig[b]["has email"] += 1 if (r.get("email") or "").strip() else 0
        sig[b]["has a real name (phase Ⓑ)"] += 1 if (r.get("name") or "").strip() else 0
        sig[b]["phase Ⓑ done"] += 1 if r.get("detail_scraped_at") else 0
        sig[b]["NO contact signal at all"] += 0 if (
            (r.get("website") or "").strip() or (r.get("phone") or "").strip()
            or (r.get("email") or "").strip()) else 1
    table("", sig, totals,
          ["phase Ⓑ done", "has a real name (phase Ⓑ)", "has website",
           "has phone", "has email", "NO contact signal at all"], "signal")

    # The subset that actually answers the question. A college the matcher could
    # see clearly — a published website or phone, a name from phase Ⓑ — and for
    # which it still found no candidate anywhere in Collegedunia's 20,306 is the
    # strongest evidence of genuine absence. Everything else in `none` is a
    # college we could not look up properly.
    strong = [c for c, b in bucket.items()
              if b == "none"
              and (rows[c].get("website") or "").strip()
              and (rows[c].get("name") or "").strip()]
    weak = totals["none"] - len(strong)
    print("\n   THE NUMBER THAT ANSWERS THE QUESTION")
    print("   %8s Shiksha colleges have a website AND a name AND no candidate" % f"{len(strong):,}")
    print("            pair anywhere in Collegedunia — searchable, and not found.")
    print("   %8s of the 'none' bucket could not be looked up properly (no" % f"{weak:,}")
    print("            website, or no name yet from phase Ⓑ). Unknown, not absent.")

    # ---------------------------------------------------------------- profile
    def cross(field: str, title: str, top: int = 10, bucketer=None) -> None:
        c: Dict[str, Counter] = {b: Counter() for b in BUCKETS}
        seen: Counter = Counter()
        for cid, b in bucket.items():
            v = rows[cid].get(field)
            v = bucketer(v) if bucketer else ((str(v).strip() or "(blank)")
                                              if v not in (None, "") else "(blank)")
            c[b][v] += 1
            seen[v] += 1
        table(title, c, totals, [k for k, _ in seen.most_common(top)], field)

    def size_bucket(v) -> str:
        try:
            i = int(v)
        except (TypeError, ValueError):
            return "(unknown)"
        if i <= 0:
            return "0 base courses"
        if i == 1:
            return "1"
        if i <= 3:
            return "2-3"
        if i <= 9:
            return "4-9"
        return "10+"

    def rev_bucket(v) -> str:
        try:
            i = int(v or 0)
        except (TypeError, ValueError):
            return "(unknown)"
        if i == 0:
            return "no reviews"
        if i <= 5:
            return "1-5"
        if i <= 25:
            return "6-25"
        return "26+"

    print("\n3. profile — matched vs pending vs none")
    cross("college_type", "   by college type")
    cross("ownership", "   by ownership")
    cross("state", "   by state (top 12)", top=12)
    cross("base_course_count", "   by how many BASE COURSES the college runs",
          bucketer=size_bucket)
    cross("reviews_count", "   by review count", bucketer=rev_bucket)

    # ------------------------------------------------------- what they teach
    print("\n4. the base courses each bucket actually offers (top 12)")
    bc: Dict[str, Counter] = {b: Counter() for b in BUCKETS}
    with sk_db.connect() as conn:
        for cid, nm in conn.execute(
                "SELECT college_id, name FROM sk_college_base_courses"):
            b = bucket.get(int(cid))
            if b and nm:
                bc[b][str(nm)] += 1
    seen = Counter()
    for b in BUCKETS:
        seen.update(bc[b])
    denom = {b: max(1, sum(bc[b].values())) for b in BUCKETS}
    print("   %-34s %18s %18s %18s" % ("base course", "matched", "pending", "none"))
    for k, _ in seen.most_common(12):
        cells = ["%7s %s" % (f"{bc[b].get(k,0):,}", pct(bc[b].get(k, 0), denom[b]))
                 for b in BUCKETS]
        print("   %-34s %s" % (k[:34], " ".join("%18s" % c for c in cells)))

    # ------------------------------------------------------------- samples
    print("\n5. samples")
    for b in BUCKETS:
        names = [(rows[c].get("name") or rows[c].get("slug") or "")
                 for c in bucket if bucket[c] == b]
        names = [x for x in names if x][:n_samples]
        print("\n   --- %s ---" % b)
        for x in names:
            print("     %s" % x[:90])

    print("\n6. what this does and does not establish")
    print("   'matched' IS on Collegedunia — the evidence says so.")
    print("   'none' means our matcher found no candidate, which is an upper")
    print("   bound on absence, not a measurement of it. Read section 2 before")
    print("   quoting any number from section 1.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
