"""
Collegedunia college  <->  Shiksha college, matched on EVIDENCE, tier by tier.

    python sk_match.py build            # compute and store candidate matches
    python sk_match.py report           # what was matched, by tier and strength
    python sk_match.py export [n]       # write the ambiguous band for judging
    python sk_match.py apply <file>     # read judged verdicts back in
    python sk_match.py verify [n]       # fetch live pages to settle a sample

The brief was "every comparison goes through your brain". Two honest limits on
that, stated before the design rather than after:

**1. Nothing on Render can call a model.** The deployed app has no API key and no
model access. A judged comparison therefore either goes out to an LLM API (a key
and a bill someone has to agree to) or comes back through this conversation in
batches. This tool supports both by writing the ambiguous band to a file and
reading verdicts back — `export` then `apply`.

**2. Judging all of it is the wrong thing to want.** 20,646 x 57,751 is 1.2
billion pairs. Even after blocking, most pairs do not need judgement: when both
sites publish the SAME WEBSITE DOMAIN for a college, that is not a similarity
score, it is the institution telling you its own identity. No model improves on
it. Reasoning is worth spending where the evidence is genuinely ambiguous, and
that band is what `export` produces.

So the tiers below run cheapest-and-strongest first, and each match records which
evidence produced it — because "these are the same college" means something very
different when it comes from a shared domain than from two names looking alike.

  tier 1  website domain   both sites publish the same registrable domain
  tier 2  phone            same last-10 digits
  tier 3  email domain     same domain in the contact email
  tier 4  shortform+city   both publish the abbreviation (IIT-B, AIIMS...) and agree on city
  tier 5  name+city        token-set similarity, the weakest signal
          --------         below the accept line: exported for judgement

Tiers 1-3 need phase Ⓑ data on the Shiksha side (website/phone/email come from
the detail crawl), so their reach grows as that crawl progresses. `report` says
how much of the inventory each tier could even see.

Writes only `sk_matches`, in the Shiksha database. The Collegedunia file is never
opened for writing.
"""
from __future__ import annotations

BUILD = "2026-09-28a"

import json
import re
import sys
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Set, Tuple

import db as _core
import sk_db

STOP = {"of", "the", "and", "for", "in", "at", "a", "an", "s"}
_NON = re.compile(r"[^a-z0-9]+")
_ID_SUFFIX = re.compile(r"-\d+$")
_DIGITS = re.compile(r"\D+")

# Free email/domain hosts carry no identity: a college using gmail tells you
# nothing about which college it is, and matching on it would merge thousands.
JUNK_DOMAINS = {
    "gmail.com", "yahoo.com", "yahoo.co.in", "hotmail.com", "outlook.com",
    "rediffmail.com", "live.com", "aol.com", "icloud.com", "protonmail.com",
    "googlemail.com", "ymail.com", "mail.com",
}
# Aggregator/CDN domains a college page may cite that are not the college.
JUNK_SITE_DOMAINS = JUNK_DOMAINS | {
    "shiksha.com", "collegedunia.com", "facebook.com", "wikipedia.org",
    "google.com", "youtube.com", "linkedin.com", "twitter.com", "x.com",
    "instagram.com", "blogspot.com", "wordpress.com", "wixsite.com",
}

# Evidence strength, strongest first. This is the ONLY thing that may decide a
# pair's `tier`, and it exists because scores are not comparable across tiers.
#
# The bug it fixes (found 2026-09-28, reproduced before being fixed): `add()`
# replaced the tier whenever a later tier scored higher. A website match with
# 2-3 candidates scores 0.9; a name match can score 0.97. So the pair
#
#   CD "Indian Institute of Technology Bombay" <-> SK "Indian Institute of
#   Technology Bombay", agreeing on BOTH iitb.ac.in and the name
#
# was stored as tier='name' — the weakest label — while the far weaker pair
#
#   CD "IIT Bombay" <-> SK "Shailesh J Mehta School of Management", sharing
#   only the domain, name_jaccard 0.1
#
# kept tier='website'. The column was inverted for exactly the pairs that
# mattered most, so `WHERE tier='website'` returned the weak one and missed the
# strong one. The evidence was never lost (it was in `also`), but the headline
# label — the one the report groups by and a consumer would filter on — was
# wrong, which defeats the whole point of recording which evidence produced a
# match.
TIER_RANK = {"website": 5, "phone": 4, "email": 3, "shortform": 2, "name": 1}

ACCEPT_NAME = 0.85      # name+city similarity accepted without judgement
JUDGE_FLOOR = 0.45      # below this, not even worth a judgement
SHORTFORM_MIN = 3       # "IIT" yes, "IT" no — two letters collide constantly
# A website/phone/email published by more than this many rows on EITHER side
# belongs to a group, not to a college, and cannot identify one.
MAX_SHARERS = 3


# ---------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------
def tokens(*parts: str) -> Set[str]:
    """Name + city as a token set, with runs of initials joined into one token.

    Joining the initials is load-bearing. Splitting on punctuation turns
    "R.R. Group of Institutions" into ['r','r','group','institutions'] and a SET
    collapses the repeat to {'r', group, institutions}; "S.R Group of
    Institutions" gives ['s','r',...] and 's' is a stopword (it is there to strip
    possessives), leaving {'r', group, institutions} — the SAME set. The two
    scored a Jaccard of 1.00 and were accepted as one college. They are not:
    R.R. Group and S.R Group are different institutions in Lucknow, and the live
    conflict report of 2026-09-28 shows the matcher claiming otherwise.

    Coalescing first gives 'rr' and 'sr', which differ, and incidentally makes
    the token agree with the shortform tier's key. "B. N. M. Institute" becomes
    {bnm, institute}, which is what a reader would call it.
    """
    out: Set[str] = set()
    for p in parts:
        if not p:
            continue
        raw = [t for t in _NON.split(str(p).lower()) if t and not t.isdigit()]
        merged: List[str] = []
        run: List[str] = []
        for t in raw:
            if len(t) == 1:
                run.append(t)
                continue
            if run:
                merged.append("".join(run))
                run = []
            merged.append(t)
        if run:
            merged.append("".join(run))
        for t in merged:
            # The stopword list applies to WORDS. A joined initialism is not one,
            # so "s" is dropped from "St. Xavier's" but kept inside "sr".
            if t not in STOP:
                out.add(t)
    return out


def slug_tokens(slug: str) -> Set[str]:
    return tokens(_ID_SUFFIX.sub("", slug or ""))


def jaccard(a: Set[str], b: Set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def domain(url_or_email: Any) -> str:
    """The registrable-ish domain, lowercased, `www.` stripped.

    Deliberately crude — no public-suffix list — because the failure mode that
    matters is merging two different colleges, and that is guarded by the junk
    list, not by suffix precision."""
    s = str(url_or_email or "").strip().lower()
    if not s:
        return ""
    if "@" in s:
        s = s.rsplit("@", 1)[-1]
    s = re.sub(r"^[a-z]+://", "", s)
    s = s.split("/")[0].split("?")[0].split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s if "." in s and len(s) > 4 else ""


def phone_key(v: Any) -> str:
    """Last 10 digits. India's numbers are 10 long; the country code and any
    0/+91/(0) prefix vary by who typed it in."""
    d = _DIGITS.sub("", str(v or ""))
    return d[-10:] if len(d) >= 10 else ""


def shortform_key(v: Any) -> str:
    s = _NON.sub("", str(v or "").lower())
    return s if len(s) >= SHORTFORM_MIN else ""


# ---------------------------------------------------------------------------
# loading
# ---------------------------------------------------------------------------
class Side:
    def __init__(self) -> None:
        self.name: Dict[int, str] = {}
        self.city: Dict[int, str] = {}
        self.toks: Dict[int, Set[str]] = {}
        self.site: Dict[int, str] = {}
        self.phone: Dict[int, str] = {}
        self.email: Dict[int, str] = {}
        self.short: Dict[int, str] = {}


def load_cd() -> Side:
    """Union of both domestic tables. `colleges` holds 453 institutions the
    directory has never seen (IIT Bombay among them) — the 2026-09-18 finding —
    so either table alone would under-count the left-hand side."""
    s = Side()
    with _core.connect() as conn:
        try:
            for cid, nm, sf, city in conn.execute(
                    "SELECT college_id, COALESCE(name,''), COALESCE(short_form,''), "
                    "COALESCE(city,'') FROM colleges_directory"):
                cid = int(cid)
                s.name[cid] = nm
                s.city[cid] = city
                if sf:
                    s.short[cid] = shortform_key(sf)
        except Exception as err:  # noqa: BLE001
            print(f"   ! colleges_directory: {str(err)[:70]}")
        try:
            for cid, nm, sf, city, web, mail, ph in conn.execute(
                    "SELECT college_id, COALESCE(name,''), COALESCE(short_form,''), "
                    "COALESCE(city,''), COALESCE(website,''), COALESCE(email,''), "
                    "COALESCE(phone,'') FROM colleges"):
                cid = int(cid)
                if len(nm) > len(s.name.get(cid, "")):
                    s.name[cid] = nm
                if city and not s.city.get(cid):
                    s.city[cid] = city
                if sf and not s.short.get(cid):
                    s.short[cid] = shortform_key(sf)
                d = domain(web)
                if d and d not in JUNK_SITE_DOMAINS:
                    s.site[cid] = d
                e = domain(mail)
                if e and e not in JUNK_DOMAINS:
                    s.email[cid] = e
                p = phone_key(ph)
                if p:
                    s.phone[cid] = p
        except Exception as err:  # noqa: BLE001
            print(f"   ! colleges: {str(err)[:70]}")
    for cid, nm in s.name.items():
        s.toks[cid] = tokens(nm, s.city.get(cid, ""))
    return s


def load_sk() -> Side:
    s = Side()
    with sk_db.connect() as conn:
        for (cid, slug, nm, short, city, web, mail, ph) in conn.execute(
                "SELECT college_id, COALESCE(slug,''), COALESCE(name,''), "
                "COALESCE(short_name,''), COALESCE(city,''), COALESCE(website,''), "
                "COALESCE(email,''), COALESCE(phone,'') FROM sk_colleges"):
            cid = int(cid)
            s.name[cid] = nm or slug
            s.city[cid] = city
            # name+city where phase Ⓑ has reached; the slug otherwise. The slug
            # already contains the city, so the two forms are comparable.
            s.toks[cid] = tokens(nm, city) if nm else slug_tokens(slug)
            if short:
                k = shortform_key(short)
                # Shiksha repeats the full name in short_name when there is no
                # real abbreviation; that is not a short form.
                if k and k != shortform_key(nm):
                    s.short[cid] = k
            d = domain(web)
            if d and d not in JUNK_SITE_DOMAINS:
                s.site[cid] = d
            e = domain(mail)
            if e and e not in JUNK_DOMAINS:
                s.email[cid] = e
            p = phone_key(ph)
            if p:
                s.phone[cid] = p
    return s


# ---------------------------------------------------------------------------
# matching
# ---------------------------------------------------------------------------
class Index:
    """Blocking on the rarest token. A pair sharing no token cannot reach the
    threshold, so nothing reachable is skipped by only considering pairs that
    share one."""

    def __init__(self, toks: Dict[int, Set[str]]):
        self.toks = toks
        self.idx: Dict[str, List[int]] = defaultdict(list)
        self.df: Counter = Counter()
        for i, t in toks.items():
            for w in t:
                self.idx[w].append(i)
                self.df[w] += 1

    def blocking_tokens(self, q: Set[str], n: int = 3) -> List[str]:
        """The n rarest tokens, chosen deterministically.

        The tiebreaker `w` is load-bearing, not tidiness. `q` is a SET, and
        sorting a set by document frequency alone leaves every tie broken by
        set iteration order — which follows string hashing, which Python
        randomises per process. Two builds over IDENTICAL data therefore chose
        different blocking tokens, scanned different candidate pools, and found
        a different `best` match for a handful of Collegedunia rows.

        Measured 2026-09-28: four tokens all with df=1, sorted across five
        PYTHONHASHSEED values, gave five different answers
        (['beta','delta','gamma'], ['delta','gamma','alpha'], …). Two
        consecutive builds on unchanged input wrote 22,599 pairs each yet left
        22,604 rows in the table — five pairs appeared in one run and not the
        other. Small, but it means the judgement queue is not reproducible: a
        pair someone judged can be replaced by a different pair on the next
        build."""
        return sorted(q, key=lambda w: (self.df.get(w, 0), w))[:n]

    def best(self, q: Set[str], cap: int = 600) -> Tuple[int, float]:
        if not q:
            return (0, 0.0)
        best_i, best_j, seen = 0, 0.0, set()
        for w in self.blocking_tokens(q):
            for i in self.idx.get(w, ()):
                if i in seen:
                    continue
                seen.add(i)
                j = jaccard(q, self.toks[i])
                if j > best_j:
                    best_i, best_j = i, j
            if len(seen) > cap:
                break
        return (best_i, best_j)


def _invert(d: Dict[int, str]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = defaultdict(list)
    for k, v in d.items():
        if v:
            out[v].append(k)
    return out


def build(db_path: str = None) -> int:
    print(f"Collegedunia x Shiksha matcher [BUILD {BUILD}]")
    cd, sk = load_cd(), load_sk()
    print("\n1. Populations and signal reach")
    print(f"   Collegedunia colleges : {len(cd.name):,}")
    print(f"   Shiksha colleges      : {len(sk.name):,}")
    for label, a, b in (("website domain", cd.site, sk.site),
                        ("phone", cd.phone, sk.phone),
                        ("email domain", cd.email, sk.email),
                        ("short form", cd.short, sk.short)):
        print(f"   {label:<15} known for {len(a):>7,} CD / {len(b):>7,} SK")
    print("   (the Shiksha side of the top three grows as phase Ⓑ progresses)")

    matches: Dict[Tuple[int, int], Dict[str, Any]] = {}

    def add(cdi: int, ski: int, tier: str, score: float, ev: Dict[str, Any],
            verdict: str):
        key = (cdi, ski)
        prev = matches.get(key)
        if prev is None:
            matches[key] = {"score": score, "tier": tier, "verdict": verdict,
                            "evidence": ev}
            return
        # Never LOSE the weaker evidence: a pair agreeing on website AND phone
        # AND name is a different claim from one agreeing on name alone.
        merged = dict(prev["evidence"])
        merged.update(ev)
        also = set(merged.get("also") or [])
        also.add(prev["tier"])
        also.add(tier)
        # `tier` follows TIER_RANK, never the score. See the note on TIER_RANK:
        # ranking by score relabelled the strongest matches as the weakest.
        if TIER_RANK.get(tier, 0) > TIER_RANK.get(prev["tier"], 0):
            prev["tier"] = tier
        # `score` stays the best across all the evidence — a pair agreeing on
        # two things is more confident than one agreeing on either alone — so
        # `tier` names the strongest EVIDENCE and `score` is the confidence.
        if score > prev["score"]:
            prev["score"] = score
        # A verdict is never downgraded. Once any evidence says 'yes' the pair
        # is accepted; weaker evidence arriving later cannot push it back into
        # the judgement queue.
        if verdict == "yes":
            prev["verdict"] = "yes"
        merged["also"] = sorted(also - {prev["tier"]})
        prev["evidence"] = merged

    # ---- tiers 1-3: identity the institutions publish about themselves ----
    for tier, cdmap, skmap in (("website", cd.site, sk.site),
                               ("phone", cd.phone, sk.phone),
                               ("email", cd.email, sk.email)):
        inv = _invert(skmap)
        cd_inv = _invert(cdmap)
        hits = dropped = 0
        for cdi, val in cdmap.items():
            cands = inv.get(val) or []
            # A domain shared by many Shiksha rows is a university's rows, or
            # junk we failed to list. Either way it is not identifying.
            if not cands or len(cands) > MAX_SHARERS:
                continue
            # The SAME test on the Collegedunia side, which was missing. It
            # capped how many SHIKSHA rows could share a value but not how many
            # COLLEGEDUNIA rows, so a university's whole group was accepted
            # against one Shiksha college. The live conflict report of
            # 2026-09-28 is full of it: five Maharishi Markandeshwar colleges
            # (Pharmacy, Engineering, Nursing, Computer Tech, the university)
            # all claiming SK 4274 on one domain; nine Uka Tarsadia colleges
            # claiming SK 101689 on one email; fifteen Collegedunia rows on a
            # single Shiksha college at the widest.
            #
            # Dropping rather than accepting is right: the evidence is real but
            # not IDENTIFYING, and the name tier can still tell the group's
            # members apart. Keeping it meant a confident 'yes' on a claim the
            # evidence never supported.
            if len(cd_inv.get(val) or ()) > MAX_SHARERS:
                dropped += 1
                continue
            for ski in cands:
                nj = jaccard(cd.toks.get(cdi, set()), sk.toks.get(ski, set()))
                add(cdi, ski, tier, 1.0 if len(cands) == 1 else 0.9,
                    {tier: val, "name_jaccard": round(nj, 2)}, "yes")
                hits += 1
        print(f"\n2.{tier:<9} matched {hits:,} pairs"
              + (f"  ({dropped:,} skipped: the value is shared by more than "
                 f"{MAX_SHARERS} Collegedunia rows, so it does not identify)"
                 if dropped else ""))

    # ---- tier 4: abbreviations, which both sites publish ----
    inv_short = _invert(sk.short)
    hits = 0
    for cdi, sf in cd.short.items():
        for ski in (inv_short.get(sf) or [])[:8]:
            if cd.city.get(cdi) and sk.city.get(ski):
                if tokens(cd.city[cdi]) & tokens(sk.city[ski]):
                    add(cdi, ski, "shortform", 0.95,
                        {"short_form": sf, "city": cd.city[cdi]}, "yes")
                    hits += 1
    print(f"2.shortform matched {hits:,} pairs")

    # ---- tier 5: names, the weakest signal ----
    idx = Index(sk.toks)
    acc = judge = 0
    for cdi, t in cd.toks.items():
        ski, j = idx.best(t)
        if not ski or j < JUDGE_FLOOR:
            continue
        if j >= ACCEPT_NAME:
            add(cdi, ski, "name", j, {"name_jaccard": round(j, 2)}, "yes")
            acc += 1
        else:
            add(cdi, ski, "name", j, {"name_jaccard": round(j, 2)}, "pending")
            judge += 1
    print(f"2.name      accepted {acc:,} (>= {ACCEPT_NAME}), "
          f"{judge:,} sent for judgement ({JUDGE_FLOOR}-{ACCEPT_NAME})")

    now = time.time()
    rows = [(cdi, ski, m["score"], m["tier"],
             json.dumps(m["evidence"], ensure_ascii=False), m["verdict"],
             "auto", None, now)
            for (cdi, ski), m in matches.items()]
    with sk_db.connect() as conn:
        conn.executemany(
            "INSERT INTO sk_matches(cd_college_id,sk_college_id,score,tier,"
            "evidence,verdict,decided_by,note,decided_at) VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(cd_college_id,sk_college_id) DO UPDATE SET "
            "score=excluded.score, tier=excluded.tier, evidence=excluded.evidence, "
            "verdict=CASE WHEN sk_matches.decided_by IN ('judge','human') "
            "  THEN sk_matches.verdict ELSE excluded.verdict END, "
            "decided_at=excluded.decided_at", rows)
    print(f"\n3. wrote {len(rows):,} candidate pairs to sk_matches")
    print("   (a verdict already set by a judge or a human is never overwritten)")
    # Rows this build did not touch are leftovers from an earlier one. Nothing
    # is deleted — the owner's standing rule — but they are NAMED, because
    # otherwise the report's totals quietly exceed what the build produced and
    # the arithmetic stops adding up. This is how the non-determinism above was
    # caught: 22,599 written, 22,604 counted.
    with sk_db.connect() as conn:
        stale = conn.execute("SELECT COUNT(*) FROM sk_matches WHERE decided_at < ?",
                             (now,)).fetchone()[0]
    if stale:
        print(f"   ! {stale:,} row(s) in sk_matches were NOT produced by this "
              f"build — leftovers from an earlier run, kept, not deleted.")
        print(f"     list them: SELECT * FROM sk_matches WHERE decided_at < {now:.0f}")
    return 0


# ---------------------------------------------------------------------------
def report() -> int:
    cd_n = len(load_cd().name)
    with sk_db.connect() as conn:
        print(f"Match report [BUILD {BUILD}]\n")
        print("by tier and verdict:")
        total = 0
        for r in conn.execute(
                "SELECT tier, verdict, COUNT(*), ROUND(AVG(score),3) "
                "FROM sk_matches GROUP BY tier, verdict ORDER BY 3 DESC"):
            print(f"   {r[0]:<11} {r[1]:<8} {r[2]:>8,}  avg score {r[3]}")
            total += r[2]
        # Print the total. Its absence is what let 22,599-written / 22,604-stored
        # go unnoticed until someone added the column up by hand.
        last = conn.execute("SELECT MAX(decided_at) FROM sk_matches").fetchone()[0]
        fresh = conn.execute("SELECT COUNT(*) FROM sk_matches WHERE decided_at >= ?",
                             (float(last or 0) - 60,)).fetchone()[0]
        print(f"   {'TOTAL':<11} {'':<8} {total:>8,}")
        if fresh and fresh != total:
            print(f"   ! only {fresh:,} of these came from the latest build; "
                  f"{total - fresh:,} are leftovers from an earlier one.")
            print(f"     They are kept, not deleted. To see them:")
            print(f"     SELECT * FROM sk_matches WHERE decided_at < "
                  f"{float(last or 0) - 60:.0f}")
        yes_cd = conn.execute(
            "SELECT COUNT(DISTINCT cd_college_id) FROM sk_matches "
            "WHERE verdict='yes'").fetchone()[0]
        pend = conn.execute("SELECT COUNT(*) FROM sk_matches "
                            "WHERE verdict='pending'").fetchone()[0]
        dupe = conn.execute(
            "SELECT COUNT(*) FROM (SELECT sk_college_id FROM sk_matches "
            "WHERE verdict='yes' GROUP BY sk_college_id HAVING COUNT(*)>1)"
        ).fetchone()[0]
        print(f"\nCollegedunia colleges with >=1 accepted match: "
              f"{yes_cd:,} / {cd_n:,} ({100.0*yes_cd/max(1,cd_n):.1f}%)")
        print(f"awaiting judgement                           : {pend:,}")
        print(f"Shiksha colleges claimed by >1 Collegedunia row: {dupe:,}")
        if dupe:
            print("   ^ these are conflicts and need resolving before the")
            print("     match set is used for anything.")
    return 0


CD_DUP = 0.80        # two Collegedunia names this alike are the same college


def conflicts(limit: int = 12) -> int:
    """Characterise the Shiksha colleges claimed by more than one Collegedunia
    row, instead of counting them.

    1,304 collisions is a single number consistent with several different
    stories, and they do not want the same treatment:

      cd-duplicate    the two Collegedunia rows are the SAME college, listed
                      twice. Then both matches are correct and the conflict is a
                      fact about Collegedunia's own inventory, not a matcher
                      error. Resolution: keep both, and note the CD duplicate.
      shared-signal   the claimants matched on the SAME value — one switchboard
                      number, one university-wide email domain. The signal is
                      not identifying here, whatever tier it sits in.
      parent-child    one claimant's name contains the other's (a university and
                      its department). Different institutions; the narrower one
                      usually belongs elsewhere.
      ambiguous       none of the above. These need a judgement.

    Read-only: prints, writes nothing.
    """
    cd, sk = load_cd(), load_sk()
    groups: Dict[int, List[Tuple[int, str, str, Dict[str, Any]]]] = defaultdict(list)
    with sk_db.connect() as conn:
        for ski, cdi, tier, score, ev in conn.execute(
                "SELECT sk_college_id, cd_college_id, tier, score, evidence "
                "FROM sk_matches WHERE verdict='yes' AND sk_college_id IN ("
                "  SELECT sk_college_id FROM sk_matches WHERE verdict='yes' "
                "  GROUP BY sk_college_id HAVING COUNT(*)>1) "
                "ORDER BY sk_college_id"):
            try:
                evd = json.loads(ev or "{}")
            except Exception:  # noqa: BLE001
                evd = {}
            groups[int(ski)].append((int(cdi), tier, score, evd))

    print(f"Conflicts [BUILD {BUILD}]")
    print(f"  {len(groups):,} Shiksha colleges claimed by >1 Collegedunia row\n")

    kinds: Counter = Counter()
    by_tier: Counter = Counter()
    samples: Dict[str, List[str]] = defaultdict(list)
    widest = 0
    for ski, rows in groups.items():
        widest = max(widest, len(rows))
        by_tier[tuple(sorted({r[1] for r in rows}))] += 1
        ids = [r[0] for r in rows]
        names = [cd.name.get(i, "") for i in ids]
        toks = [cd.toks.get(i, set()) for i in ids]
        pairwise = [jaccard(toks[a], toks[b])
                    for a in range(len(ids)) for b in range(a + 1, len(ids))]
        shared = [v for r in rows
                  for k, v in r[3].items()
                  if k in ("website", "phone", "email")]
        lowered = [n.lower() for n in names if n]

        if pairwise and min(pairwise) >= CD_DUP:
            kind = "cd-duplicate"
        elif shared and len(set(shared)) == 1 and len(shared) == len(rows):
            kind = "shared-signal"
        elif any(a != b and (a in b or b in a)
                 for a in lowered for b in lowered):
            kind = "parent-child"
        else:
            kind = "ambiguous"
        kinds[kind] += 1
        if len(samples[kind]) < limit:
            samples[kind].append(
                "  SK %-8s %s\n%s" % (
                    ski, (sk.name.get(ski) or "?")[:70],
                    "\n".join("       CD %-8s %-52s [%s %.2f]"
                              % (r[0], (cd.name.get(r[0]) or "?")[:52],
                                 r[1], r[2])
                              for r in rows)))

    print("by kind:")
    for k, n in kinds.most_common():
        print(f"   {k:<14} {n:>6,}   ({100.0*n/max(1,len(groups)):.1f}%)")
    print(f"\n   widest collision: {widest} Collegedunia rows on one Shiksha college")

    print("\nby the tiers involved:")
    for t, n in by_tier.most_common(8):
        print(f"   {'+'.join(t):<28} {n:>6,}")

    for k in ("cd-duplicate", "shared-signal", "parent-child", "ambiguous"):
        if not samples[k]:
            continue
        print(f"\n--- {k} — {kinds[k]:,} total, showing {len(samples[k])} ---")
        for s in samples[k]:
            print(s)
    print("\nNothing was changed. Resolution is a judgement, not a default.")
    return 0


def export(limit: int = 300, path: str = "/data/sk_judge.jsonl") -> int:
    """The ambiguous band, smallest file that still carries the evidence."""
    cd, sk = load_cd(), load_sk()
    n = 0
    with sk_db.connect() as conn, open(path, "w", encoding="utf-8") as fh:
        for cdi, ski, score, ev in conn.execute(
                "SELECT cd_college_id, sk_college_id, score, evidence "
                "FROM sk_matches WHERE verdict='pending' "
                "ORDER BY score DESC LIMIT ?", (int(limit),)):
            fh.write(json.dumps({
                "cd_id": cdi, "sk_id": ski, "score": round(score, 3),
                "cd": {"name": cd.name.get(cdi, ""), "city": cd.city.get(cdi, ""),
                       "site": cd.site.get(cdi, ""), "short": cd.short.get(cdi, "")},
                "sk": {"name": sk.name.get(ski, ""), "city": sk.city.get(ski, ""),
                       "site": sk.site.get(ski, ""), "short": sk.short.get(ski, "")},
            }, ensure_ascii=False) + "\n")
            n += 1
    print(f"wrote {n:,} pairs to {path}")
    print("Judge each one and write back a file of "
          '{"cd_id":…,"sk_id":…,"verdict":"yes|no","note":"…"}, then:')
    print("   python sk_match.py apply <file>")
    return 0


def apply_verdicts(path: str) -> int:
    now, n = time.time(), 0
    with sk_db.connect() as conn, open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            v = str(d.get("verdict", "")).lower()
            if v not in ("yes", "no"):
                continue
            conn.execute(
                "UPDATE sk_matches SET verdict=?, decided_by='judge', note=?, "
                "decided_at=? WHERE cd_college_id=? AND sk_college_id=?",
                (v, str(d.get("note", ""))[:300], now,
                 int(d["cd_id"]), int(d["sk_id"])))
            n += 1
    print(f"applied {n:,} judged verdicts")
    return 0


USAGE = """usage:
  python sk_match.py build
  python sk_match.py report
  python sk_match.py conflicts [n]
  python sk_match.py export [n] [path]
  python sk_match.py apply <file>"""


def main(argv: List[str]) -> int:
    # sk_db.connect() opens the file; it does not create anything. The
    # sk_matches table ships in sk_db.SCHEMA, but SCHEMA is only applied by
    # init_db(), and nothing here was calling it — so a correctly deployed
    # schema still produced "no such table" after several minutes of matching
    # work. init_db() is idempotent (CREATE TABLE IF NOT EXISTS throughout) and
    # also adds any phase Ⓑ columns a database predating them is missing.
    sk_db.init_db()
    cmd = argv[0] if argv else "report"
    if cmd == "build":
        return build()
    if cmd == "report":
        return report()
    if cmd == "conflicts":
        return conflicts(int(argv[1]) if len(argv) > 1 else 12)
    if cmd == "export":
        n = int(argv[1]) if len(argv) > 1 else 300
        p = argv[2] if len(argv) > 2 else "/data/sk_judge.jsonl"
        return export(n, p)
    if cmd == "apply" and len(argv) > 1:
        return apply_verdicts(argv[1])
    print(USAGE)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
