"""
Matcher tests — chiefly, that a pair is labelled by its STRONGEST evidence.

The bug these exist for (2026-09-28, found by arithmetic on a live report and
reproduced before being fixed): `add()` replaced a pair's `tier` whenever a later
tier scored higher. Website agreement with 2-3 candidates scores 0.9 and a name
match can score 0.97, so the pair

    CD "Indian Institute of Technology Bombay"  <->  SK same name,
    agreeing on BOTH iitb.ac.in AND the name

stored as tier='name' — the weakest label — while

    CD "IIT Bombay"  <->  SK "Shailesh J Mehta School of Management",
    sharing only the domain, name_jaccard 0.1

kept tier='website'. `SELECT ... WHERE tier='website'` therefore returned the
weak pair and missed the strong one. Scores are not comparable across tiers;
only TIER_RANK may decide a tier.

Run:  python t_shiksha_match.py
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

_TMP = tempfile.mkdtemp()
os.environ["CD_DB_PATH"] = os.path.join(_TMP, "data.db")
os.environ["CD_SK_DB_PATH"] = os.path.join(_TMP, "shiksha.db")

import db as _core        # noqa: E402
import sk_db              # noqa: E402
import sk_match           # noqa: E402

IITB = "Indian Institute of Technology Bombay"


def reset():
    _core.init_db()
    sk_db.init_db()
    with _core.connect() as c:
        for t in ("colleges", "colleges_directory"):
            try:
                c.execute(f"DELETE FROM {t}")
            except Exception:  # noqa: BLE001
                pass
    with sk_db.connect() as c:
        c.execute("DELETE FROM sk_colleges")
        c.execute("DELETE FROM sk_matches")


def cd_row(cid, name, city="", website="", email="", phone="", short=""):
    with _core.connect() as c:
        c.execute(
            "INSERT OR REPLACE INTO colleges(college_id,name,city,website,email,"
            "phone,short_form,scraped_at) VALUES(?,?,?,?,?,?,?,0)",
            (cid, name, city, website, email, phone, short))


def sk_row(cid, name, city="", website="", email="", phone="", short=""):
    sk_db.upsert_colleges([{"college_id": cid, "slug": f"s-{cid}", "name": name,
                            "city": city, "website": website, "email": email,
                            "phone": phone, "short_name": short}])


def stored():
    with sk_db.connect() as c:
        return {(r[0], r[1]): {"tier": r[2], "score": r[3], "verdict": r[4],
                               "evidence": json.loads(r[5] or "{}")}
                for r in c.execute("SELECT cd_college_id,sk_college_id,tier,"
                                   "score,verdict,evidence FROM sk_matches")}


def run():
    import io
    import contextlib
    with contextlib.redirect_stdout(io.StringIO()):
        sk_match.build()
    return stored()


class TierFollowsEvidenceNotScore(unittest.TestCase):
    """The reproduction, kept as a test."""

    def setUp(self):
        reset()
        cd_row(1, IITB, "Mumbai", "https://www.iitb.ac.in")
        # TWO Shiksha rows on the domain, so the website tier scores 0.9 rather
        # than 1.0 and a 0.97 name match can outscore it.
        sk_row(101, IITB, "Mumbai", "https://www.iitb.ac.in")
        sk_row(102, "Shailesh J Mehta School of Management", "Mumbai",
               "https://www.iitb.ac.in")
        self.m = run()

    def test_domain_and_name_agreeing_is_labelled_website(self):
        self.assertEqual(self.m[(1, 101)]["tier"], "website")

    def test_the_weaker_pair_is_also_website_and_says_so_in_its_score(self):
        self.assertEqual(self.m[(1, 102)]["tier"], "website")
        self.assertLess(self.m[(1, 102)]["score"], self.m[(1, 101)]["score"])
        self.assertLess(self.m[(1, 102)]["evidence"]["name_jaccard"], 0.5)

    def test_the_weaker_evidence_is_kept_not_discarded(self):
        self.assertIn("name", self.m[(1, 101)]["evidence"]["also"])

    def test_score_is_the_best_across_all_evidence(self):
        self.assertAlmostEqual(self.m[(1, 101)]["score"], 1.0, places=3)

    def test_filtering_on_the_strong_tier_finds_the_strong_pair(self):
        """The failure this whole file exists for: before the fix, this query
        returned 102 and missed 101."""
        with sk_db.connect() as c:
            got = {r[0] for r in c.execute(
                "SELECT sk_college_id FROM sk_matches WHERE tier='website'")}
        self.assertIn(101, got)


class VerdictIsNeverDowngraded(unittest.TestCase):

    def test_a_yes_survives_a_pending_for_the_same_pair(self):
        """The name tier runs LAST. These two names share 3 of 5 tokens, which
        lands in the 0.45-0.85 judgement band, so the name tier emits 'pending'
        for a pair the email tier has already accepted. The verdict must stay
        'yes' — otherwise settled matches leak back into the judgement queue."""
        reset()
        cd_row(1, "Alpha Institute of Technology", "Delhi",
               email="office@alphaedu.ac.in")
        sk_row(201, "Alpha Institute of Management", "Delhi",
               email="info@alphaedu.ac.in")
        m = run()
        pair = m.get((1, 201))
        self.assertIsNotNone(pair, "email agreement should have matched")
        self.assertEqual(pair["tier"], "email")
        self.assertEqual(pair["verdict"], "yes")
        self.assertIn("name", pair["evidence"].get("also", []))
        # and the name similarity really is in the judgement band, so the
        # 'pending' branch was genuinely exercised
        self.assertTrue(0.45 <= pair["evidence"]["name_jaccard"] < 0.85,
                        pair["evidence"])


class NonIdentifyingSignalsAreRefused(unittest.TestCase):

    def test_a_domain_shared_by_many_rows_is_not_identity(self):
        """One domain across >3 Shiksha rows is a university's departments, or
        junk. Matching on it would merge institutions that are not the same."""
        reset()
        cd_row(1, "Some Affiliated College", "Pune", "https://unipune.ac.in")
        for i, n in enumerate(["A College", "B College", "C College",
                               "D College", "E College"], start=301):
            sk_row(i, n, "Pune", "https://unipune.ac.in")
        m = run()
        self.assertEqual([k for k in m if k[0] == 1 and
                          m[k]["tier"] == "website"], [])

    def test_a_free_mail_host_is_not_identity(self):
        reset()
        cd_row(1, "Alpha College", "Delhi", email="alpha@gmail.com")
        sk_row(401, "Beta College", "Delhi", email="beta@gmail.com")
        m = run()
        self.assertEqual([k for k in m if m[k]["tier"] == "email"], [])

    def test_the_aggregators_own_domain_is_not_identity(self):
        """BOTH sides citing the same aggregator — the case that actually
        merges institutions if the junk list is not applied."""
        reset()
        cd_row(1, "Alpha College", "Delhi", "https://collegedunia.com/alpha")
        sk_row(501, "Beta College", "Kolkata", "https://collegedunia.com/beta")
        m = run()
        self.assertEqual([k for k in m if m[k]["tier"] == "website"], [])

    def test_a_shared_social_profile_is_not_identity(self):
        reset()
        cd_row(1, "Alpha College", "Delhi", "https://facebook.com/alpha")
        sk_row(502, "Beta College", "Kolkata", "https://facebook.com/beta")
        m = run()
        self.assertEqual([k for k in m if m[k]["tier"] == "website"], [])


class BlockingIsDeterministic(unittest.TestCase):
    """Two builds over identical data must produce identical pairs.

    They did not. `best()` sorted a SET of tokens by document frequency alone,
    so every tie was broken by set iteration order — string hashing, which
    Python randomises per process. Two consecutive live builds each wrote
    22,599 pairs and left 22,604 rows: five pairs existed in one run and not the
    other, which means the judgement queue was not reproducible."""

    TIED = {10: {"alpha", "beta", "gamma", "delta"}, 11: {"alpha"},
            12: {"beta"}, 13: {"gamma"}, 14: {"delta"}}

    def test_ties_break_on_the_token_not_on_the_hash(self):
        idx = sk_match.Index(self.TIED)
        # every token has df=1, so a total order can only come from the token
        self.assertEqual(idx.blocking_tokens(self.TIED[10]),
                         ["alpha", "beta", "delta"])

    def test_rarity_still_wins_over_the_tiebreaker(self):
        toks = {1: {"zzz", "aaa"}, 2: {"aaa"}, 3: {"aaa"}, 4: {"aaa"}}
        idx = sk_match.Index(toks)
        # 'aaa' has df=4, 'zzz' df=1 — the rare one must come first even though
        # it sorts last alphabetically
        self.assertEqual(idx.blocking_tokens(toks[1], n=1), ["zzz"])

    def test_the_same_answer_under_different_hash_seeds(self):
        """The real proof: a fresh interpreter per seed, which is the only way
        to vary PYTHONHASHSEED. An in-process test cannot catch this."""
        import subprocess
        import sys
        code = ("import sk_match;"
                "t={10:{'alpha','beta','gamma','delta'},11:{'alpha'},"
                "12:{'beta'},13:{'gamma'},14:{'delta'}};"
                "print(sk_match.Index(t).blocking_tokens(t[10]))")
        answers = set()
        for seed in ("1", "2", "3", "4", "5"):
            env = dict(os.environ, PYTHONHASHSEED=seed)
            out = subprocess.run([sys.executable, "-c", code], env=env,
                                 capture_output=True, text=True,
                                 cwd=os.path.dirname(os.path.abspath(__file__)))
            answers.add(out.stdout.strip())
        self.assertEqual(len(answers), 1,
                         "blocking tokens differ across hash seeds: %s" % answers)


class StaleRowsAreNamed(unittest.TestCase):
    """Rows dropped from the match set are kept — the owner's standing rule is
    that nothing is deleted — but they must be VISIBLE, or the report's totals
    silently exceed what the build wrote."""

    def test_report_prints_a_total(self):
        import contextlib
        import io
        reset()
        cd_row(1, IITB, "Mumbai", "https://www.iitb.ac.in")
        sk_row(101, IITB, "Mumbai", "https://www.iitb.ac.in")
        run()
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sk_match.report()
        self.assertIn("TOTAL", buf.getvalue())

    def test_a_leftover_row_is_called_out(self):
        import contextlib
        import io
        reset()
        cd_row(1, IITB, "Mumbai", "https://www.iitb.ac.in")
        sk_row(101, IITB, "Mumbai", "https://www.iitb.ac.in")
        run()
        # a pair from an imaginary earlier build that this one no longer emits
        with sk_db.connect() as c:
            c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,score,"
                      "tier,evidence,verdict,decided_by,decided_at) "
                      "VALUES(1,999,0.5,'name','{}','pending','auto',1000)")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sk_match.report()
        out = buf.getvalue()
        self.assertIn("leftovers from an earlier one", out)
        # and it is still there afterwards — named, not removed
        with sk_db.connect() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM sk_matches "
                                       "WHERE sk_college_id=999").fetchone()[0], 1)


class ReportContract(unittest.TestCase):

    def test_tier_rank_covers_every_tier_add_can_emit(self):
        """A tier missing from TIER_RANK silently ranks 0 and loses every
        comparison — the same class of failure as the bug above."""
        self.assertEqual(sorted(sk_match.TIER_RANK),
                         ["email", "name", "phone", "shortform", "website"])

    def test_website_outranks_every_other_tier(self):
        r = sk_match.TIER_RANK
        self.assertTrue(all(r["website"] > v for k, v in r.items()
                            if k != "website"))
        self.assertTrue(all(r["name"] <= v for v in r.values()))


if __name__ == "__main__":
    unittest.main(verbosity=2, exit=False)
