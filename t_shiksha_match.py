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

    def leftover_setup(self):
        reset()
        cd_row(1, IITB, "Mumbai", "https://www.iitb.ac.in")
        sk_row(101, IITB, "Mumbai", "https://www.iitb.ac.in")
        run()
        # A pair an earlier build accepted and this one no longer emits — the
        # exact shape of the 1,385 rows the cap and tokeniser fixes removed.
        with sk_db.connect() as c:
            c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,score,"
                      "tier,evidence,verdict,decided_by,decided_at) "
                      "VALUES(42,999,0.99,'website','{}','yes','auto',1000)")

    def report_text(self):
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sk_match.report()
        return buf.getvalue()

    def test_a_leftover_row_is_called_out(self):
        self.leftover_setup()
        self.assertIn("stored but excluded here", self.report_text())

    def test_a_leftover_row_is_kept_not_removed(self):
        self.leftover_setup()
        self.report_text()
        with sk_db.connect() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM sk_matches "
                                       "WHERE sk_college_id=999").fetchone()[0], 1)

    def test_a_leftover_does_not_inflate_the_headline_figures(self):
        """The failure this exists for: after the cap and tokeniser fixes the
        build wrote 21,715 pairs while the table held 23,100, and the mixture
        reported website=9,982 against a build that had matched 9,523 — and
        COVERAGE RISING when the fixes had removed matches."""
        self.leftover_setup()
        out = self.report_text()
        # CD 42 exists only in the leftover row, so it must not be counted as
        # a matched Collegedunia college
        self.assertIn("1 / 1", out.replace(",", ""))
        self.assertNotIn("2 / 1", out)

    def test_a_leftover_is_not_counted_as_a_conflict(self):
        import contextlib
        import io
        reset()
        cd_row(1, IITB, "Mumbai", "https://www.iitb.ac.in")
        sk_row(101, IITB, "Mumbai", "https://www.iitb.ac.in")
        run()
        with sk_db.connect() as c:      # a second, stale claimant on SK 101
            c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,score,"
                      "tier,evidence,verdict,decided_by,decided_at) "
                      "VALUES(42,101,0.99,'website','{}','yes','auto',1000)")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sk_match.conflicts(limit=5)
        self.assertIn("0 Shiksha colleges claimed by >1", buf.getvalue())


class ConflictClassification(unittest.TestCase):
    """1,304 collisions is one number covering several different situations,
    and they do not want the same treatment. Each fixture below is one of
    them, built so exactly one classification can fit."""

    def classify(self):
        import contextlib
        import io
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            sk_match.conflicts(limit=5)
        out = buf.getvalue()
        got = {}
        for k in ("cd-duplicate", "shared-signal", "parent-child", "ambiguous"):
            for line in out.splitlines():
                if line.strip().startswith(k):
                    got[k] = int(line.split()[1].replace(",", ""))
                    break
        return got, out

    def test_two_collegedunia_rows_for_the_same_college(self):
        """Both matches are CORRECT; the duplicate is Collegedunia's, and this
        is a fact about their inventory rather than a matcher error."""
        reset()
        cd_row(1, "Alpha Institute of Technology", "Pune", "https://alpha1.ac.in")
        cd_row(2, "Alpha Institute of Technology", "Pune", "https://alpha2.ac.in")
        sk_row(101, "Alpha Institute of Technology", "Pune",
               "https://alpha1.ac.in", phone="9000000001")
        with sk_db.connect() as c:
            for cdi in (1, 2):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,0.98,'name','{}','yes','auto',9e9)", (cdi,))
        got, _ = self.classify()
        self.assertEqual(got.get("cd-duplicate"), 1, got)

    def test_one_switchboard_number_claimed_by_two_colleges(self):
        """Same evidence VALUE on both claimants — the signal is not
        identifying here, whatever tier it sits in."""
        reset()
        cd_row(1, "Alpha College of Arts", "Pune")
        cd_row(2, "Beta College of Commerce", "Pune")
        sk_row(101, "Gamma University", "Pune")
        with sk_db.connect() as c:
            for cdi in (1, 2):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,0.9,'phone',"
                          "'{\"phone\":\"2025551234\"}','yes','auto',9e9)", (cdi,))
        got, _ = self.classify()
        self.assertEqual(got.get("shared-signal"), 1, got)

    def test_a_university_and_its_department(self):
        reset()
        cd_row(1, "Delta University", "Delhi")
        cd_row(2, "Delta University School of Law", "Delhi")
        sk_row(101, "Delta University", "Delhi")
        with sk_db.connect() as c:
            for cdi, sc in ((1, 0.99), (2, 0.86)):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,?,'name','{}','yes','auto',9e9)", (cdi, sc))
        got, _ = self.classify()
        self.assertEqual(got.get("parent-child"), 1, got)

    def test_anything_else_is_left_for_judgement(self):
        reset()
        cd_row(1, "Epsilon College of Nursing", "Jaipur")
        cd_row(2, "Zeta Academy of Design", "Jaipur")
        sk_row(101, "Eta Institute", "Jaipur")
        with sk_db.connect() as c:
            for cdi, ev in ((1, '{"website":"a.ac.in"}'),
                            (2, '{"website":"b.ac.in"}')):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,0.9,'website',?,'yes','auto',9e9)",
                          (cdi, ev))
        got, _ = self.classify()
        self.assertEqual(got.get("ambiguous"), 1, got)

    def test_it_changes_nothing(self):
        reset()
        cd_row(1, "Alpha", "Pune")
        cd_row(2, "Alpha", "Pune")
        sk_row(101, "Alpha", "Pune")
        with sk_db.connect() as c:
            for cdi in (1, 2):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,0.98,'name','{}','yes','auto',9e9)", (cdi,))
            before = c.execute("SELECT COUNT(*), SUM(score) FROM sk_matches"
                               ).fetchone()
        self.classify()
        with sk_db.connect() as c:
            after = c.execute("SELECT COUNT(*), SUM(score) FROM sk_matches"
                              ).fetchone()
        self.assertEqual(tuple(before), tuple(after))

    def test_a_partly_duplicated_group_is_not_a_clean_duplicate(self):
        """Three claimants: two identical, one unrelated. Judging the group by
        the CLOSEST pair would call it a tidy Collegedunia duplicate and hide
        the third row. Every pair must be alike, so the test uses min()."""
        reset()
        cd_row(1, "Alpha Institute of Technology", "Pune")
        cd_row(2, "Alpha Institute of Technology", "Pune")
        cd_row(3, "Zeta College of Nursing", "Pune")
        sk_row(101, "Alpha Institute of Technology", "Pune")
        with sk_db.connect() as c:
            for cdi in (1, 2, 3):
                c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                          "score,tier,evidence,verdict,decided_by,decided_at) "
                          "VALUES(?,101,0.9,'name','{}','yes','auto',9e9)", (cdi,))
        got, out = self.classify()
        self.assertIsNone(got.get("cd-duplicate"), got)
        self.assertEqual(got.get("ambiguous"), 1, got)
        self.assertIn("widest collision: 3", out)

    def test_a_single_claimant_is_not_a_conflict(self):
        reset()
        cd_row(1, "Solo College", "Pune")
        sk_row(101, "Solo College", "Pune")
        with sk_db.connect() as c:
            c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,"
                      "score,tier,evidence,verdict,decided_by,decided_at) "
                      "VALUES(1,101,0.99,'name','{}','yes','auto',9e9)")
        got, out = self.classify()
        self.assertEqual(got, {})
        self.assertIn("0 Shiksha colleges claimed by >1", out)


class InitialsAreNotInterchangeable(unittest.TestCase):
    """Splitting on punctuation and collecting into a SET made two different
    institutions identical. From the live conflict report: R.R. Group of
    Institutions and S.R Group of Institutions, both Lucknow, scored
    `name 1.00` and were accepted as one college.

    ['r','r',...] collapses to {'r'}; ['s','r',...] loses 's' to the stopword
    list (which is there for possessives) and also leaves {'r'}."""

    def test_two_different_initialisms_stay_different(self):
        a = sk_match.tokens("R.R. Group of Institutions", "Lucknow")
        b = sk_match.tokens("S.R Group of Institutions", "Lucknow")
        self.assertNotEqual(a, b)
        self.assertLess(sk_match.jaccard(a, b), sk_match.ACCEPT_NAME)
        self.assertIn("rr", a)
        self.assertIn("sr", b)

    def test_spacing_of_the_same_initialism_does_not_matter(self):
        a = sk_match.tokens("B. N. M. Institute of Technology", "Bangalore")
        b = sk_match.tokens("B.N.M. Institute of Technology", "Bangalore")
        self.assertEqual(a, b)
        self.assertIn("bnm", a)

    def test_a_possessive_s_is_still_dropped(self):
        a = sk_match.tokens("St. Xavier's College", "Mumbai")
        b = sk_match.tokens("St Xavier College", "Mumbai")
        self.assertEqual(a, b)
        self.assertNotIn("s", a)

    def test_a_single_trailing_letter_is_not_glued_to_the_previous_word(self):
        self.assertEqual(sk_match.tokens("Alpha College B"),
                         {"alpha", "college", "b"})

    def test_digits_are_still_ignored(self):
        self.assertNotIn("2026", sk_match.tokens("Alpha College 2026"))


class SharedValuesAreCappedOnBothSides(unittest.TestCase):
    """The cap on how many rows may share an identifying value was applied to
    Shiksha only. The live report shows what that cost: five Maharishi
    Markandeshwar colleges on one domain all claiming SK 4274, nine Uka Tarsadia
    colleges on one email all claiming SK 101689, and fifteen Collegedunia rows
    on a single Shiksha college at the widest."""

    def group(self, n):
        reset()
        for i in range(1, n + 1):
            cd_row(i, f"Group College Number {i}", "Pune",
                   "https://onegroup.ac.in")
        sk_row(101, "One Group Institute", "Pune", "https://onegroup.ac.in")
        return run()

    def test_a_domain_on_too_many_collegedunia_rows_does_not_identify(self):
        m = self.group(sk_match.MAX_SHARERS + 1)
        self.assertEqual([k for k in m if m[k]["tier"] == "website"], [])

    def test_a_domain_on_few_enough_rows_still_matches(self):
        m = self.group(sk_match.MAX_SHARERS)
        self.assertTrue([k for k in m if m[k]["tier"] == "website"])

    def test_the_cap_is_the_same_number_on_both_sides(self):
        """One constant, so the two sides cannot drift apart again."""
        import inspect
        import re
        src = inspect.getsource(sk_match.build)
        guards = [ln.strip() for ln in src.splitlines()
                  if re.search(r"if\s+.*len\(.*\)\s*>", ln)]
        self.assertEqual(len(guards), 2, guards)
        for g in guards:
            self.assertIn("MAX_SHARERS", g, g)


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
