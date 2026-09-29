"""
Tests for the Collegedunia/Shiksha gap profile.

What these guard is not arithmetic but an argument. Twice now the 20,306 vs
57,751 question has been answered from totals alone, and
`finding-college-discovery-topology-2026-09-18` names why that fails: *a summary
statistic consistent with more than one underlying story, treated as if it
identified one.*

So the tests below check that the tool cannot make that mistake:

  * a college with no candidate pair AND no way to look it up is never counted
    as evidence of absence;
  * only the searchable subset — a website and a real name — feeds the headline
    number;
  * a pair an earlier build accepted and the current one dropped cannot make a
    college look matched;
  * the buckets partition the inventory exactly.

Run:  python t_shiksha_gap.py
"""
from __future__ import annotations

import contextlib
import io
import os
import re
import tempfile
import time
import unittest

os.environ["CD_SK_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "shiksha.db")

import sk_db     # noqa: E402
import sk_gap    # noqa: E402


def reset():
    sk_db.init_db()
    with sk_db.connect() as c:
        c.execute("DELETE FROM sk_colleges")
        c.execute("DELETE FROM sk_matches")
        c.execute("DELETE FROM sk_college_base_courses")
        c.execute("DELETE FROM sk_settings")


def college(cid, name="", website="", phone="", detail=False):
    sk_db.upsert_colleges([{"college_id": cid, "slug": "c-%d" % cid,
                            "name": name, "website": website, "phone": phone,
                            "city": "Pune", "state": "Maharashtra",
                            "detail_scraped_at": time.time() if detail else None}])


def match(cid, verdict, at=None, cd=None):
    """cd defaults to a per-college id; pass it explicitly when one Shiksha
    college needs two different Collegedunia claimants (the PK is the pair)."""
    with sk_db.connect() as c:
        c.execute("INSERT INTO sk_matches(cd_college_id,sk_college_id,score,tier,"
                  "evidence,verdict,decided_by,decided_at) "
                  "VALUES(?,?,0.9,'website','{}',?,'auto',?)",
                  (cd if cd is not None else cid * 1000, cid, verdict,
                   at if at is not None else time.time()))


def run():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        sk_gap.main([])
    return buf.getvalue()


def headline(out):
    """The 'searchable, and not found' figure."""
    m = re.search(r"([\d,]+) Shiksha colleges have a website AND a name", out)
    return int(m.group(1).replace(",", "")) if m else None


def unknown(out):
    m = re.search(r"([\d,]+) of the 'none' bucket could not be looked up", out)
    return int(m.group(1).replace(",", "")) if m else None


class Buckets(unittest.TestCase):

    def setUp(self):
        reset()
        sk_db.set_setting("last_match_build_at", time.time() - 10)
        college(1, "Alpha College", "https://a.ac.in", detail=True)
        match(1, "yes")
        college(2, "Beta College", "https://b.ac.in", detail=True)
        match(2, "pending")
        college(3, "Gamma College", "https://c.ac.in", detail=True)   # no pair
        college(4)                                                    # no signal
        self.out = run()

    def test_the_three_buckets_partition_the_inventory(self):
        self.assertIn("TOTAL", self.out)
        self.assertNotIn("do not trust the rest", self.out)

    def test_an_accepted_pair_means_matched(self):
        self.assertRegex(self.out, r"matched\s+1\s")

    def test_a_pending_pair_is_not_counted_as_matched(self):
        self.assertRegex(self.out, r"pending\s+1\s")

    def test_accepted_wins_over_pending_for_the_same_college(self):
        reset()
        sk_db.set_setting("last_match_build_at", time.time() - 10)
        college(1, "Alpha College", "https://a.ac.in", detail=True)
        match(1, "pending", cd=7001)
        match(1, "yes", cd=7002)
        out = run()
        self.assertRegex(out, r"matched\s+1\s")
        self.assertRegex(out, r"pending\s+0\s")


class TheControl(unittest.TestCase):
    """The part that stops this becoming another totals story."""

    def setUp(self):
        reset()
        sk_db.set_setting("last_match_build_at", time.time() - 10)

    def test_a_searchable_college_with_no_pair_counts_as_evidence(self):
        college(1, "Gamma College", "https://c.ac.in", detail=True)
        out = run()
        self.assertEqual(headline(out), 1)
        self.assertEqual(unknown(out), 0)

    def test_a_college_with_no_website_does_not(self):
        """We could not look it up. That is not absence."""
        college(1, "Gamma College", "", detail=True)
        out = run()
        self.assertEqual(headline(out), 0)
        self.assertEqual(unknown(out), 1)

    def test_a_college_with_no_name_does_not_either(self):
        """Phase Ⓑ has not reached it; only a slug exists."""
        college(1, "", "https://c.ac.in")
        out = run()
        self.assertEqual(headline(out), 0)
        self.assertEqual(unknown(out), 1)

    def test_the_two_figures_account_for_the_whole_none_bucket(self):
        college(1, "A", "https://a.ac.in", detail=True)   # searchable, no pair
        college(2, "B", "", detail=True)                  # no website
        college(3, "", "https://c.ac.in")                 # no name
        college(4)                                        # neither
        out = run()
        self.assertEqual(headline(out) + unknown(out), 4)

    def test_a_matched_college_never_reaches_the_headline(self):
        college(1, "A", "https://a.ac.in", detail=True)
        match(1, "yes")
        self.assertEqual(headline(run()), 0)

    def test_the_no_signal_row_is_reported(self):
        college(1)
        self.assertIn("NO contact signal at all", run())


class StaleRowsDoNotCount(unittest.TestCase):

    def test_a_pair_from_an_earlier_build_does_not_make_a_college_matched(self):
        """The same contamination that made the match report claim
        website=9,982 against a build that had matched 9,523."""
        reset()
        cut = time.time()
        sk_db.set_setting("last_match_build_at", cut)
        college(1, "Alpha College", "https://a.ac.in", detail=True)
        match(1, "yes", at=cut - 5000)          # an earlier build's row
        out = run()
        self.assertRegex(out, r"matched\s+0\s")
        self.assertEqual(headline(out), 1)      # searchable, and genuinely unpaired

    def test_without_a_recorded_build_the_tool_says_so(self):
        reset()
        college(1, "Alpha College", "https://a.ac.in", detail=True)
        match(1, "yes")
        out = run()
        self.assertIn("no match build recorded", out)


class ItWritesNothing(unittest.TestCase):

    def test_read_only(self):
        reset()
        sk_db.set_setting("last_match_build_at", time.time() - 10)
        college(1, "Alpha College", "https://a.ac.in", detail=True)
        match(1, "yes")
        with sk_db.connect() as c:
            before = c.execute("SELECT COUNT(*) FROM sk_colleges").fetchone()[0], \
                     c.execute("SELECT COUNT(*) FROM sk_matches").fetchone()[0]
        run()
        with sk_db.connect() as c:
            after = c.execute("SELECT COUNT(*) FROM sk_colleges").fetchone()[0], \
                    c.execute("SELECT COUNT(*) FROM sk_matches").fetchone()[0]
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main(verbosity=2, exit=False)
