"""
Phase Ⓒ / Ⓓ tests — the per-course rows.

The fixtures are NOT invented. Every value below was read out of a live
shiksha.com page in a real browser on 2026-09-28 (college 72, ARCH College of
Design and Business): the listing tuple and paginationData from
/courses, the courseData block from
/course-b-des-in-fashion-textile-design-306967. Where the live page had a null
(workExpString, difficultyLevel, skills, shikshaRank, courseLevel) the fixture
keeps the null, because those nulls are the reason phase Ⓓ exists.

Run:  python t_shiksha_courses.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest

# One database for the whole run, chosen BEFORE sk_db is imported — the module
# binds SK_DB_PATH into its function defaults at import time, so reassigning the
# attribute later moves the explicit calls and not the runner's internal ones,
# which is exactly the sort of half-redirected test that passes while testing
# nothing. Tables are emptied between tests instead.
os.environ["CD_SK_DB_PATH"] = os.path.join(tempfile.mkdtemp(), "shiksha.db")

import sk_db             # noqa: E402
import sk_parse          # noqa: E402

_TABLES = ("sk_offerings", "sk_courses", "sk_colleges", "sk_college_progress",
           "sk_course_progress", "sk_jobs", "sk_logs", "data_changes")


def fresh_db() -> str:
    """Empty every table this suite touches, keeping one file and one schema."""
    sk_db.init_db()
    with sk_db.connect() as conn:
        for t in _TABLES:
            try:
                conn.execute(f"DELETE FROM {t}")
            except Exception:  # noqa: BLE001  (table may not exist)
                pass
    return sk_db.SK_DB_PATH

SLUG = "arch-college-of-design-and-business-jaipur-72"

# --- live listing tuple, verbatim ------------------------------------------
TUPLE = {
    "courseId": 306967,
    "name": "B.Des. in Fashion &amp; Textile Design",
    "fees": 1940000,
    "duration": 4, "durationUnit": "years",
    "maxDuration": None, "maxDurationUnit": None,
    "totalSeats": 25,
    "baseCourseId": 9, "baseCourseName": "B.Des",
    "exams": [{"id": 12135, "name": "AIEED",
               "url": "/college/arch-college-of-design-and-business/aieed-exam",
               "scope": "national"}],
    "url": f"/college/{SLUG}/course-b-des-in-fashion-textile-design-306967",
    "courseMedianSalary": 0, "courseRating": 4.6, "ratingCount": 9,
    "scholarshipsCount": 0, "eligibilityXII": 50, "eligibilityGraduation": 0,
    "workExpString": None, "difficultyLevel": None, "skills": None,
    "credential": "Degree", "isOnline": False,
    "curricullumPDF": "https://images.shiksha.com/mediadata/pdf/x.pdf",
    "intakeDates": [], "courseCommencementDates": None,
    "courseAdmissionStatus": None, "shikshaRank": None,
    "totalExamsCount": 1, "instituteGrade": None, "rankingString": None,
    "instituteId": 72,
}

PAGINATION = {
    "nextUrls": [{"pageNumber": 2, "url": f"/college/{SLUG}/courses-2"},
                 {"pageNumber": 3, "url": f"/college/{SLUG}/courses-3"},
                 {"pageNumber": 4, "url": f"/college/{SLUG}/courses-4"}],
    "prevUrls": [], "currentPageNUmber": 1,
    "nextUrlOfCurrentPage": f"/college/{SLUG}/courses-2",
}


def listing_state(tuples=None, pagination=None, total=42):
    """The shape the site actually serves: childPageData one level below root."""
    return {"config": {"API_SERVER": "apis.shiksha.jsb9.net"},
            "instituteData": {},          # the empty decoy, as on the live page
            "childPageData": {
                "listingId": 72,
                # No `instituteId` — NIT Trichy's top card carries instituteName,
                # h1, logoImageUrl and no id at all, so the fixture matches the
                # site rather than the parser's first guess.
                "instituteTopCardData": {"instituteName": "ACDB"},
                "courseTuples": TUPLE if tuples is None else tuples,
                "paginationData": PAGINATION if pagination is None else pagination,
                "totalCourses": total, "totalCourseCount": 0,
                "pageSize": 12, "pageNumber": 0}}


DETAIL_STATE = {
    "courseData": {
        "courseId": 306967, "instituteId": 72,
        "courseName": "B.Des. in Fashion & Textile Design",
        "baseCourseName": "B.Des",
        "specializationName": "Fashion Design",
        "substreamName": "Fashion Design",
        "educationType": {"id": 20, "name": "Full Time"},
        "deliveryMethod": {"id": 33, "name": "Classroom"},
        "durationValue": 4, "durationUnit": "years",
        "mediumOfInstruction": [{"id": 62, "name": "English"}],
        "entryCourseTypeInformation": {
            "type": "entry", "credential": {"id": 9, "name": "Degree"},
            "hierarchy": [{"stream_id": 3, "substream_id": 39,
                           "specialization_id": 183, "primary_hierarchy": 1}],
            "course_id": 306967, "base_course": 9,
            "course_level": {"id": 14, "name": "UG"}},
        "courseFees": {
            "courseId": 306967, "year": 2026, "feesUnit": 1,
            "feesUnitName": "INR",
            "fees": {"periodType": None, "fees": {},
                     "totalFees": {"general": {"value": 1940000,
                                               "category": "general"}},
                     "oneTimePayment": {"general": {"value": 75000,
                                                    "category": "general"}},
                     "showDisclaimer": False, "totalIncludes": [""],
                     "hostelFees": {}, "deposit": {},
                     "otherFees": 5000},
            "locationWiseFees": None,
            "description": "The fees might include components other than "
                           "tuition fees.",
            "hostelDescription": None,
            "otpDescription": "The mentioned fee includes Security Deposit and "
                              "Registration/Admission/Lab/Library Deposit Fee, etc",
            "depositDescription": None,
            "feesBrochureUrl": "https://images.shiksha.com/x.pdf",
            "categoryNameMapping": {"general": "General"}},
        "eligibility": {
            "year": 2026, "tenthDetails": None,
            "twelthDetails": {
                "description": "Students who have appeared for/cleared Grade "
                               "10+2 … are eligible.",
                "scoreType": "percentage",
                "categoryWiseScores": {"general": {"score": 50,
                                                   "category": "general",
                                                   "maxScore": 100}},
                "subjects": None, "cutoff": {},
                "noCutoffMentionedFlag": False},
            "graduationDetails": None, "postGraduationDetails": None,
            "exams": {"exam:12135": {"examId": 12135, "examName": "AIEED",
                                     "examUrl": "https://www.shiksha.com/x",
                                     "scoreType": None,
                                     "categoryWiseScores": {}, "cutOffData": {},
                                     "noCutoffMentionedFlag": True}},
            "minWorkEx": None, "maxWorkEx": None, "minAge": None, "maxAge": None,
            "numberofBacklog": None, "conditionalOffer": None,
            "description": "Admission process involves: 1. Complete the Online "
                           "AIEED Application Form, 2. Submit your SOP & POA.",
            "internationalDescription": None, "additionalInfo": None},
        "courseStructure": {"period": None, "periodWiseCourses": {},
                            "curriculumPdfUrl": "https://images.shiksha.com/c.pdf",
                            "curriculumThumbnailUrl": "/mediadata/images/x.jpeg"},
        # Keyed "1","2","3" — an ORDERED list wearing a dict.
        "admissionProcess": {
            "1": {"admissionName": "Register and Apply",
                  "description": "Interested candidates can apply online."},
            "2": {"admissionName": "Entrance Test",
                  "description": "Eligible candidates appear for AIEED."},
            "3": {"admissionName": "Final Admission",
                  "description": "Selected candidates pay the fee."}},
        "seatsData": {"categoryWiseSeats": None, "examWiseSeats": None,
                      "domicileWiseSeats": None, "relatedStates": None,
                      "totalSeats": 25},
        "placements": {"type": "placements", "course": "39",
                       "course_type": "substreamId", "avg_salary": None,
                       "batch_year": 2023, "max_salary": 1300000,
                       "median_salary": None, "min_salary": None,
                       "percentage_batch_placed": 100, "salary_unit": 1,
                       "salary_unit_name": "INR", "report_url": "https://x/r",
                       "is_internship_available": None},
        "recruitmentCompanies": [{"companyName": "Amrapali Group"},
                                 {"companyName": "Fabindia"}],
        "affiliationData": {"universityId": 23071,
                            "name": "UNIRAJ - University of Rajasthan",
                            "url": "/university/uniraj-university-of-rajasthan-jaipur-23071",
                            "scope": "domestic"},
        "highlights": [{"description": "Earn a degree after completion of "
                                       "course from Arch College of Design and "
                                       "Business",
                        "description_type": "usp",
                        "created_on": "2026-09-25 12:08:27"}],
        "nzqfCategorization": {"id": None, "name": None},
        "importantDates": {
            "source": "layer", "isCourseDates": False, "importantDates": None,
            "entityWiseDates": {"All::-1": [
                {"eventName": "AIEED Examination (Situation Test and GD)",
                 "startDate": 6, "startMonth": 4, "startYear": 2026,
                 "endDate": 18, "endMonth": 4, "endYear": 2026,
                 "type": "exam", "examId": 12135, "examName": "AIEED"}]}},
        "courseType": "Paid", "instituteType": "college",
        "courseVariant": 3, "isCoursePaid": True}}


# --- NIT Trichy B.Tech CSE (course 7948), verbatim ------------------------
# A government institute on a /university/ URL: hostel fees and otherFees are
# populated, eligibility carries real CATEGORY-WISE scores, and the placement
# grain is "clientCourse" rather than college 72's "substreamId".
DETAIL_STATE_NIT = {
    "courseData": {
        "courseId": 7948, "instituteId": 2996,
        "courseName": "B.Tech. in Computer Science and Engineering",
        "baseCourseName": "B.Tech",
        "specializationName": "Computer Science Engineering",
        "substreamName": None,
        "instituteType": "university", "courseType": "Paid",
        "courseVariant": 3, "isCoursePaid": False,
        "entryCourseTypeInformation": {
            "credential": {"id": 9, "name": "Degree"},
            "hierarchy": [{"stream_id": 2, "substream_id": 0,
                           "specialization_id": 108, "primary_hierarchy": 1}],
            "course_level": {"id": 14, "name": "UG"}},
        "courseFees": {
            "courseId": 7948, "year": 2025, "feesUnit": 1, "feesUnitName": "INR",
            "fees": {"periodType": None, "fees": {},
                     "totalFees": {"general": {"value": 500000,
                                               "category": "general"}},
                     "oneTimePayment": {"general": {"value": 35300,
                                                    "category": "general"}},
                     "hostelFees": {"general": {"value": 375000,
                                                "category": "general"}},
                     "deposit": {}, "totalIncludes": [""],
                     "otherFees": 152100},
            "locationWiseFees": None,
            # The prose, not the number, is what says which categories the one
            # `general` figure actually covers.
            "description": "Above mentioned tuition fees for CATEGORY: "
                           "OPEN/OPEN-PWD/OPEN-EWS/OBC-NCL/OBC-PWD/ICCR/"
                           "DASA(CIWG) For more Info please refer the fee PDF.",
            "hostelDescription": "Meal Plan is not included in the mentioned fee.",
            "otpDescription": "One time payment include Admission fee, Campus "
                              "Development fee, Medical Exam fee.",
            "depositDescription": None, "feesBrochureUrl": None,
            "categoryNameMapping": {"general": "General"}},
        "eligibility": {
            "year": 2026,
            "twelthDetails": {
                "scoreType": "percentage",
                "categoryWiseScores": {
                    "sc": {"score": 65, "category": "sc", "maxScore": 100},
                    "general": {"score": 75, "category": "general",
                                "maxScore": 100},
                    "st": {"score": 65, "category": "st", "maxScore": 100}},
                "cutoff": {}, "subjects": None},
            "exams": {"exam:6244": {"examId": 6244, "examName": "JEE Main",
                                    "scoreType": "rank",
                                    "categoryWiseScores": {},
                                    "cutOffData": {}}},
            "minWorkEx": None, "minAge": None, "maxAge": None,
            "numberofBacklog": None},
        "seatsData": {"categoryWiseSeats": None, "examWiseSeats": None,
                      "domicileWiseSeats": None, "totalSeats": 119},
        "placements": {"course_type": "clientCourse", "batch_year": 2026,
                       "percentage_batch_placed": 88.1, "max_salary": None,
                       "avg_salary": None, "median_salary": None,
                       "salary_unit_name": None},
        "recruitmentCompanies": [{"companyName": "Aricent"},
                                 {"companyName": "Ashok Leyland"},
                                 {"companyName": "HONEYWELL"},
                                 {"companyName": "Infosys"},
                                 {"companyName": "Microsoft"}],
        "highlights": [],
        "admissionProcess": {}}}


def one(state, cid=72):
    got = sk_parse.parse_course_listing(state, cid, job_id=7)
    return got


# ---------------------------------------------------------------------------
class ListingParse(unittest.TestCase):

    def setUp(self):
        self.got = one(listing_state([TUPLE]))
        self.row = self.got["offerings"][0]

    def test_one_row_per_course(self):
        self.assertEqual(len(self.got["offerings"]), 1)
        self.assertEqual(self.row["course_id"], 306967)
        self.assertEqual(self.row["college_id"], 72)

    def test_fees_is_a_number_not_a_range(self):
        """The whole point of phase Ⓒ: sk_college_base_courses would give this
        course a min/max spanning every B.Des the college runs."""
        self.assertEqual(self.row["fees_amount"], 1940000)

    def test_entities_are_unescaped(self):
        self.assertEqual(self.row["course_name"],
                         "B.Des. in Fashion & Textile Design")
        self.assertNotIn("&amp;", self.row["course_name"])

    def test_duration_single_value_when_max_is_null(self):
        self.assertEqual(self.row["duration"], "4 years")

    def test_duration_becomes_a_range_when_max_differs(self):
        t = dict(TUPLE, maxDuration=5)
        self.assertEqual(one(listing_state([t]))["offerings"][0]["duration"],
                         "4-5 years")

    def test_exams_are_names_not_objects(self):
        self.assertEqual(json.loads(self.row["exams"]), ["AIEED"])

    def test_eligibility_is_numeric(self):
        self.assertEqual(self.row["eligibility_xii"], 50)
        self.assertEqual(self.row["eligibility_grad"], 0)

    def test_level_is_empty_because_the_listing_has_none(self):
        """Not a parser gap — the live tuple carries no courseLevel. Leaving it
        blank is what makes phase Ⓓ's job visible instead of guessed."""
        self.assertEqual(self.row["level"], "")

    def test_url_and_slug_feed_phase_d(self):
        self.assertTrue(self.row["url"].endswith(
            "course-b-des-in-fashion-textile-design-306967"))
        self.assertEqual(self.row["course_slug"],
                         "course-b-des-in-fashion-textile-design-306967")

    def test_null_fields_do_not_become_the_string_none(self):
        for k in ("work_experience", "difficulty_level", "skills",
                  "admission_status", "institute_grade", "ranking"):
            self.assertEqual(self.row[k], "", k)
        self.assertIsNone(self.row["shiksha_rank"])

    def test_catalogue_row_accompanies_the_edge(self):
        self.assertEqual(self.got["courses"][0]["course_id"], 306967)

    def test_total_and_page_are_read(self):
        self.assertEqual(self.got["total"], 42)
        self.assertEqual(self.got["page"], 1)


class Pagination(unittest.TestCase):

    def test_next_paths_come_from_the_site(self):
        got = one(listing_state([TUPLE]))
        self.assertEqual(got["next_paths"][0], f"/college/{SLUG}/courses-2")
        self.assertEqual(len(got["next_paths"]), 3)

    def test_last_page_offers_nothing_further(self):
        got = one(listing_state([TUPLE], pagination={"nextUrls": [],
                                                     "currentPageNUmber": 4}))
        self.assertEqual(got["next_paths"], [])

    def test_missing_pagination_is_not_an_error(self):
        st = listing_state([TUPLE])
        del st["childPageData"]["paginationData"]
        self.assertEqual(one(st)["next_paths"], [])


class NodeLocation(unittest.TestCase):
    """find_listing must locate by SHAPE. The root's `instituteData` is an empty
    dict on the live page — the same decoy that would have produced 57,751 blank
    colleges in phase Ⓑ."""

    def test_found_below_the_root(self):
        self.assertIsNotNone(sk_parse.find_listing(listing_state([TUPLE])))

    def test_found_at_the_root(self):
        self.assertIsNotNone(sk_parse.find_listing(
            {"courseTuples": [TUPLE], "listingId": 72}))

    def test_the_page_own_listing_id_wins_over_the_caller(self):
        """`listingId` is the page's own claim about which college it is. When
        it disagrees with the queue the page has redirected — expected, since
        discovery found ~26k alias slugs resolving to fewer ids — and attributing
        the rows to the requested id instead would corrupt the edge table."""
        st = listing_state([dict(TUPLE, instituteId=99)])
        st["childPageData"]["listingId"] = 99
        got = sk_parse.parse_course_listing(st, 72, job_id=7)
        self.assertEqual(got["offerings"][0]["college_id"], 99)

    def test_the_top_card_is_not_the_source(self):
        """It looks like the obvious place and it is not there: NIT Trichy's
        top card carries instituteName, h1, logoImageUrl — and no id."""
        st = listing_state([dict(TUPLE, instituteId=99)])
        st["childPageData"]["listingId"] = 99
        st["childPageData"]["instituteTopCardData"] = {"instituteId": 72}
        got = sk_parse.parse_course_listing(st, 72, job_id=7)
        self.assertEqual(got["offerings"][0]["college_id"], 99)

    def test_absent_returns_none_not_a_guess(self):
        self.assertIsNone(sk_parse.find_listing({"childPageData": {"a": 1}}))
        got = sk_parse.parse_course_listing({"childPageData": {}}, 72)
        self.assertEqual(got["offerings"], [])
        self.assertIsNone(got["total"])


class CrossSellIsNotAnOffering(unittest.TestCase):
    """The listing page carries cards for OTHER colleges' courses — 25 such
    links were counted on the live page. Writing them as edges of college 72
    would invent offerings that do not exist."""

    def test_foreign_institute_row_is_dropped(self):
        foreign = dict(TUPLE, courseId=124473, instituteId=31240)
        got = one(listing_state([TUPLE, foreign]))
        self.assertEqual([r["course_id"] for r in got["offerings"]], [306967])

    def test_row_without_an_institute_id_is_kept_for_this_college(self):
        anon = dict(TUPLE, courseId=999); anon.pop("instituteId")
        got = one(listing_state([TUPLE, anon]))
        self.assertEqual(len(got["offerings"]), 2)

    def test_duplicate_course_id_is_written_once(self):
        got = one(listing_state([TUPLE, dict(TUPLE)]))
        self.assertEqual(len(got["offerings"]), 1)

    def test_row_without_a_course_id_is_dropped(self):
        bad = dict(TUPLE); bad["courseId"] = None
        self.assertEqual(one(listing_state([bad]))["offerings"], [])


class DetailParse(unittest.TestCase):

    def setUp(self):
        self.row = sk_parse.parse_course_detail(DETAIL_STATE, job_id=9)

    def test_specialization_the_listing_cannot_give(self):
        self.assertEqual(self.row["specialization"], "Fashion Design")
        self.assertEqual(self.row["specialization_id"], 183)

    def test_hierarchy_ids(self):
        self.assertEqual(self.row["stream_id"], 3)
        self.assertEqual(self.row["substream_id"], 39)

    def test_level_and_credential(self):
        self.assertEqual(self.row["course_level"], "UG")
        self.assertEqual(self.row["credential"], "Degree")

    def test_attributes(self):
        self.assertEqual(self.row["education_type"], "Full Time")
        self.assertEqual(self.row["delivery_method"], "Classroom")
        self.assertEqual(json.loads(self.row["medium"]), ["English"])

    def test_fee_breakdown_not_one_number(self):
        self.assertEqual(self.row["fees_total"], 1940000)
        self.assertEqual(self.row["fees_onetime"], 75000)
        self.assertEqual(self.row["fees_year"], 2026)
        self.assertEqual(self.row["fees_currency"], "INR")

    def test_a_page_without_coursedata_is_none_not_a_blank_row(self):
        self.assertIsNone(sk_parse.parse_course_detail({"childPageData": {}}))
        self.assertIsNone(sk_parse.parse_course_detail({"courseData": {}}))

    def test_missing_fee_block_does_not_raise(self):
        st = {"courseData": dict(DETAIL_STATE["courseData"])}
        st["courseData"].pop("courseFees")
        row = sk_parse.parse_course_detail(st)
        self.assertIsNone(row["fees_total"])
        self.assertEqual(row["specialization"], "Fashion Design")


class FeeBreakdown(unittest.TestCase):
    """Every money figure is a category-keyed map. The scalars read `general`;
    the _json twins keep the map, so a category we have not seen yet is stored
    rather than dropped."""

    def setUp(self):
        self.a = sk_parse.parse_course_detail(DETAIL_STATE)          # college 72
        self.b = sk_parse.parse_course_detail(DETAIL_STATE_NIT)      # NIT Trichy

    def test_all_four_money_buckets(self):
        self.assertEqual(self.b["fees_total"], 500000)
        self.assertEqual(self.b["fees_onetime"], 35300)
        self.assertEqual(self.b["fees_hostel"], 375000)
        self.assertEqual(self.b["fees_other"], 152100)
        self.assertIsNone(self.b["fees_deposit"])

    def test_an_empty_bucket_stores_blank_not_an_empty_object(self):
        """'' so the preserve_nonempty upsert reads it as 'nothing to say'
        instead of overwriting a value an earlier run captured."""
        self.assertEqual(self.a["fees_hostel_json"], "")
        self.assertEqual(self.a["fees_period_json"], "")

    def test_the_whole_map_survives_not_just_general(self):
        self.assertEqual(json.loads(self.b["fees_total_json"])["general"]["value"],
                         500000)
        self.assertEqual(json.loads(self.b["fees_hostel_json"])["general"]["value"],
                         375000)

    def test_a_second_category_would_not_be_lost(self):
        """No live page has yet published one — this proves the column would
        carry it if one did, instead of a scalar quietly reading `general`."""
        st = json.loads(json.dumps(DETAIL_STATE_NIT))
        st["courseData"]["courseFees"]["fees"]["totalFees"]["sc"] = {
            "value": 125000, "category": "sc"}
        row = sk_parse.parse_course_detail(st)
        self.assertEqual(row["fees_total"], 500000)            # general
        self.assertEqual(json.loads(row["fees_total_json"])["sc"]["value"],
                         125000)

    def test_the_prose_is_stored_because_it_qualifies_the_number(self):
        """NIT's one `general` figure actually covers OPEN/EWS/OBC-NCL/PWD — and
        only the description says so."""
        self.assertIn("OBC-NCL", self.b["fees_note"])
        self.assertIn("Meal Plan", self.b["fees_hostel_note"])
        self.assertIn("Security Deposit", self.a["fees_onetime_note"])

    def test_fee_year_is_per_course_not_per_site(self):
        self.assertEqual(self.a["fees_year"], 2026)
        self.assertEqual(self.b["fees_year"], 2025)

    def test_currency_and_brochure(self):
        self.assertEqual(self.a["fees_currency"], "INR")
        self.assertTrue(self.a["fees_brochure_url"].endswith(".pdf"))
        self.assertEqual(self.b["fees_brochure_url"], "")

    def test_category_name_mapping_is_kept(self):
        self.assertEqual(json.loads(self.a["fees_categories"]),
                         {"general": "General"})


class EligibilityAndSeats(unittest.TestCase):

    def setUp(self):
        self.a = sk_parse.parse_course_detail(DETAIL_STATE)
        self.b = sk_parse.parse_course_detail(DETAIL_STATE_NIT)

    def test_category_wise_scores_do_populate(self):
        """Unlike fees. NIT asks 75% general, 65% SC, 65% ST — so the
        category-keyed shape is real and used; it is the FEE maps specifically
        that collapse to `general`."""
        self.assertEqual(self.b["elig_xii_general"], 75)
        scores = json.loads(self.b["elig_xii_scores"])
        self.assertEqual(scores["sc"]["score"], 65)
        self.assertEqual(scores["st"]["score"], 65)
        self.assertEqual(self.b["elig_xii_score_type"], "percentage")

    def test_accepted_exams_carry_their_cutoffs(self):
        ex = json.loads(self.b["elig_exams_json"])
        self.assertEqual(ex[0]["id"], 6244)
        self.assertEqual(ex[0]["name"], "JEE Main")
        self.assertIn("cutoff", ex[0])

    def test_eligibility_prose_and_year(self):
        self.assertEqual(self.a["elig_year"], 2026)
        self.assertIn("AIEED Application Form", self.a["elig_note"])

    def test_absent_numeric_limits_stay_none(self):
        for k in ("elig_min_workex", "elig_min_age", "elig_max_age",
                  "elig_backlogs"):
            self.assertIsNone(self.a[k], k)

    def test_seats(self):
        self.assertEqual(self.a["seats_total"], 25)
        self.assertEqual(self.b["seats_total"], 119)
        self.assertEqual(self.a["seats_category_json"], "")


class AdmissionPlacementsAffiliation(unittest.TestCase):

    def setUp(self):
        self.a = sk_parse.parse_course_detail(DETAIL_STATE)
        self.b = sk_parse.parse_course_detail(DETAIL_STATE_NIT)

    def test_admission_steps_keep_their_order(self):
        steps = json.loads(self.a["admission_steps"])
        self.assertEqual([s["name"] for s in steps],
                         ["Register and Apply", "Entrance Test",
                          "Final Admission"])

    def test_steps_sort_numerically_not_lexically(self):
        st = json.loads(json.dumps(DETAIL_STATE))
        st["courseData"]["admissionProcess"]["10"] = {
            "admissionName": "Enrol", "description": "last"}
        steps = json.loads(sk_parse.parse_course_detail(st)["admission_steps"])
        self.assertEqual(steps[-1]["name"], "Enrol")   # "10" after "3", not "1"

    def test_empty_admission_process_is_blank(self):
        self.assertEqual(self.b["admission_steps"], "")

    def test_placement_grain_is_stored_beside_the_number(self):
        """Without it the salary is misleading: at college 72 the figure belongs
        to the SUBSTREAM, at NIT Trichy to the course itself."""
        self.assertEqual(self.a["placement_grain"], "substreamId")
        self.assertEqual(self.b["placement_grain"], "clientCourse")

    def test_placement_numbers(self):
        self.assertEqual(self.a["salary_max"], 1300000)
        self.assertEqual(self.a["placement_batch_year"], 2023)
        self.assertEqual(self.a["placement_pct"], 100.0)
        self.assertEqual(self.b["placement_pct"], 88.1)   # a float, not an int

    def test_recruiters(self):
        self.assertEqual(json.loads(self.a["recruiters"]),
                         ["Amrapali Group", "Fabindia"])
        self.assertEqual(len(json.loads(self.b["recruiters"])), 5)

    def test_affiliation_links_the_course_to_a_university(self):
        self.assertEqual(self.a["affiliation_university_id"], 23071)
        self.assertEqual(self.a["affiliation_scope"], "domestic")
        self.assertIn("Rajasthan", self.a["affiliation_name"])

    def test_highlights_and_empty_highlights(self):
        self.assertIn("Earn a degree", json.loads(self.a["highlights"])[0])
        self.assertEqual(self.b["highlights"], "")

    def test_important_dates_are_flattened_out_of_their_buckets(self):
        d = json.loads(self.a["important_dates_json"])
        self.assertEqual(d[0]["type"], "exam")
        self.assertEqual(d[0]["start"], [2026, 4, 6])
        self.assertEqual(d[0]["end"], [2026, 4, 18])

    def test_attributes_and_nzqf(self):
        self.assertEqual(self.a["course_type"], "Paid")
        self.assertEqual(self.b["institute_type"], "university")
        self.assertEqual(self.a["is_course_paid"], 1)
        self.assertEqual(self.b["is_course_paid"], 0)
        self.assertEqual(self.a["nzqf"], "")      # {"id":null,"name":null}

    def test_curriculum_pdf_comes_from_the_structure_block(self):
        self.assertTrue(self.a["curriculum_pdf"].endswith("c.pdf"))


class ColumnContract(unittest.TestCase):
    """The parser and the schema must not drift apart — a key with no column is
    silently dropped by the upsert, which is the quietest way to lose data."""

    def test_every_parsed_key_has_a_column(self):
        row = sk_parse.parse_course_detail(DETAIL_STATE_NIT)
        self.assertEqual(sorted(set(row) - set(sk_db.OFFERING_DEEP_COLS)), [])

    def test_the_two_passes_do_not_overlap_beyond_shared_identity(self):
        shared = set(sk_db.OFFERING_LISTING_COLS) & set(sk_db.OFFERING_DEEP_COLS)
        self.assertEqual(shared, {
            "college_id", "course_id", "course_name", "base_course_name",
            "credential", "duration", "curriculum_pdf", "source_job_id"})

    def test_every_declared_column_belongs_to_a_pass(self):
        declared = {c for c, _ in sk_db.OFFERING_COLUMNS}
        covered = set(sk_db.OFFERING_LISTING_COLS) | set(sk_db.OFFERING_DEEP_COLS)
        self.assertEqual(sorted(declared - covered), [])


class Writes(unittest.TestCase):
    """Drive the real writers against a real file — the columns must exist and
    the two passes must not erase each other."""

    def setUp(self):
        # Per test, not per class: two of these tests set deep_scraped_at, and a
        # shared file would let one decide the other's outcome.
        self.path = fresh_db()

    def rows(self):
        got = one(listing_state([TUPLE]))
        for r in got["offerings"]:
            r["listed_at"] = time.time()
        return got["offerings"]

    def test_listing_then_deep_keeps_both(self):
        sk_db.upsert_offering_listing(self.rows(), db_path=self.path)
        deep = sk_parse.parse_course_detail(DETAIL_STATE, job_id=9)
        deep["deep_scraped_at"] = time.time()
        sk_db.upsert_offering_deep([deep], db_path=self.path)
        with sk_db.connect(self.path) as conn:
            r = conn.execute(
                "SELECT fees_amount, total_seats, specialization, course_level,"
                "       fees_onetime, exams FROM sk_offerings "
                "WHERE college_id=72 AND course_id=306967").fetchone()
        self.assertEqual(r["fees_amount"], 1940000)   # from Ⓒ, survived Ⓓ
        self.assertEqual(r["total_seats"], 25)        # from Ⓒ, survived Ⓓ
        self.assertEqual(r["specialization"], "Fashion Design")   # from Ⓓ
        self.assertEqual(r["course_level"], "UG")                 # from Ⓓ
        self.assertEqual(r["fees_onetime"], 75000)                # from Ⓓ
        self.assertEqual(json.loads(r["exams"]), ["AIEED"])

    def test_every_deep_column_round_trips_through_sqlite(self):
        """A column named in OFFERING_DEEP_COLS but never ALTERed onto the table
        fails the whole INSERT. This drives the real writer with a full row so a
        missing column cannot reach production as a runtime error."""
        sk_db.upsert_offering_listing(self.rows(), db_path=self.path)
        deep = sk_parse.parse_course_detail(DETAIL_STATE_NIT, job_id=9)
        deep["college_id"], deep["course_id"] = 72, 306967   # reuse the edge
        deep["deep_scraped_at"] = time.time()
        sk_db.upsert_offering_deep([deep], db_path=self.path)
        with sk_db.connect(self.path) as conn:
            r = conn.execute(
                "SELECT fees_hostel, fees_other, fees_total_json, fees_note,"
                "       elig_xii_general, elig_xii_scores, elig_exams_json,"
                "       placement_grain, placement_pct, seats_total,"
                "       recruiters, institute_type, fees_year "
                "FROM sk_offerings WHERE college_id=72 AND course_id=306967"
            ).fetchone()
        self.assertEqual(r["fees_hostel"], 375000)
        self.assertEqual(r["fees_other"], 152100)
        self.assertEqual(json.loads(r["fees_total_json"])["general"]["value"],
                         500000)
        self.assertIn("OBC-NCL", r["fees_note"])
        self.assertEqual(r["elig_xii_general"], 75)
        self.assertEqual(json.loads(r["elig_xii_scores"])["sc"]["score"], 65)
        self.assertEqual(json.loads(r["elig_exams_json"])[0]["name"], "JEE Main")
        self.assertEqual(r["placement_grain"], "clientCourse")
        self.assertEqual(r["placement_pct"], 88.1)
        self.assertEqual(r["seats_total"], 119)
        self.assertEqual(len(json.loads(r["recruiters"])), 5)
        self.assertEqual(r["institute_type"], "university")
        self.assertEqual(r["fees_year"], 2025)

    def test_deep_pass_alone_cannot_blank_listing_columns(self):
        sk_db.upsert_offering_listing(self.rows(), db_path=self.path)
        sk_db.upsert_offering_deep(
            [{"college_id": 72, "course_id": 306967,
              "deep_scraped_at": time.time()}], db_path=self.path)
        with sk_db.connect(self.path) as conn:
            r = conn.execute("SELECT fees_amount, total_seats FROM sk_offerings "
                             "WHERE college_id=72 AND course_id=306967").fetchone()
        self.assertEqual(r["fees_amount"], 1940000)
        self.assertEqual(r["total_seats"], 25)

    def test_queue_requires_phase_b_done(self):
        sk_db.upsert_colleges([{"college_id": 4242, "slug": "x",
                                "url": "https://www.shiksha.com/college/x-4242"}],
                              db_path=self.path)
        ids = [c["college_id"] for c in
               sk_db.colleges_pending_courses(db_path=self.path)]
        self.assertNotIn(4242, ids)
        sk_db.set_college_progress(4242, "done", db_path=self.path)
        ids = [c["college_id"] for c in
               sk_db.colleges_pending_courses(db_path=self.path)]
        self.assertIn(4242, ids)

    def test_queue_drains_when_marked(self):
        sk_db.set_college_progress(4243, "done", db_path=self.path)
        sk_db.upsert_colleges([{"college_id": 4243, "slug": "y",
                                "url": "https://www.shiksha.com/college/y-4243"}],
                              db_path=self.path)
        sk_db.set_course_progress(4243, "done", pages=1, found=3, expected=3,
                                  db_path=self.path)
        ids = [c["college_id"] for c in
               sk_db.colleges_pending_courses(db_path=self.path)]
        self.assertNotIn(4243, ids)

    def test_gone_is_terminal(self):
        sk_db.set_college_progress(4244, "done", db_path=self.path)
        sk_db.upsert_colleges([{"college_id": 4244, "slug": "z",
                                "url": "https://www.shiksha.com/college/z-4244"}],
                              db_path=self.path)
        sk_db.set_course_progress(4244, "gone", db_path=self.path)
        ids = [c["college_id"] for c in
               sk_db.colleges_pending_courses(db_path=self.path)]
        self.assertNotIn(4244, ids)

    def test_error_stays_in_the_queue(self):
        sk_db.set_college_progress(4245, "done", db_path=self.path)
        sk_db.upsert_colleges([{"college_id": 4245, "slug": "w",
                                "url": "https://www.shiksha.com/college/w-4245"}],
                              db_path=self.path)
        sk_db.set_course_progress(4245, "error", message="boom",
                                  db_path=self.path)
        ids = [c["college_id"] for c in
               sk_db.colleges_pending_courses(db_path=self.path)]
        self.assertIn(4245, ids)

    def test_deep_queue_needs_a_url(self):
        sk_db.upsert_offering_listing(self.rows(), db_path=self.path)
        pend = sk_db.offerings_pending_deep(db_path=self.path)
        self.assertIn(306967, [p["course_id"] for p in pend])
        sk_db.upsert_offering_deep([{"college_id": 72, "course_id": 306967,
                                     "deep_scraped_at": time.time()}],
                                   db_path=self.path)
        pend = sk_db.offerings_pending_deep(db_path=self.path)
        self.assertNotIn(306967, [p["course_id"] for p in pend])


class FirstObservationIsNotAChange(unittest.TestCase):
    """Discovery creates every offering row and fingerprints it, so when phase Ⓒ
    later fills forty empty columns, `freshness` takes the 'changed' branch and
    logs one row PER FIELD — forty "changes" to a page nothing had ever looked
    at. Phase Ⓓ makes it worse: eighty more columns.

    Measured 2026-09-29 on 2,000 offerings through the real writers: 8.73 KB per
    row, of which 6.63 KB was change log — 2.01 GB of the 2.65 GB that Ⓒ+Ⓓ would
    cost across 317,907 offerings. Nulling the fingerprint for a row the phase
    has never written takes the other branch and logs a single `new`.

    What must NOT be lost is the log's actual job: a fee moving later is still a
    change and must still be diffed field by field."""

    def setUp(self):
        self.path = fresh_db()
        # discovery's thin row — the fingerprint that causes the problem
        sk_db.upsert_offerings([{"college_id": 72, "course_id": 306967,
                                 "url": "/college/x-72/course-y-306967",
                                 "course_slug": "course-y-306967",
                                 "discovered_at": time.time()}],
                               db_path=self.path)

    def changes(self, kind=None):
        sql = ("SELECT change_type, field, old_value, new_value FROM data_changes "
               "WHERE table_name='sk_offerings'")
        if kind:
            sql += " AND change_type='%s'" % kind
        with sk_db.connect(self.path) as c:
            return [tuple(r) for r in c.execute(sql)]

    def listing_row(self, **over):
        r = one(listing_state([TUPLE]))["offerings"][0]
        r.update({"listed_at": time.time(), "scraped_at": time.time()})
        r.update(over)
        return r

    def test_the_first_listing_write_logs_one_new_not_forty_changes(self):
        before = len(self.changes())
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        added = self.changes()[before:]
        self.assertEqual(len(added), 1, added)
        self.assertEqual(added[0][0], "new")

    def test_the_first_deep_write_also_logs_one_new(self):
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        before = len(self.changes())
        deep = sk_parse.parse_course_detail(DETAIL_STATE_NIT, job_id=9)
        deep.update({"college_id": 72, "course_id": 306967,
                     "deep_scraped_at": time.time()})
        sk_db.upsert_offering_deep([deep], db_path=self.path)
        added = self.changes()[before:]
        self.assertEqual(len(added), 1, added)
        self.assertEqual(added[0][0], "new")

    def test_a_genuine_later_change_is_still_diffed_field_by_field(self):
        """The whole point. Suppressing first observations must not suppress
        real ones — a fee moving is what the log exists for."""
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        sk_db.upsert_offering_listing(
            [self.listing_row(fees_amount=2500000)], db_path=self.path)
        changed = self.changes("changed")
        self.assertTrue(changed, "a real fee change was not logged")
        fields = {c[1] for c in changed}
        self.assertIn("fees_amount", fields)
        row = [c for c in changed if c[1] == "fees_amount"][0]
        self.assertEqual((row[2], row[3]), ("1940000", "2500000"))

    def test_a_second_pass_over_an_unchanged_row_logs_nothing_new(self):
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        n = len(self.changes())
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        self.assertEqual(len(self.changes()), n)

    def test_the_stamp_is_what_decides_not_the_row_existing(self):
        """Once listed_at is set the row is no longer a first observation, so a
        later write must go down the diff path even if deep columns are empty."""
        sk_db.upsert_offering_listing([self.listing_row()], db_path=self.path)
        with sk_db.connect(self.path) as c:
            n = sk_db.mark_first_observation(
                c, "sk_offerings", ["college_id", "course_id"],
                [{"college_id": 72, "course_id": 306967}], "listed_at")
        self.assertEqual(n, 0)

    def test_a_missing_stamp_column_is_not_an_error(self):
        with sk_db.connect(self.path) as c:
            self.assertEqual(
                sk_db.mark_first_observation(
                    c, "sk_offerings", ["college_id", "course_id"],
                    [{"college_id": 72, "course_id": 306967}], "no_such_column"), 0)

    def test_rows_without_keys_are_skipped(self):
        with sk_db.connect(self.path) as c:
            self.assertEqual(
                sk_db.mark_first_observation(
                    c, "sk_offerings", ["college_id", "course_id"],
                    [{"college_id": None, "course_id": None}], "listed_at"), 0)

    def test_no_visit_stamp_sits_inside_the_offerings_fingerprint(self):
        """A contract, not an example. `listed_at` was in the fingerprint
        because it does not end in `_scraped_at`, so the suffix rule missed it
        and the explicit list had not been extended — the exact failure mode the
        comment above VOLATILE_SUFFIXES warns about. This fails the moment
        someone adds another stamp under a new name."""
        import freshness as _fr
        with sk_db.connect(self.path) as c:
            tracked = set(_fr.tracked_columns(c, "sk_offerings"))
        hashed = {c for c in tracked if c.endswith("_at")}
        self.assertEqual(hashed, set(),
                         "these visit stamps are inside the fingerprint and "
                         "will log a phantom change on every re-crawl: %s"
                         % sorted(hashed))

    def test_a_batch_larger_than_sqlites_parameter_limit_works(self):
        """900 parameters is the chunk; a batch of 1,000 two-key rows is 2,000."""
        many = [{"college_id": 72, "course_id": 306967}] + [
            {"college_id": 99, "course_id": 500000 + i} for i in range(999)]
        with sk_db.connect(self.path) as c:
            n = sk_db.mark_first_observation(
                c, "sk_offerings", ["college_id", "course_id"], many, "listed_at")
        self.assertEqual(n, 1)     # only the one discovery row exists


class RunnerWiring(unittest.TestCase):
    """Drive the REAL runner with the transport monkeypatched at
    fetch_college_state — the same seam the phase Ⓑ tests use — so pagination,
    resume and the empty-parse guard are exercised, not just the parser."""

    def setUp(self):
        import sk_courses
        self.mod = sk_courses
        self.path = fresh_db()
        sk_db.upsert_colleges(
            [{"college_id": 72, "slug": SLUG,
              "url": f"https://www.shiksha.com/college/{SLUG}"}],
            db_path=self.path)
        sk_db.set_college_progress(72, "done", db_path=self.path)
        self.job = sk_db.create_job("courses", {"concurrency": 1})
        self.seen = []
        self._orig = self.mod.fetch_college_state

    def tearDown(self):
        self.mod.fetch_college_state = self._orig

    def run_with(self, pages):
        def fake(client, url, cid, route=None):
            self.seen.append(url)
            key = url.rsplit("/", 1)[-1]
            if key not in pages:
                raise self.mod.NoStateError(f"no state for {key}")
            return pages[key]
        self.mod.fetch_college_state = fake
        self.mod.run_course_listing(self.job, {"concurrency": 1, "delay": 0,
                                               "proxy_mode": "none"},
                                    log=lambda m: None)

    def test_follows_every_page_and_writes_every_row(self):
        p1 = listing_state([TUPLE])
        p2 = listing_state([dict(TUPLE, courseId=307021,
                                 name="B.Des. in Interior Design")],
                           pagination={"nextUrls": [], "currentPageNUmber": 2})
        self.run_with({"courses": p1, "courses-2": p2})
        self.assertEqual(len(self.seen), 2)
        self.assertTrue(self.seen[1].endswith("/courses-2"))
        with sk_db.connect(self.path) as conn:
            n = conn.execute("SELECT COUNT(*) FROM sk_offerings "
                             "WHERE college_id=72").fetchone()[0]
            p = conn.execute("SELECT status,pages,found,expected FROM "
                             "sk_course_progress WHERE college_id=72").fetchone()
        self.assertEqual(n, 2)
        self.assertEqual(p["status"], "done")
        self.assertEqual(p["pages"], 2)
        self.assertEqual(p["found"], 2)
        self.assertEqual(p["expected"], 42)

    def test_a_page_that_parses_to_nothing_is_an_error_not_done(self):
        """The phase Ⓑ mutation-test lesson: never mark a college done on a
        parse that produced no rows while the site advertised some."""
        empty = listing_state([], pagination={"nextUrls": []}, total=42)
        self.run_with({"courses": empty})
        with sk_db.connect(self.path) as conn:
            p = conn.execute("SELECT status FROM sk_course_progress "
                             "WHERE college_id=72").fetchone()
        self.assertEqual(p["status"], "error")
        self.assertIn(72, [c["college_id"] for c in
                           sk_db.colleges_pending_courses(db_path=self.path)])

    def test_a_college_that_genuinely_has_no_courses_is_done(self):
        empty = listing_state([], pagination={"nextUrls": []}, total=0)
        self.run_with({"courses": empty})
        with sk_db.connect(self.path) as conn:
            p = conn.execute("SELECT status FROM sk_course_progress "
                             "WHERE college_id=72").fetchone()
        self.assertEqual(p["status"], "done")

    def test_short_count_is_recorded_not_hidden(self):
        p1 = listing_state([TUPLE], pagination={"nextUrls": []}, total=42)
        self.run_with({"courses": p1})
        with sk_db.connect(self.path) as conn:
            p = conn.execute("SELECT found,expected,message FROM "
                             "sk_course_progress WHERE college_id=72").fetchone()
        self.assertEqual((p["found"], p["expected"]), (1, 42))
        self.assertIn("42", p["message"])

    def test_self_referential_pagination_cannot_loop(self):
        loop = listing_state([TUPLE], pagination={
            "nextUrls": [{"pageNumber": 1, "url": f"/college/{SLUG}/courses"}]})
        self.run_with({"courses": loop})
        self.assertEqual(len(self.seen), 1)

    def test_a_redirect_is_recorded_not_passed_over(self):
        st = listing_state([dict(TUPLE, instituteId=99)],
                           pagination={"nextUrls": []}, total=1)
        st["childPageData"]["listingId"] = 99
        self.run_with({"courses": st})
        with sk_db.connect(self.path) as conn:
            p = conn.execute("SELECT status,message FROM sk_course_progress "
                             "WHERE college_id=72").fetchone()
            owner = conn.execute("SELECT college_id FROM sk_offerings "
                                 "WHERE course_id=306967").fetchone()[0]
        self.assertEqual(p["status"], "done")
        self.assertIn("redirected to 99", p["message"])
        self.assertEqual(owner, 99)

    def test_no_state_marks_gone_and_drains(self):
        self.run_with({})
        with sk_db.connect(self.path) as conn:
            p = conn.execute("SELECT status FROM sk_course_progress "
                             "WHERE college_id=72").fetchone()
        self.assertEqual(p["status"], "gone")
        self.assertNotIn(72, [c["college_id"] for c in
                              sk_db.colleges_pending_courses(db_path=self.path)])


if __name__ == "__main__":
    unittest.main(verbosity=2, exit=False)
