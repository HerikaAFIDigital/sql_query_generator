"""
Automated Test Suite for the Safe Delivery Analytics Direct Pipeline.

Usage:
    .venv/bin/python test_suite.py             # run all tests (calls LLM)
    .venv/bin/python test_suite.py --verbose   # also show generated SQL

What it does:
- Runs each test question through ask_qwen_sql() (the Direct Fast Pipeline)
- Compares numeric answers to KNOWN ground truth fetched live from the DB
- Prints PASS / FAIL / ERROR with expected vs actual values
- Shows a final score and lists all failures
"""

import argparse
import os
import re
import sys
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

PG_KWARGS = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
    user=os.getenv("POSTGRES_USER", "vanna_readonly"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

# ─────────────────────────────────────────────────────────────────
# GROUND TRUTH  (fetched live from DB so it stays accurate)
# ─────────────────────────────────────────────────────────────────
GT = {}

def fetch_ground_truth():
    conn = psycopg2.connect(**PG_KWARGS)
    cur = conn.cursor()
    def q(sql):
        cur.execute(sql)
        r = cur.fetchone()
        return r[0] if r else 0
    GT["total_users"]             = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events")
    GT["language_screen"]         = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE screen_name ILIKE '%language_list%'")
    GT["category_screen"]         = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE screen_name ILIKE '%category_list%'")
    GT["onboarding_complete"]     = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE screen_name ILIKE '%onboarding_complete%'")
    GT["onboarding_v400"]         = q("SELECT COUNT(DISTINCT a.profile_id) FROM app_lifecycle_events a JOIN v_all_events v ON a.profile_id=v.profile_id WHERE a.app_version='4.0.0' AND v.screen_name ILIKE '%onboarding_complete%'")
    GT["onboarding_below_v400"]   = q("SELECT COUNT(DISTINCT a.profile_id) FROM app_lifecycle_events a JOIN v_all_events v ON a.profile_id=v.profile_id WHERE a.app_version<'4.0.0' AND v.screen_name ILIKE '%onboarding_complete%'")
    GT["shared_yes"]              = q("SELECT COUNT(DISTINCT profile_id) FROM onboarding_events WHERE question_id ILIKE '%shared phone%' AND answer_id='yes'")
    GT["shared_no"]               = q("SELECT COUNT(DISTINCT profile_id) FROM onboarding_events WHERE question_id ILIKE '%shared phone%' AND answer_id='no'")
    GT["healthcare_yes"]          = q("SELECT COUNT(DISTINCT profile_id) FROM onboarding_events WHERE question_id ILIKE '%healthcare professional%' AND answer_id='yes'")
    GT["healthcare_no"]           = q("SELECT COUNT(DISTINCT profile_id) FROM onboarding_events WHERE question_id ILIKE '%healthcare professional%' AND answer_id='no'")
    GT["android_users"]           = q("SELECT COUNT(DISTINCT profile_id) FROM app_lifecycle_events WHERE device_os='Android'")
    GT["users_sep20"]             = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE DATE(event_time)='2026-09-20'")
    GT["users_sep21"]             = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE DATE(event_time)='2026-09-21'")
    GT["home_screen"]             = q("SELECT COUNT(DISTINCT profile_id) FROM v_all_events WHERE screen_name ILIKE '%home_screen%'")
    GT["dropped_before_category"] = q("SELECT COUNT(DISTINCT a.profile_id) FROM app_lifecycle_events a WHERE a.profile_id NOT IN (SELECT DISTINCT profile_id FROM download_category_events)")
    GT["v400_total"]              = q("SELECT COUNT(DISTINCT profile_id) FROM app_lifecycle_events WHERE app_version='4.0.0'")
    GT["below_v400_total"]        = q("SELECT COUNT(DISTINCT profile_id) FROM app_lifecycle_events WHERE app_version<'4.0.0'")
    cur.close()
    conn.close()
    print(f"  Ground truth loaded: {GT}")


# ─────────────────────────────────────────────────────────────────
# CHECK HELPERS
# ─────────────────────────────────────────────────────────────────

def _numbers_in_rows(rows):
    nums = []
    for row in rows:
        for v in row.values():
            try:
                nums.append(int(float(str(v))))
            except Exception:
                pass
    return nums


def check_single(key):
    def _check(rows):
        expected = GT[key]
        nums = _numbers_in_rows(rows)
        got = nums[0] if nums else None
        ok = got == expected
        return ok, f"expected={expected}  got={got}  all_numbers={nums}"
    return _check


def check_both_present(key_a, key_b):
    def _check(rows):
        ea, eb = GT[key_a], GT[key_b]
        nums = _numbers_in_rows(rows)
        ok = ea in nums and eb in nums
        return ok, f"need {ea} and {eb} in rows, got={sorted(set(nums))}"
    return _check


def check_has_rows(min_rows=1):
    def _check(rows):
        ok = len(rows) >= min_rows
        return ok, f"rows={len(rows)} (need >={min_rows})"
    return _check


# ─────────────────────────────────────────────────────────────────
# TEST DEFINITIONS  (25 tests across 7 categories)
# ─────────────────────────────────────────────────────────────────

TESTS = [
    # ── Screens / Funnel ────────────────────────────────────────
    ("T01", "Screens",       "How many users opened language screen and how many reached category screen please do a comparison?", check_both_present("language_screen", "category_screen")),
    ("T02", "Screens",       "How many users visited the language screen?",                         check_single("language_screen")),
    ("T03", "Screens",       "How many users reached category screen?",                             check_single("category_screen")),
    ("T04", "Screens",       "How many users made it to the home screen?",                          check_single("home_screen")),
    ("T05", "Screens",       "How many users installed the app but dropped off before category screen?", check_single("dropped_before_category")),

    # ── Onboarding ──────────────────────────────────────────────
    ("T06", "Onboarding",    "How many users completed all onboarding steps till now?",             check_single("onboarding_complete")),
    ("T07", "Onboarding",    "How many users have completed all onbaording steps till now inversiom 4.0.0", check_single("onboarding_v400")),
    ("T08", "Onboarding",    "How many users have completed all onboarding steps in version 4.0.0 and in version less than 4.0.0?", check_both_present("onboarding_v400", "onboarding_below_v400")),

    # ── Survey / Onboarding Questions ───────────────────────────
    ("T09", "Survey",        "How many users answered yes to is this device shared and how many said no", check_both_present("shared_yes", "shared_no")),
    ("T10", "Survey",        "How many users said yes to the shared phone question?",               check_single("shared_yes")),
    ("T11", "Survey",        "How many users said no to the shared phone question?",                check_single("shared_no")),
    ("T12", "Survey",        "Are you a healthcare professional yes no breakdown",                  check_both_present("healthcare_yes", "healthcare_no")),
    ("T13", "Survey",        "How many users said they are healthcare professionals?",              check_single("healthcare_yes")),
    ("T14", "Survey",        "Show me all onboarding survey questions and answer counts",           check_has_rows(3)),

    # ── Users & Dates ────────────────────────────────────────────
    ("T15", "Users & Dates", "How many total users do we have?",                                    check_single("total_users")),
    ("T16", "Users & Dates", "How many users were there on 20th of september",                      check_single("users_sep20")),
    ("T17", "Users & Dates", "How many users were active on 21 september 2026?",                    check_single("users_sep21")),
    ("T18", "Users & Dates", "Daily user count breakdown by date",                                  check_has_rows(2)),

    # ── App Versions ─────────────────────────────────────────────
    ("T19", "App Versions",  "How many users are on version 4.0.0?",                               check_single("v400_total")),
    ("T20", "App Versions",  "Breakdown of users by app version",                                   check_has_rows(2)),

    # ── Device & OS ──────────────────────────────────────────────
    ("T21", "Device & OS",   "Breakdown of users by device OS",                                    check_has_rows(2)),
    ("T22", "Device & OS",   "How many users are on Android?",                                     check_single("android_users")),
    ("T23", "Device & OS",   "How many users on Android downloaded a category?",                   check_has_rows(1)),

    # ── Languages ────────────────────────────────────────────────
    ("T24", "Languages",     "Which languages had the most downloads?",                             check_has_rows(1)),
    ("T25", "Languages",     "Show language download breakdown",                                    check_has_rows(1)),
]


PASS_SYM  = "PASS "
FAIL_SYM  = "FAIL "
ERROR_SYM = "ERROR"


def run_tests(verbose=False):
    print()
    print("=" * 80)
    print("  Safe Delivery Analytics — Automated Test Suite  (Direct Fast Pipeline)")
    print("=" * 80)
    print()

    fetch_ground_truth()

    # Import pipeline after env is loaded
    sys.path.insert(0, ".")
    from ui import ask_qwen_sql  # noqa: E402

    results = []
    cat_results = {}

    for (tid, cat, question, check_fn) in TESTS:
        short_q = question[:60] + ("..." if len(question) > 60 else "")
        print(f"\n[{tid}] [{cat}]")
        print(f"  Q: {question}")

        t0 = time.time()
        status = FAIL_SYM
        detail = ""
        sql_used = ""
        rows = []

        try:
            answer_text, rows = ask_qwen_sql(question)
            elapsed = round(time.time() - t0, 1)

            sql_match = re.search(r"```sql\s*([\s\S]+?)\s*```", answer_text)
            sql_used = sql_match.group(1).strip() if sql_match else ""

            is_error = any(e in answer_text.lower() for e in [
                "sql error:", "couldn't generate", "fix attempt also failed", "syntax error"
            ])

            if is_error:
                status = ERROR_SYM
                detail = answer_text[:150].replace("\n", " ")
            else:
                passed, detail = check_fn(rows)
                status = PASS_SYM if passed else FAIL_SYM

            detail = f"{detail}  [{elapsed}s]"

        except Exception as exc:
            status = ERROR_SYM
            detail = str(exc)[:150]

        icon = "✅" if status == PASS_SYM else ("💥" if status == ERROR_SYM else "❌")
        print(f"  {icon} {status} — {detail}")
        if verbose and sql_used:
            print(f"  SQL: {sql_used[:120]}{'...' if len(sql_used) > 120 else ''}")

        results.append((tid, cat, question, status, detail))
        cat_results.setdefault(cat, []).append(status)

    # ── Summary ────────────────────────────────────────────────
    total   = len(results)
    passed  = sum(1 for _, _, _, s, _ in results if s == PASS_SYM)
    failed  = sum(1 for _, _, _, s, _ in results if s == FAIL_SYM)
    errors  = sum(1 for _, _, _, s, _ in results if s == ERROR_SYM)

    print()
    print("=" * 80)
    print("  FINAL RESULTS")
    print("=" * 80)
    print(f"  ✅ PASS  : {passed}/{total}  ({100*passed//total}%)")
    print(f"  ❌ FAIL  : {failed}")
    print(f"  💥 ERROR : {errors}")
    print()
    print("  By Category:")
    for cat, statuses in cat_results.items():
        cat_pass = sum(1 for s in statuses if s == PASS_SYM)
        bar = "█" * cat_pass + "░" * (len(statuses) - cat_pass)
        print(f"    {cat:<25s}  {bar}  {cat_pass}/{len(statuses)}")

    if failed + errors > 0:
        print()
        print("  FAILURES TO FIX:")
        for tid, cat, q, status, detail in results:
            if status in (FAIL_SYM, ERROR_SYM):
                icon = "💥" if status == ERROR_SYM else "❌"
                print(f"    {icon} {tid}  {q[:65]}")
                print(f"         {detail}")

    print()
    print("=" * 80)
    return passed, failed, errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true", help="Show generated SQL for each test")
    args = parser.parse_args()
    passed, failed, errors = run_tests(verbose=args.verbose)
    sys.exit(1 if (failed + errors) > 0 else 0)
