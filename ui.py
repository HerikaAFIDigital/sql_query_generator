import asyncio
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from sqlalchemy import create_engine

from vanna import Agent
from vanna.core.enhancer import LlmContextEnhancer
from vanna.core.lifecycle import LifecycleHook
from vanna.core.registry import ToolRegistry
from vanna.core.user import User
from vanna.core.user.request_context import RequestContext
from vanna.core.user.resolver import UserResolver
from vanna.integrations.local.agent_memory import DemoAgentMemory
from vanna.integrations.ollama import OllamaLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.tools import RunSqlTool

from journey_engine import UserJourneyEngine, parse_journey_query

# ============================================================
# 1. ASYNCIO FIX
# ============================================================
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

load_dotenv()

# ============================================================
# 2. POSTGRES CONFIGURATION
# ============================================================
PG_KWARGS = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
    user=os.getenv("POSTGRES_USER", "vanna_readonly"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

TEXT_TYPES = ("text", "character varying", "varchar", "char", "character")

# Maternity Foundation palette — crimson + charcoal + white
PALETTE = ["#A80041", "#1D1D1B", "#D9004B", "#555555", "#6B6B6B", "#888888", "#BBBBBB", "#333333"]

# ============================================================
# 3. ANSWER CACHE
# ============================================================
CACHE_DB_PATH = "query_cache.db"
CACHE_TTL_SECONDS = 60 * 60 * 24 * 7  # 1 week


def _normalize_question(question: str, engine: str = "") -> str:
    norm = " ".join(question.strip().lower().split())
    if engine:
        return f"{engine.strip().lower()}::{norm}"
    return norm


def _init_cache_db():
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS answer_cache (
            question_key TEXT PRIMARY KEY,
            original_question TEXT,
            answer_text TEXT,
            rows_json TEXT,
            created_at REAL,
            hit_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()


def get_cached_answer(question: str, engine: str = ""):
    key = _normalize_question(question, engine)
    conn = sqlite3.connect(CACHE_DB_PATH)
    cur = conn.execute(
        "SELECT answer_text, rows_json, created_at FROM answer_cache WHERE question_key = ?",
        (key,),
    )
    row = cur.fetchone()
    if row is None:
        conn.close()
        return None
    answer_text, rows_json, created_at = row

    # Auto-evict if cached entry is an error or failed attempt
    if any(err_word in (answer_text or "").lower() for err_word in ["sql error:", "syntax error", "fix attempt also failed"]):
        conn.execute("DELETE FROM answer_cache WHERE question_key = ?", (key,))
        conn.commit()
        conn.close()
        return None

    if CACHE_TTL_SECONDS is not None and (time.time() - created_at) > CACHE_TTL_SECONDS:
        conn.execute("DELETE FROM answer_cache WHERE question_key = ?", (key,))
        conn.commit()
        conn.close()
        return None
    conn.execute("UPDATE answer_cache SET hit_count = hit_count + 1 WHERE question_key = ?", (key,))
    conn.commit()
    conn.close()
    rows = json.loads(rows_json) if rows_json else []
    return answer_text, rows


def store_cached_answer(question: str, answer_text: str, rows: list, engine: str = ""):
    if not answer_text and not rows:
        return
    # Never cache SQL errors or failures
    if any(err_word in (answer_text or "").lower() for err_word in ["sql error:", "syntax error", "fix attempt also failed", "couldn't generate a valid sql"]):
        return
    key = _normalize_question(question, engine)
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.execute(
        """
        INSERT INTO answer_cache (question_key, original_question, answer_text, rows_json, created_at, hit_count)
        VALUES (?, ?, ?, ?, ?, 0)
        ON CONFLICT(question_key) DO UPDATE SET
            answer_text = excluded.answer_text,
            rows_json = excluded.rows_json,
            created_at = excluded.created_at
        """,
        (key, question, answer_text, json.dumps(rows) if rows else None, time.time()),
    )
    conn.commit()
    conn.close()


def get_cache_stats():
    conn = sqlite3.connect(CACHE_DB_PATH)
    cur = conn.execute("SELECT COUNT(*), COALESCE(SUM(hit_count), 0) FROM answer_cache")
    count, total_hits = cur.fetchone()
    conn.close()
    return count, total_hits


def clear_cache():
    conn = sqlite3.connect(CACHE_DB_PATH)
    conn.execute("DELETE FROM answer_cache")
    conn.commit()
    conn.close()


_init_cache_db()


# ============================================================
# 4. DATABASE SCHEMA CONTEXT & DOMAIN RULES
# ============================================================

@st.cache_resource
def _get_schema_context() -> str:
    """Load full DB schema once and cache it for the process lifetime."""
    try:
        conn = psycopg2.connect(**PG_KWARGS)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        lines = []
        for table in tables:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table,))
            cols = cur.fetchall()
            if not cols:
                continue
            col_parts = []
            for name, dtype in cols:
                part = f"{name} ({dtype})"
                # Add enum values for low-cardinality text columns
                if dtype in TEXT_TYPES and name not in {
                    "selected_categories", "selected_category_titles", "extra",
                    "id", "session_id", "app_installation_id", "category_id",
                    "language_id", "source_file", "ingested_at", "event_timestamp",
                } and not table.startswith("v_"):
                    try:
                        cur.execute(f'SELECT COUNT(DISTINCT "{name}") FROM "{table}"')
                        row = cur.fetchone()
                        if row and 0 < row[0] <= 30:
                            cur.execute(f'SELECT DISTINCT "{name}" FROM "{table}" WHERE "{name}" IS NOT NULL ORDER BY 1')
                            values = [str(r[0]) for r in cur.fetchall()]
                            part += f" [values: {', '.join(repr(v) for v in values)}]"
                    except Exception:
                        pass
                col_parts.append(part)
            lines.append(f"- {table}({', '.join(col_parts)})")
        cur.close()
        conn.close()
        return "\n".join(lines)
    except Exception as exc:
        return f"(schema lookup failed: {exc})"



# ============================================================
# 4b. ONBOARDING QUESTION FUZZY MATCHER
# ============================================================

# Exact question_id values as stored in the database.
# Used to validate/correct LLM-generated ILIKE patterns.
ONBOARDING_QUESTIONS = [
    "Is this device a shared phone/tablet?",
    "Approximately how many users share this device?",
    "Are you a healthcare professional (or studying to become one)?",
    "What is your profession?",
    "How many years of experience do you have as a healthcare professional?",
    "Where do you work?",
    "How did you hear about the Safe Delivery app?",
]

# Keyword aliases: user phrases → safe ILIKE pattern guaranteed to match
QUESTION_KEYWORD_ALIASES = {
    "shared": "%shared phone%",
    "shared phone": "%shared phone%",
    "shared device": "%shared phone%",
    "device shared": "%shared phone%",
    "is this device": "%shared phone%",
    "how many users share": "%Approximately how many%",
    "how many share": "%Approximately how many%",
    "users share": "%Approximately how many%",
    "healthcare professional": "%healthcare professional%",
    "healthcare": "%healthcare professional%",
    "profession": "%profession%",
    "what is your profession": "%profession%",
    "years of experience": "%years of experience%",
    "experience": "%years of experience%",
    "where do you work": "%Where do you work%",
    "where work": "%Where do you work%",
    "how did you hear": "%How did you hear%",
    "hear about": "%How did you hear%",
}


def fix_onboarding_ilike(sql: str) -> str:
    """
    Detects bad question_id ILIKE patterns in queries against onboarding_events
    and replaces them with patterns guaranteed to match actual DB values.
    Strategy: for each ILIKE pattern found, check if user phrase maps to a known alias.
    """
    if "onboarding_events" not in sql:
        return sql

    def replace_ilike(m):
        raw_pattern = m.group(1).strip("'%").lower()
        # Check keyword aliases first
        for kw, safe_pattern in QUESTION_KEYWORD_ALIASES.items():
            if kw in raw_pattern:
                return '"question_id" ILIKE ' + "'" + safe_pattern + "'"
        # Check if pattern matches any actual question (substring match)
        for q in ONBOARDING_QUESTIONS:
            if raw_pattern in q.lower() or any(w in q.lower() for w in raw_pattern.split() if len(w) > 3):
                # Use first meaningful word from the actual question as safe anchor
                words = [w for w in q.split() if len(w) > 3]
                anchor = words[0] if words else raw_pattern
                return '"question_id" ILIKE ' + "'%" + anchor + "%'"
        return m.group(0)  # unchanged if no match found

    _pat = '"?question_id"?' + r'\s+ILIKE\s+' + "('[^']+')"  # match: question_id ILIKE 'xxx'
    sql = re.sub(_pat, replace_ilike, sql, flags=re.IGNORECASE)
    return sql


TABLE_NOTES = """
## CRITICAL ARCHITECTURE & RELATIONSHIP RULES:
1. `profile_id` is the shared user identifier across ALL tables (app_lifecycle_events, download_language_events, download_category_events, onboarding_events, terms_conditions_events, interaction_events, v_all_events).
2. Joining multiple tables:
   - When a question requires filtering or aggregating across multiple tables (e.g. users on Android who downloaded a category), JOIN the tables on "profile_id".
   - Example: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a JOIN "download_category_events" c ON a."profile_id" = c."profile_id" WHERE a."device_os" = 'Android' AND c."event_type" = 'downloaded';
3. `event_time` (timestamp) is the primary time column for date filtering. Use: DATE("event_time") = 'YYYY-MM-DD'.
4. Always use `COUNT(DISTINCT "profile_id")` when counting users.
5. Do NOT use LIMIT unless specifically asked.
6. Always qualify table names with double-quotes e.g. "download_category_events", "v_all_events".
7. NEVER append SQL line comments (-- ...) to the query. Output ONLY the SQL statement.

## DOMAIN MAPPING & VERB ROUTING REFERENCE (From Ingestion Service):
- Machine-generated screen views are logged in `"interaction_events"` with `screen_name`.
- Domain actions (downloads, quiz answers, logins) are logged in their domain tables.
- **Unified Master View (`"v_all_events"`)**:
  - Combines ALL tables with: profile_id, event_time, event_timestamp, event_type, description, screen_name, log_source, session_id, autonym_script.
  - ALWAYS use `"v_all_events"` for cross-screen comparisons, funnels, drop-offs, retention, journeys.

## SCREEN NAME VARIANTS & DYNAMIC NORMALIZATION - CRITICAL:
  The database has screen names in MULTIPLE casing styles from different app versions (e.g. 'language_list_screen' vs 'LanguageListScreen', 'splash_screen' vs 'Splash').
  - When matching a specific screen in WHERE clauses, ALWAYS use ILIKE with wildcards:
    - Language screen:        screen_name ILIKE '%language_list%'
    - Category screen:        screen_name ILIKE '%category_list%'
    - Onboarding complete:    screen_name ILIKE '%onboarding_complete%'
    - Onboarding survey:      screen_name ILIKE '%onboarding_survey%'
    - Home screen:            screen_name ILIKE '%home_screen%' OR screen_name = 'HomeScreen'
    - Splash screen:          screen_name ILIKE '%splash%'
  - When GROUPING by screen name across all screens, ALWAYS normalize screen names dynamically by removing '_screen', 'Screen', and underscores:
    - Example: `SELECT LOWER(REPLACE(REPLACE(REPLACE(screen_name, '_screen', ''), 'Screen', ''), '_', '')) AS screen, COUNT(DISTINCT profile_id) AS user_count FROM v_all_events WHERE screen_name IS NOT NULL GROUP BY 1 ORDER BY user_count DESC;`

## USER DROP-OFF & FUNNEL ANALYSIS RULES (MANDATORY):
- When the user asks "where did users drop off", "drop off comparison", or "last screen before leaving":
  Determine the LAST active screen for each user dynamically:
  ```sql
  WITH last_screen_per_user AS (
      SELECT DISTINCT ON (profile_id)
          profile_id,
          screen_name,
          event_time
      FROM v_all_events
      WHERE screen_name IS NOT NULL
      ORDER BY profile_id, event_time DESC
  )
  SELECT 
      LOWER(REPLACE(REPLACE(REPLACE(screen_name, '_screen', ''), 'Screen', ''), '_', '')) AS drop_off_screen,
      COUNT(DISTINCT profile_id) AS user_count
  FROM last_screen_per_user
  GROUP BY 1
  ORDER BY user_count DESC;
  ```
  This is completely dynamic and works for any current or future screens without hardcoding.

## SCREEN & EVENT LOGGING RULES (MANDATORY):
- **Screen Comparisons**: MUST query "v_all_events" with ILIKE on screen_name.
- **Language Screen**: "download_language_events" and v_all_events (ILIKE '%language_list%'). Event types: selected, downloaded, cancelled.
- **Category Screen**: "download_category_events" and v_all_events (ILIKE '%category_list%'). Event types: selected, downloaded, interacted, started.
- **App Install/Launch**: "app_lifecycle_events". Columns: device_model, device_os, app_version, app_id. Event types: installed, started, app_launch.
- **Terms & Conditions**: "terms_conditions_events". Event type: accepted, is_accepted = '1'.
- **Onboarding Survey**: "onboarding_events". Event type: answered. question_id = question text; answer_id = answer code.
- **Onboarding Completion**:
  - ALWAYS use: WHERE "screen_name" ILIKE '%onboarding_complete%'
  - This covers BOTH 'onboarding_complete_screen' AND 'OnboardingCompleteScreen' variants.
  - Total completions: SELECT COUNT(DISTINCT "profile_id") FROM "v_all_events" WHERE "screen_name" ILIKE '%onboarding_complete%';
  - Filtered by version: SELECT COUNT(DISTINCT a."profile_id") AS "user_count" FROM "app_lifecycle_events" a JOIN "v_all_events" v ON a."profile_id" = v."profile_id" WHERE a."app_version" = '4.0.0' AND v."screen_name" ILIKE '%onboarding_complete%';
  - Version comparison (version X AND below X): Use UNION ALL with app_version = 'X' and app_version < 'X'.
- **App Version Filtering**: app_version in "app_lifecycle_events" (e.g. '4.0.0', '3.9.3', '3.6.8').
- **Content & Modules**: "content_events", "clinical_content_events". Columns: resource_title, resource_id, resource_type, event_value.
- **Quiz & Learning**: "learning_attempts", "learning_results", "champion_attempts", "champion_results".

## DATE & TIME RANGE RULES (MANDATORY):
- **Dataset Timeframe**: All data is from **September 2026** (September 19, 20, 21 of 2026).
- If no year given, ALWAYS use 2026. NEVER use 2023, 2024, or 2025!

FEW-SHOT EXAMPLES:
User: Give me a graph of comparison of all users who dropped where / where did users drop off
SQL: WITH last_screen_per_user AS (SELECT DISTINCT ON ("profile_id") "profile_id", "screen_name", "event_time" FROM "v_all_events" WHERE "screen_name" IS NOT NULL ORDER BY "profile_id", "event_time" DESC) SELECT LOWER(REPLACE(REPLACE("screen_name", '_screen', ''), 'Screen', '')) AS "drop_off_screen", COUNT(DISTINCT "profile_id") AS "user_count" FROM last_screen_per_user GROUP BY 1 ORDER BY "user_count" DESC;

User: How many users have completed all onboarding steps till now in version 4.0.0?
SQL: SELECT COUNT(DISTINCT a."profile_id") AS "user_count" FROM "app_lifecycle_events" a JOIN "v_all_events" v ON a."profile_id" = v."profile_id" WHERE a."app_version" = '4.0.0' AND v."screen_name" ILIKE '%onboarding_complete%';

User: How many users have completed all onboarding steps in version 4.0.0 and in version less than 4.0.0?
SQL: SELECT '4.0.0' AS "version", COUNT(DISTINCT a."profile_id") AS "user_count" FROM "app_lifecycle_events" a JOIN "v_all_events" v ON a."profile_id" = v."profile_id" WHERE a."app_version" = '4.0.0' AND v."screen_name" ILIKE '%onboarding_complete%' UNION ALL SELECT 'Below 4.0.0' AS "version", COUNT(DISTINCT a."profile_id") AS "user_count" FROM "app_lifecycle_events" a JOIN "v_all_events" v ON a."profile_id" = v."profile_id" WHERE a."app_version" < '4.0.0' AND v."screen_name" ILIKE '%onboarding_complete%';

User: How many users opened language screen and how many reached category screen please do a comparison?
SQL: SELECT 'Language Screen' AS "screen", COUNT(DISTINCT "profile_id") AS "user_count" FROM "v_all_events" WHERE "screen_name" ILIKE '%language_list%' UNION ALL SELECT 'Category Screen' AS "screen", COUNT(DISTINCT "profile_id") AS "user_count" FROM "v_all_events" WHERE "screen_name" ILIKE '%category_list%' ORDER BY "user_count" DESC;

User: How many users were there on 20th of september
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "v_all_events" WHERE DATE("event_time") = '2026-09-20';

User: Daily user count / breakdown of users by date
SQL: SELECT DATE("event_time") AS "event_date", COUNT(DISTINCT "profile_id") AS "user_count" FROM "v_all_events" GROUP BY DATE("event_time") ORDER BY "event_date" ASC;

User: Breakdown of users on 20th september
SQL: SELECT "screen_name", COUNT(DISTINCT "profile_id") AS "user_count" FROM "v_all_events" WHERE DATE("event_time") = '2026-09-20' GROUP BY "screen_name" ORDER BY "user_count" DESC;

User: How many users viewed category screen
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "download_category_events";

User: How many users viewed language screen
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "download_language_events" WHERE "screen_name" = 'language_list_screen';

User: Which languages had the most downloads?
SQL: SELECT "autonym_script", COUNT(DISTINCT "profile_id") AS "download_count" FROM "download_language_events" GROUP BY "autonym_script" ORDER BY "download_count" DESC;

User: Breakdown of users by device OS
SQL: SELECT "device_os", COUNT(DISTINCT "profile_id") AS "user_count" FROM "app_lifecycle_events" GROUP BY "device_os" ORDER BY "user_count" DESC;

User: How many users on Android downloaded a category?
SQL: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a JOIN "download_category_events" c ON a."profile_id" = c."profile_id" WHERE a."device_os" = 'Android' AND c."event_type" = 'downloaded';

User: How many users answered yes to the shared phone question?
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "onboarding_events" WHERE "question_id" ILIKE '%shared phone%' AND "answer_id" = 'yes';

User: How many users answered yes to is this device shared and how many said no?
SQL: SELECT "answer_id" AS "response", COUNT(DISTINCT "profile_id") AS "user_count" FROM "onboarding_events" WHERE "question_id" ILIKE '%shared phone%' GROUP BY "answer_id" ORDER BY "user_count" DESC;

User: How many users are healthcare professionals yes and no breakdown?
SQL: SELECT "answer_id" AS "response", COUNT(DISTINCT "profile_id") AS "user_count" FROM "onboarding_events" WHERE "question_id" ILIKE '%healthcare professional%' GROUP BY "answer_id" ORDER BY "user_count" DESC;

User: Breakdown of onboarding survey answers for shared device question
SQL: SELECT "answer_id" AS "response", COUNT(DISTINCT "profile_id") AS "user_count" FROM "onboarding_events" WHERE "question_id" ILIKE '%shared phone%' GROUP BY "answer_id" ORDER BY "user_count" DESC;

User: How many users accepted terms and conditions?
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "terms_conditions_events" WHERE "event_type" = 'accepted';

User: How many users installed the app but dropped off before category screen?
SQL: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a WHERE a."profile_id" NOT IN (SELECT DISTINCT "profile_id" FROM "download_category_events");
"""


# ============================================================
# 5. SQL SANITIZER & GUARDS
# ============================================================

def extract_clean_sql(raw_text: str) -> str:
    """Robustly extracts ONLY the SQL query from LLM output, eliminating all markdown, explanations, and thoughts."""
    if not raw_text:
        return ""

    text = raw_text.strip()

    # 1. First preference: extract content inside ```sql ... ``` or ``` ... ``` code blocks
    code_blocks = re.findall(r"```(?:sql)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    sql_candidate = ""
    for block in code_blocks:
        if re.search(r"\bSELECT\b", block, flags=re.IGNORECASE):
            sql_candidate = block.strip()
            break

    # 2. If no code block contained SELECT, try extracting from the entire text
    if not sql_candidate:
        sel_match = re.search(r"\b(SELECT\b[\s\S]+)", text, flags=re.IGNORECASE)
        if sel_match:
            sql_candidate = sel_match.group(1).strip()
        else:
            return ""

    # 3. Cut off at markdown headings, thoughts, or explanation sections
    cutoff_patterns = [
        r"\n\s*#{1,6}\s+",
        r"\n\s*(?:###|##|#)\s*",
        r"\n\s*(?:Explanation|Notes?|Here is|This query|Output|Result|Let me know)\b",
        r"\n\s*\*\*(?:Explanation|Notes?)\*\*",
        r"\n\s*---",
    ]
    for pattern in cutoff_patterns:
        m = re.search(pattern, sql_candidate, flags=re.IGNORECASE)
        if m:
            sql_candidate = sql_candidate[:m.start()].strip()

    # 4. Strip trailing SQL line-comments (-- ...) that LLMs sometimes append after the query
    #    These are not inside string literals at end-of-query level and cause parse errors.
    sql_candidate = re.sub(r"\s*--[^\n]*$", "", sql_candidate, flags=re.MULTILINE).strip()

    # 5. If there is a semicolon, take everything up to the first semicolon
    if ";" in sql_candidate:
        parts = sql_candidate.split(";")
        sql_candidate = parts[0].strip()

    # 6. Clean up any trailing backticks or whitespace
    sql_candidate = re.sub(r"```.*$", "", sql_candidate, flags=re.MULTILINE).strip()
    return sql_candidate.rstrip(";").strip()


def sanitize_generated_sql(sql: str) -> str:
    """Sanitizes LLM-generated SQL against known false assumptions and redirects."""
    if not sql:
        return ""

    # Strip any markdown headings, trailing explanation lines, or SQL comments
    lines = []
    for line in sql.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("```"):
            continue
        if re.match(r"^(?:###|##|#|\*\*|Explanation|Notes?|Here is|This query|Output|Result|Let me know)\b", stripped, re.IGNORECASE):
            break
        # Strip trailing SQL line comments (-- text) from each line to prevent parse errors
        line_clean = re.sub(r"\s*--.*$", "", line)
        lines.append(line_clean)
    sql = "\n".join(lines).strip()
    # Remove any completely blank lines left after comment stripping
    sql = "\n".join(l for l in sql.split("\n") if l.strip())

    # 0. Fix year hallucinations (database contains data strictly for September 2026)
    sql = re.sub(r"'(?:202[0-5]|201\d)-0?9-", "'2026-09-", sql)
    sql = re.sub(r"\b(?:202[0-5]|201\d)-0?9-(\d{1,2})\b", r"2026-09-\1", sql)

    # 1. If querying interaction_events for category or language screens, redirect to v_all_events
    if "interaction_events" in sql:
        if any(k in sql.lower() for k in ["category", "language"]):
            sql = re.sub(r'["\']?interaction_events["\']?', '"v_all_events"', sql)

    # 2. Remove erroneous event_type = 'viewed' from category/language tables or v_all_events with category/language screen
    if any(k in sql for k in ["download_category_events", "category_list_screen", "download_language_events", "language_list_screen"]):
        sql = re.sub(r"""\s+AND\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"""\s+WHERE\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]\s+AND\s+""", " WHERE ", sql, flags=re.IGNORECASE)
        sql = re.sub(r"""WHERE\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)

    # 3. If querying app_lifecycle_events for general user counts / active users by date, redirect to v_all_events
    lifecycle_cols = ["device_os", "device_model", "app_version", "app_id", "location_lat", "location_long", "installed"]
    has_lifecycle_cols = any(col in sql.lower() for col in lifecycle_cols)
    has_date_filter = any(w in sql.lower() for w in ["event_time", "date(", "date '", "2026-09-", "september"])
    if not has_lifecycle_cols and has_date_filter and "app_lifecycle_events" in sql:
        sql = re.sub(r'["\']?app_lifecycle_events["\']?', '"v_all_events"', sql)

    # 4. Fix bad onboarding_events question_id ILIKE patterns
    sql = fix_onboarding_ilike(sql)

    return sql.strip()


# ============================================================
# 6. VANNA AI AGENT PIPELINE
# ============================================================

class DynamicSchemaEnhancer(LlmContextEnhancer):
    """Feeds dynamic schema and relationship rules into Vanna Agent."""
    async def enhance_system_prompt(self, system_prompt, user_message, user):
        schema = _get_schema_context()
        return (
            system_prompt
            + "\n\n## Database schema (PostgreSQL production data)\n"
            + schema
            + "\n\n"
            + TABLE_NOTES
        )

    async def enhance_user_messages(self, messages, user):
        return messages


class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context):
        return User(id="local-user", username="local-user", group_memberships=["user"])


class TrackingPostgresRunner(PostgresRunner):
    """Postgres runner that preserves the last executed SQL query and DataFrame for display."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_sql = None
        self.last_df = None

    async def run_sql(self, args, context):
        if hasattr(args, "sql") and args.sql:
            args.sql = sanitize_generated_sql(args.sql)
        elif isinstance(args, dict) and "sql" in args:
            args["sql"] = sanitize_generated_sql(args["sql"])
        self.last_sql = getattr(args, "sql", None) if not isinstance(args, dict) else args.get("sql")
        df = await super().run_sql(args, context)
        self.last_df = df
        return df


class VannaResultHook(LifecycleHook):
    """Lifecycle hook that captures SQL result rows from ToolResult metadata."""
    def __init__(self):
        self.rows = []

    async def after_tool(self, result):
        if getattr(result, "success", False) and getattr(result, "metadata", None):
            res = result.metadata.get("results")
            if res and isinstance(res, list):
                self.rows = res
        return None


@st.cache_resource
def get_vanna_agent_bundle():
    """Initializes and caches the official Vanna Agent orchestrator."""
    runner = TrackingPostgresRunner(**PG_KWARGS)
    sql_tool = RunSqlTool(sql_runner=runner)
    tools = ToolRegistry()
    tools.register_local_tool(sql_tool, access_groups=["user"])
    user_res = SimpleUserResolver()
    mem = DemoAgentMemory()
    enhancer = DynamicSchemaEnhancer()
    hook = VannaResultHook()

    agent_inst = Agent(
        llm_service=OllamaLlmService(
            model=os.getenv("OLLAMA_MODEL", "qwen3:8b"),
            host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
            temperature=0.1,
        ),
        tool_registry=tools,
        user_resolver=user_res,
        agent_memory=mem,
        llm_context_enhancer=enhancer,
        lifecycle_hooks=[hook],
    )
    return agent_inst, runner, hook


def ask_vanna_agent(question: str):
    """Runs a query through the full Vanna Agent framework.
    Returns (answer_text, rows).
    """
    agent_inst, runner, hook = get_vanna_agent_bundle()
    hook.rows = []
    runner.last_sql = None
    runner.last_df = None

    async def _run():
        ctx = RequestContext(metadata={"user_id": "local-user"})
        texts = []
        async for comp in agent_inst.send_message(ctx, question):
            if hasattr(comp, "simple_component") and comp.simple_component and hasattr(comp.simple_component, "text"):
                texts.append(comp.simple_component.text)
        return texts

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        texts = loop.run_until_complete(_run())
    finally:
        loop.close()

    answer_text = texts[-1] if texts else "No response returned from Vanna agent."
    if runner.last_sql and "```sql" not in answer_text:
        answer_text = f"{answer_text}\n\n```sql\n{runner.last_sql.strip()}\n```"

    rows = []
    if runner.last_df is not None and not runner.last_df.empty:
        rows = runner.last_df.to_dict(orient="records")
    elif hook.rows:
        rows = hook.rows
    elif runner.last_sql:
        try:
            conn = psycopg2.connect(**PG_KWARGS)
            cur = conn.cursor()
            cur.execute(runner.last_sql)
            if cur.description:
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.close()
            conn.close()
        except Exception:
            pass

    return answer_text, rows


# ============================================================
# 7. DIRECT QWEN SQL PIPELINE
# ============================================================

def _call_qwen(messages: list, max_tokens: int = 500) -> str:
    """Call Qwen via Ollama /api/chat with think=False. Returns text content."""
    host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    model = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    payload = json.dumps({
        "model": model,
        "messages": messages,
        "stream": False,
        "think": False,
        "options": {"temperature": 0.1, "num_predict": max_tokens},
    }).encode()
    req = urllib.request.Request(
        f"{host}/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode())
    return body.get("message", {}).get("content", "").strip()


def ask_qwen_sql(question: str):
    """3-step pipeline: question → SQL → execute → explain.
    Returns (answer_text, rows).
    """
    schema = _get_schema_context()

    # ── Step 1: Generate SQL ──────────────────────────────────
    sql_raw = _call_qwen([
        {
            "role": "system",
            "content": (
                "You are a PostgreSQL expert for the Maternity Foundation Safe Delivery analytics database.\n"
                "Given a user question (tolerate minor user typos like 'onbaording' for onboarding, 'inversiom' for in version, 'inversion' for 'in version'):\n"
                "Return ONLY a valid PostgreSQL SELECT query inside a ```sql code block.\n"
                "Do NOT include ANY explanation, comments, or extra text — not inside nor outside the code block.\n"
                "Do NOT append SQL line comments (-- ...) to the query.\n\n"
                f"Database schema:\n{schema}\n\n"
                f"{TABLE_NOTES}"
            ),
        },
        {"role": "user", "content": question},
    ], max_tokens=800)

    sql = extract_clean_sql(sql_raw)
    if not sql:
        return ("I couldn't generate a valid SQL query for that question. Try rephrasing it.", [])
    sql = sanitize_generated_sql(sql)

    # ── Step 2: Execute SQL ───────────────────────────────────
    try:
        conn = psycopg2.connect(**PG_KWARGS)
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description:
            col_names = [d[0] for d in cur.description]
            fetched = cur.fetchall()
            rows = [dict(zip(col_names, row)) for row in fetched]
        else:
            rows = []
        cur.close()
        conn.close()
    except Exception as sql_err:
        # ── Step 2b: SQL failed — ask Qwen to fix it once ────
        try:
            fix_raw = _call_qwen([
                {
                    "role": "system",
                    "content": (
                        "You are a PostgreSQL expert. Fix the SQL error below and return ONLY "
                        "the corrected SQL query inside a ```sql code block. No explanation. No comments. No -- lines.\n\n"
                        f"Database schema:\n{schema}\n{TABLE_NOTES}"
                    ),
                },
                {"role": "user", "content": f"Original question: {question}\n\nFailed SQL:\n{sql}\n\nError: {sql_err}\n\nFixed SQL:"},
            ], max_tokens=800)
            fix_sql = extract_clean_sql(fix_raw)
            if fix_sql:
                sql = sanitize_generated_sql(fix_sql)
                conn = psycopg2.connect(**PG_KWARGS)
                cur = conn.cursor()
                cur.execute(sql)
                if cur.description:
                    col_names = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = [dict(zip(col_names, row)) for row in fetched]
                else:
                    rows = []
                cur.close()
                conn.close()
            else:
                return (f"SQL error: {sql_err}", [])
        except Exception as fix_err:
            return (f"SQL error: {sql_err}\nFix attempt also failed: {fix_err}", [])

    # ── Step 3a: Zero-row smart retry for onboarding_events ──
    if not rows and "onboarding_events" in sql and "question_id" in sql.lower():
        # The LLM likely generated a bad ILIKE pattern. Retry with actual question list.
        actual_qs = "\n".join(f"  - {q}" for q in ONBOARDING_QUESTIONS)
        retry_raw = _call_qwen([
            {
                "role": "system",
                "content": (
                    "You are a PostgreSQL expert. The previous SQL returned 0 rows because the "
                    "question_id ILIKE pattern did not match any database values.\n"
                    "Here are the EXACT question_id values stored in the onboarding_events table:\n"
                    f"{actual_qs}\n\n"
                    "Rewrite the SQL using the correct question_id value (use ILIKE with a substring "
                    "that is guaranteed to match one of the above). "
                    "Return ONLY the corrected SQL inside a ```sql code block. No explanation."
                ),
            },
            {"role": "user", "content": f"Original question: {question}\n\nFailed SQL (returned 0 rows):\n{sql}\n\nFixed SQL:"},
        ], max_tokens=800)
        retry_sql = extract_clean_sql(retry_raw)
        if retry_sql:
            retry_sql = sanitize_generated_sql(retry_sql)
            try:
                conn = psycopg2.connect(**PG_KWARGS)
                cur = conn.cursor()
                cur.execute(retry_sql)
                if cur.description:
                    col_names = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    rows = [dict(zip(col_names, row)) for row in fetched]
                cur.close()
                conn.close()
                if rows:
                    sql = retry_sql  # use the corrected SQL for explanation
            except Exception:
                pass  # fall through to "no rows" explanation

    # ── Step 3b: Explain results in plain English ─────────────
    if not rows:
        explanation = _call_qwen([
            {"role": "system", "content": "You are a data analyst. Answer concisely in 1-2 sentences."},
            {"role": "user", "content": f"This SQL query returned no rows:\n\n{sql}\n\nThe question was: {question}\nWhat does this likely mean?"},
        ], max_tokens=120)
        return (explanation, [])

    rows_preview = json.dumps(rows[:20], default=str, indent=2)
    explanation = _call_qwen([
        {
            "role": "system",
            "content": (
                "You are a data analyst for Maternity Foundation Safe Delivery app. "
                "Given a user question and query results, write a clear, concise answer in 2-4 sentences. "
                "Mention key numbers. Do not repeat the SQL."
            ),
        },
        {
            "role": "user",
            "content": f"Question: {question}\n\nSQL used:\n{sql}\n\nResults ({len(rows)} rows, showing first 20):\n{rows_preview}\n\nAnswer:",
        },
    ], max_tokens=200)

    full_answer = f"{explanation}\n\n```sql\n{sql}\n```"
    return (full_answer, rows)


# Keep these for backwards compatibility / journey report path
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ============================================================
# 6. CHART HELPERS
# ============================================================
def clean_column_name(col):
    return str(col).replace("_", " ").replace("-", " ").title()


def looks_like_date_column(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if series.dtype == object or pd.api.types.is_string_dtype(series):
        sample = series.dropna().head(10).astype(str)
        if sample.empty:
            return False
        date_pattern = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}")
        if sample.str.match(date_pattern).mean() >= 0.7:
            return True
        try:
            return pd.to_datetime(sample, format="mixed", errors="coerce").notna().mean() >= 0.7
        except Exception:
            return False
    return False


def style_fig(fig, height=440):
    fig.update_layout(
        template="plotly_white",
        height=height,
        font=dict(family="Montserrat, Inter, -apple-system, sans-serif", size=13, color="#1D1D1B"),
        title_font=dict(size=16, color="#1D1D1B", family="Montserrat, Inter, sans-serif"),
        margin=dict(l=40, r=40, t=60, b=50),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
        bargap=0.25,
        bargroupgap=0.1,
        xaxis=dict(showgrid=False, showline=True, linecolor="#E0E0E0", tickfont=dict(size=12)),
        yaxis=dict(showgrid=True, gridcolor="#F2F2F2", zeroline=False, tickfont=dict(size=12)),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
    )
    return fig


def get_total_app_users_count() -> int:
    """Helper to fetch total distinct users in the app for single-metric context."""
    try:
        conn = psycopg2.connect(**PG_KWARGS)
        cur = conn.cursor()
        cur.execute('SELECT COUNT(DISTINCT "profile_id") FROM "v_all_events";')
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row and row[0] else 15
    except Exception:
        return 15


def analyze_dataframe_axes(df: pd.DataFrame):
    """Accurately analyzes dataframe to identify Dimensions (X-axis candidates)
    and Measures/Metrics (Y-axis candidates) like PowerBI.
    """
    chart_df = df.copy()

    # 1. Normalize types (convert Decimals to float, object numbers to float)
    for col in chart_df.columns:
        chart_df[col] = chart_df[col].apply(lambda v: float(v) if isinstance(v, Decimal) else v)
        if chart_df[col].dtype == object or pd.api.types.is_string_dtype(chart_df[col]):
            converted_num = pd.to_numeric(chart_df[col], errors="coerce")
            if pd.Series(converted_num).notna().mean() >= 0.8:
                chart_df[col] = converted_num

    numeric_cols = chart_df.select_dtypes(include=["number"]).columns.tolist()
    all_cols = chart_df.columns.tolist()
    non_numeric = [c for c in all_cols if c not in numeric_cols]

    # Detect date columns
    date_cols = [c for c in non_numeric if looks_like_date_column(chart_df[c])]
    cat_cols = [c for c in non_numeric if c not in date_cols]

    # Keyword weights for identifying Measures (Y-axis)
    measure_keywords = (
        "count", "total", "users", "sum", "avg", "downloads", "duration",
        "score", "rate", "num", "amount", "pct", "percentage", "attempts"
    )
    dimension_keywords = (
        "id", "name", "screen", "date", "time", "day", "language", "category",
        "os", "model", "version", "event", "type", "question", "answer", "status", "script"
    )

    def measure_score(col):
        name_lower = col.lower()
        score = 10
        if any(k in name_lower for k in measure_keywords):
            score += 50
        if any(k in name_lower for k in dimension_keywords):
            score -= 30
        return score

    sorted_measures = sorted(numeric_cols, key=measure_score, reverse=True)

    # Low-cardinality numeric IDs that can act as dimensions (e.g. language code)
    id_numeric_dims = [
        c for c in numeric_cols
        if (any(k in c.lower() for k in ("id", "code", "year")) or chart_df[c].nunique() <= 15)
        and c not in sorted_measures[:1]
    ]

    dim_candidates = date_cols + cat_cols + id_numeric_dims
    if not dim_candidates and all_cols:
        dim_candidates = [c for c in all_cols if c not in sorted_measures[:1]] or all_cols

    default_x = dim_candidates[0] if dim_candidates else (all_cols[0] if all_cols else None)
    default_y = sorted_measures[0] if sorted_measures else None

    if default_y == default_x and len(numeric_cols) > 1:
        default_y = [c for c in numeric_cols if c != default_x][0]

    return chart_df, default_x, default_y, date_cols, cat_cols, sorted_measures, dim_candidates


def display_chart(df: pd.DataFrame, key_prefix: str = "chart"):
    """PowerBI-grade interactive visualization component.
    Determines dimensions and measures, recommends the optimal chart type,
    and provides an interactive chart switcher & axis customizer.
    """
    if df is None or df.empty:
        st.info("No data returned to plot.")
        return

    chart_df, auto_x, auto_y, date_cols, cat_cols, numeric_cols, dim_candidates = analyze_dataframe_axes(df)
    all_cols = chart_df.columns.tolist()

    # ────────────────────────────────────────────────────────────
    # SCENARIO 1: SINGLE KPI METRIC (1 row, 1 numeric measure)
    # ────────────────────────────────────────────────────────────
    if len(chart_df) == 1 and len(numeric_cols) == 1 and not cat_cols and not date_cols:
        val = chart_df[numeric_cols[0]].iloc[0]
        val_num = float(val) if pd.notna(val) else 0.0
        lbl = clean_column_name(numeric_cols[0])
        total_users = get_total_app_users_count()

        col_kpi, col_gauge = st.columns([1, 1.4])
        with col_kpi:
            st.markdown(f"""
            <div style="background:#FFFFFF; border:1px solid #E5E5E5; border-top:4px solid #A80041; border-radius:6px; padding:22px 20px; box-shadow:0 2px 8px rgba(0,0,0,0.04); margin-bottom:12px;">
                <div style="font-size:11px; font-weight:700; text-transform:uppercase; color:#777777; letter-spacing:0.06em; margin-bottom:6px;">{lbl}</div>
                <div style="font-size:42px; font-weight:800; color:#1D1D1B; font-family:'Montserrat', sans-serif; line-height:1.1;">
                    {int(val_num):,}
                </div>
                <div style="margin-top:10px; font-size:13px; color:#555555;">
                    <span style="background:#EBF7F0; color:#1B7D50; padding:2px 8px; border-radius:12px; font-weight:600; font-size:11px;">Active Cohort</span>
                    &nbsp;·&nbsp; Out of {total_users:,} registered users
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.caption("💡 *Tip: For a multi-bar breakdown chart, ask 'by device OS', 'by date', or 'by language'.*")

        with col_gauge:
            pct = min(100.0, (val_num / total_users * 100)) if total_users > 0 else 0.0
            remainder = max(0, total_users - int(val_num))
            fig_kpi = go.Figure(data=[go.Pie(
                labels=[lbl, "Other App Users"],
                values=[int(val_num), remainder],
                hole=0.68,
                marker=dict(colors=["#A80041", "#ECECEC"]),
                textinfo="percent",
                hoverinfo="label+value+percent",
                sort=False,
            )])
            fig_kpi.update_layout(
                annotations=[dict(
                    text=f"<b>{pct:.1f}%</b><br><span style='font-size:11px;color:#777;'>of total users</span>",
                    x=0.5, y=0.5, font_size=18, showarrow=False, font_family="Montserrat, sans-serif"
                )],
                showlegend=True,
                height=220,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5, font=dict(size=11)),
            )
            st.plotly_chart(fig_kpi, use_container_width=True)
        return

    # ────────────────────────────────────────────────────────────
    # SCENARIO 2: 1 ROW WITH MULTIPLE MEASURES (e.g. 3 KPI counts)
    # ────────────────────────────────────────────────────────────
    if len(chart_df) == 1 and len(numeric_cols) > 1 and not cat_cols:
        trans_data = []
        for col in numeric_cols:
            val = chart_df[col].iloc[0]
            trans_data.append({"Metric": clean_column_name(col), "Value": float(val) if pd.notna(val) else 0.0})
        trans_df = pd.DataFrame(trans_data)
        fig = px.bar(
            trans_df,
            x="Metric",
            y="Value",
            text="Value",
            color="Metric",
            color_discrete_sequence=PALETTE,
            title="Comparison of Metrics",
        )
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        style_fig(fig, height=380)
        st.plotly_chart(fig, use_container_width=True)
        return

    # ────────────────────────────────────────────────────────────
    # SCENARIO 3: MULTIPLE ROWS (TABULAR & ANALYTICAL BREAKDOWNS)
    # ────────────────────────────────────────────────────────────
    if not numeric_cols and cat_cols:
        primary_cat = cat_cols[0]
        grouped = chart_df[primary_cat].value_counts().reset_index()
        grouped.columns = [primary_cat, "User Count"]
        chart_df = grouped
        auto_x = primary_cat
        auto_y = "User Count"
        numeric_cols = ["User Count"]
        dim_candidates = [primary_cat]

    if not numeric_cols:
        st.dataframe(chart_df, use_container_width=True)
        return

    # Auto-recommend optimal chart type
    recommended_type = "Vertical Bar"
    if date_cols and auto_x in date_cols:
        recommended_type = "Line Trend"
    elif auto_x and chart_df[auto_x].nunique() > 7:
        recommended_type = "Horizontal Bar"
    elif auto_x and 2 <= chart_df[auto_x].nunique() <= 6:
        recommended_type = "Donut / Pie"

    # ── PowerBI Controls Toolbar ──
    ctrl_col1, ctrl_col2 = st.columns([2.5, 1])
    with ctrl_col1:
        chart_type_options = ["Vertical Bar", "Horizontal Bar", "Donut / Pie", "Line Trend", "Area Chart"]
        default_idx = chart_type_options.index(recommended_type) if recommended_type in chart_type_options else 0
        selected_chart_type = st.radio(
            "Visual Type",
            chart_type_options,
            index=default_idx,
            horizontal=True,
            key=f"{key_prefix}_chart_type",
            label_visibility="collapsed",
        )

    with ctrl_col2:
        with st.popover("🛠️ Customize Axes"):
            st.markdown("**PowerBI Axis & Encoding Designer**")
            custom_x = st.selectbox(
                "Dimension (X-Axis)",
                options=all_cols,
                index=all_cols.index(auto_x) if auto_x in all_cols else 0,
                key=f"{key_prefix}_sel_x",
            )
            custom_y = st.selectbox(
                "Measure (Y-Axis)",
                options=numeric_cols,
                index=numeric_cols.index(auto_y) if auto_y in numeric_cols else 0,
                key=f"{key_prefix}_sel_y",
            )
            other_cats = ["None"] + [c for c in all_cols if c not in [custom_x, custom_y]]
            custom_color = st.selectbox(
                "Breakdown / Legend (Color)",
                options=other_cats,
                index=0,
                key=f"{key_prefix}_sel_color",
            )
            sort_order = st.radio(
                "Sort Order",
                ["Descending by Value", "Ascending by Value", "Natural / Chronological"],
                index=0,
                key=f"{key_prefix}_sel_sort",
            )

    # Apply axis selections
    chosen_x = st.session_state.get(f"{key_prefix}_sel_x", auto_x or all_cols[0])
    chosen_y = st.session_state.get(f"{key_prefix}_sel_y", auto_y or numeric_cols[0])
    chosen_color_raw = st.session_state.get(f"{key_prefix}_sel_color", "None")
    chosen_color = None if chosen_color_raw == "None" else chosen_color_raw
    chosen_sort = st.session_state.get(f"{key_prefix}_sel_sort", "Descending by Value")

    # Sorting
    plot_df = chart_df.copy()
    if chosen_sort == "Descending by Value" and chosen_y in plot_df.columns:
        plot_df = plot_df.sort_values(by=chosen_y, ascending=False)
    elif chosen_sort == "Ascending by Value" and chosen_y in plot_df.columns:
        plot_df = plot_df.sort_values(by=chosen_y, ascending=True)
    elif chosen_sort == "Natural / Chronological" and chosen_x in plot_df.columns:
        plot_df = plot_df.sort_values(by=chosen_x, ascending=True)

    title_str = f"<b>{clean_column_name(chosen_y)}</b> by {clean_column_name(chosen_x)}"
    if chosen_color:
        title_str += f" (split by {clean_column_name(chosen_color)})"

    # Render selected chart
    fig = None
    if selected_chart_type == "Vertical Bar":
        fig = px.bar(
            plot_df,
            x=chosen_x,
            y=chosen_y,
            color=chosen_color or chosen_x,
            text=chosen_y,
            color_discrete_sequence=PALETTE,
            title=title_str,
        )
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")

    elif selected_chart_type == "Horizontal Bar":
        hbar_df = plot_df.sort_values(by=chosen_y, ascending=True)
        fig = px.bar(
            hbar_df,
            x=chosen_y,
            y=chosen_x,
            orientation="h",
            color=chosen_color or chosen_x,
            text=chosen_y,
            color_discrete_sequence=PALETTE,
            title=title_str,
        )
        fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig.update_layout(yaxis=dict(autorange="reversed"))

    elif selected_chart_type == "Donut / Pie":
        fig = px.pie(
            plot_df,
            names=chosen_x,
            values=chosen_y,
            hole=0.45,
            color_discrete_sequence=PALETTE,
            title=title_str,
        )
        fig.update_traces(textinfo="label+percent", hoverinfo="label+value+percent")

    elif selected_chart_type == "Line Trend":
        fig = px.line(
            plot_df,
            x=chosen_x,
            y=chosen_y,
            color=chosen_color,
            markers=True,
            color_discrete_sequence=PALETTE,
            title=title_str,
        )
        fig.update_traces(line=dict(width=3))

    elif selected_chart_type == "Area Chart":
        fig = px.area(
            plot_df,
            x=chosen_x,
            y=chosen_y,
            color=chosen_color,
            color_discrete_sequence=PALETTE,
            title=title_str,
        )

    if fig is not None:
        style_fig(fig, height=440)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# 7. CSV UPLOAD & INGESTION HELPER
# ============================================================
def ingest_uploaded_csvs(uploaded_files: list) -> dict:
    from load_csvs_to_postgres import ingest_csv_directory

    PG_ADMIN_USER = os.getenv("POSTGRES_ADMIN_USER", "developer01")
    PG_ADMIN_PASSWORD = os.getenv("POSTGRES_ADMIN_PASSWORD")
    PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
    PG_PORT = os.getenv("POSTGRES_PORT", "5432")
    PG_DATABASE = os.getenv("POSTGRES_DATABASE", "vanna_demo")

    if not PG_ADMIN_PASSWORD:
        return {"status": "error", "message": "POSTGRES_ADMIN_PASSWORD not set in .env"}

    csv_folder = Path("csv_data")
    csv_folder.mkdir(exist_ok=True)

    saved_names = []
    for f in uploaded_files:
        dest = csv_folder / f.name
        content = f.getvalue() if hasattr(f, "getvalue") else f.read()
        dest.write_bytes(content)
        saved_names.append(f.name)

    engine = create_engine(
        f"postgresql+psycopg2://{PG_ADMIN_USER}:{PG_ADMIN_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )
    result = ingest_csv_directory(str(csv_folder), engine=engine)
    result["saved_files"] = saved_names
    return result


# ============================================================
# 8. MATERNITY FOUNDATION THEME  — exact brand: crimson + white + charcoal
# ============================================================
MF_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap');

/* ── Design tokens ── */
:root {
    --mf-red:       #A80041;
    --mf-red-dark:  #870034;
    --mf-red-light: #F2E6EC;
    --mf-white:     #FFFFFF;
    --mf-off:       #F7F7F7;
    --mf-border:    #E0E0E0;
    --mf-ink:       #1D1D1B;
    --mf-ink2:      #444444;
    --mf-muted:     #888888;
    --mf-good:      #1E7A4B;
    --mf-good-bg:   #E8F5EE;
    --mf-bad:       #A80041;
    --mf-bad-bg:    #F2E6EC;
    --mf-warn:      #B56000;
    --mf-warn-bg:   #FEF3DC;
    --mf-shadow:    0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.06);
}

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Montserrat', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif !important;
    color: var(--mf-ink) !important;
    -webkit-font-smoothing: antialiased !important;
}
.stApp { background: var(--mf-white) !important; }

/* ── Hide default Streamlit header ── */
header[data-testid="stHeader"] {
    background: var(--mf-white) !important;
    border-bottom: 1px solid var(--mf-border) !important;
    box-shadow: none !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: var(--mf-white) !important;
    border-right: 1px solid var(--mf-border) !important;
}
section[data-testid="stSidebar"] * { color: var(--mf-ink) !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 700 !important;
    color: var(--mf-ink) !important;
}
/* Sidebar sample question buttons */
section[data-testid="stSidebar"] .stButton > button {
    background: var(--mf-off) !important;
    border: 1px solid var(--mf-border) !important;
    color: var(--mf-ink2) !important;
    border-radius: 4px !important;
    font-size: 12px !important;
    text-align: left !important;
    padding: 9px 12px !important;
    line-height: 1.4 !important;
    width: 100% !important;
    transition: background 0.15s, border-color 0.15s !important;
    font-weight: 500 !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: var(--mf-red-light) !important;
    border-color: var(--mf-red) !important;
    color: var(--mf-red) !important;
}
section[data-testid="stSidebar"] hr {
    border-color: var(--mf-border) !important;
    margin: 12px 0 !important;
}

/* ── Page brand bar ── */
.mf-topbar {
    background: var(--mf-white);
    border-bottom: 3px solid var(--mf-red);
    padding: 18px 0 14px;
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    gap: 14px;
}
.mf-topbar-logo {
    font-family: 'Montserrat', sans-serif;
    font-weight: 800;
    font-size: 20px;
    color: var(--mf-ink);
    letter-spacing: -0.02em;
    line-height: 1.1;
}
.mf-topbar-logo span { color: var(--mf-red); }
.mf-topbar-sub {
    font-size: 12px;
    color: var(--mf-muted);
    font-weight: 500;
    letter-spacing: 0.02em;
    margin-top: 3px;
}
.mf-topbar-badge {
    margin-left: auto;
    background: var(--mf-red);
    color: #fff;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 5px 14px;
    border-radius: 2px;
}

/* ── Tabs ── */
[data-testid="stTabs"] [role="tablist"] {
    border-bottom: 2px solid var(--mf-border) !important;
    background: transparent !important;
    padding: 0 !important;
    gap: 0 !important;
}
[data-testid="stTabs"] [role="tab"] {
    border-radius: 0 !important;
    border: none !important;
    border-bottom: 3px solid transparent !important;
    padding: 10px 22px !important;
    margin-bottom: -2px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    color: var(--mf-muted) !important;
    background: transparent !important;
    letter-spacing: 0.02em !important;
    transition: color 0.15s, border-color 0.15s !important;
    text-transform: uppercase !important;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: var(--mf-red) !important;
    border-bottom: 3px solid var(--mf-red) !important;
    background: transparent !important;
}
[data-testid="stTabs"] [role="tab"]:hover:not([aria-selected="true"]) {
    color: var(--mf-ink) !important;
    border-bottom-color: var(--mf-border) !important;
}

/* ── Primary button ── */
.stButton > button[kind="primary"],
.stButton > button[kind="primary"] *,
button[kind="primary"],
button[kind="primary"] * {
    background: var(--mf-red) !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: none !important;
    border-radius: 3px !important;
    font-weight: 700 !important;
    font-size: 13px !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    transition: background 0.15s !important;
    box-shadow: none !important;
}
.stButton > button[kind="primary"]:hover,
button[kind="primary"]:hover {
    background: var(--mf-red-dark) !important;
}

/* ── Secondary button ── */
.stButton > button:not([kind="primary"]) {
    border: 1.5px solid var(--mf-border) !important;
    border-radius: 3px !important;
    background: var(--mf-white) !important;
    color: var(--mf-ink) !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    padding: 8px 18px !important;
    transition: border-color 0.15s, color 0.15s !important;
}
.stButton > button:not([kind="primary"]):hover {
    border-color: var(--mf-red) !important;
    color: var(--mf-red) !important;
}

/* ── Download button ── */
[data-testid="stDownloadButton"] button,
[data-testid="stDownloadButton"] button * {
    background: var(--mf-red) !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: none !important;
    border-radius: 3px !important;
    font-weight: 700 !important;
    font-size: 13px !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    padding: 10px 22px !important;
}
[data-testid="stDownloadButton"] button:hover {
    background: var(--mf-red-dark) !important;
}

/* ── Form inputs, Selectboxes & Labels (Dark Mode Immune) ── */
[data-testid="stWidgetLabel"] label,
[data-testid="stWidgetLabel"] p,
[data-testid="stWidgetLabel"] span {
    color: #1D1D1B !important;
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}

[data-testid="stTextInput"] > div,
[data-testid="stTextInput"] input,
div[data-baseweb="input"],
div[data-baseweb="input"] input {
    background-color: #FFFFFF !important;
    color: #1D1D1B !important;
    border: 1.5px solid #D0D0D0 !important;
    border-radius: 4px !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 13.5px !important;
}
[data-testid="stTextInput"] input::placeholder,
div[data-baseweb="input"] input::placeholder {
    color: #888888 !important;
    opacity: 1 !important;
    -webkit-text-fill-color: #888888 !important;
}
[data-testid="stTextInput"] input:focus,
div[data-baseweb="input"] input:focus {
    border-color: var(--mf-red) !important;
    box-shadow: 0 0 0 2px rgba(168,0,65,0.12) !important;
}

/* Selectbox */
[data-testid="stSelectbox"] > div > div,
div[data-baseweb="select"] > div {
    background-color: #FFFFFF !important;
    border: 1.5px solid #D0D0D0 !important;
    border-radius: 4px !important;
    color: #1D1D1B !important;
}
div[data-baseweb="select"] * {
    color: #1D1D1B !important;
    fill: #1D1D1B !important;
}
div[data-baseweb="select"] svg {
    fill: #1D1D1B !important;
    color: #1D1D1B !important;
}
div[data-baseweb="popover"],
div[data-baseweb="menu"],
ul[role="listbox"] {
    background-color: #FFFFFF !important;
    border: 1px solid #D0D0D0 !important;
    box-shadow: 0 4px 16px rgba(0,0,0,0.14) !important;
}
li[role="option"] {
    background-color: #FFFFFF !important;
    color: #1D1D1B !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 13px !important;
    padding: 9px 14px !important;
}
li[role="option"]:hover,
li[role="option"][aria-selected="true"] {
    background-color: #F9E6EE !important;
    color: #A80041 !important;
    font-weight: 600 !important;
}

/* ── Metric cards ── */
[data-testid="stMetric"] {
    background: var(--mf-white) !important;
    border: 1px solid var(--mf-border) !important;
    border-top: 3px solid var(--mf-red) !important;
    border-radius: 3px !important;
    padding: 18px 20px !important;
    box-shadow: var(--mf-shadow) !important;
}
[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.07em !important;
    color: var(--mf-muted) !important;
}
[data-testid="stMetricValue"] {
    font-family: 'Montserrat', sans-serif !important;
    font-size: 28px !important;
    font-weight: 800 !important;
    color: var(--mf-ink) !important;
    line-height: 1.2 !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--mf-border) !important;
    border-radius: 3px !important;
    box-shadow: var(--mf-shadow) !important;
}

/* ── Headings & Markdown Text Contrast ── */
[data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3,
[data-testid="stMarkdownContainer"] h4,
[data-testid="stMarkdownContainer"] strong,
[data-testid="stMarkdownContainer"] b {
    color: #1D1D1B !important;
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 700 !important;
}
[data-testid="stMarkdownContainer"] p,
[data-testid="stMarkdownContainer"] span,
[data-testid="stMarkdownContainer"] li {
    color: #333333 !important;
    font-family: 'Montserrat', sans-serif !important;
}
[data-testid="stMarkdownContainer"] h3 {
    font-size: 18px !important;
}
[data-testid="stMarkdownContainer"] h4 {
    font-size: 15px !important;
}

/* ── Alerts ── */
[data-testid="stAlert"] { border-radius: 3px !important; }
[data-testid="stAlert"][data-baseweb="notification"] { border-left: 4px solid var(--mf-red) !important; }

/* ── Caption / small text ── */
[data-testid="stCaptionContainer"] p,
[data-testid="stCaptionContainer"],
small,
.stCaption {
    color: #666666 !important;
    font-size: 12px !important;
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    border: 2px dashed var(--mf-border) !important;
    border-radius: 3px !important;
    background: var(--mf-off) !important;
    padding: 16px !important;
    transition: border-color 0.15s !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: var(--mf-red) !important;
}
[data-testid="stFileUploader"] label { font-weight: 600 !important; }

/* ── Code blocks ── */
code, pre {
    background: var(--mf-off) !important;
    border-radius: 3px !important;
    color: var(--mf-ink) !important;
    border: 1px solid var(--mf-border) !important;
}

/* ── Divider ── */
hr { border-color: var(--mf-border) !important; margin: 20px 0 !important; }

/* ── Checkbox ── */
[data-testid="stCheckbox"] label span { color: var(--mf-ink2) !important; }

/* ── Section heading component ── */
.mf-section-heading {
    font-family: 'Montserrat', sans-serif;
    font-size: 13px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: var(--mf-muted);
    margin: 24px 0 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid var(--mf-border);
}

/* ── KPI strip ── */
.mf-kpi-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin-bottom: 24px;
}
.mf-kpi {
    background: var(--mf-white);
    border: 1px solid var(--mf-border);
    border-top: 3px solid var(--mf-red);
    border-radius: 3px;
    padding: 18px 20px;
    box-shadow: var(--mf-shadow);
}
.mf-kpi small {
    display: block;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--mf-muted);
    margin-bottom: 8px;
}
.mf-kpi b {
    display: block;
    font-family: 'Montserrat', sans-serif;
    font-size: 30px;
    font-weight: 800;
    color: var(--mf-ink);
    line-height: 1.1;
}
.mf-kpi em {
    font-style: normal;
    font-size: 12px;
    color: var(--mf-muted);
    margin-top: 4px;
    display: block;
}
.mf-kpi.good  b { color: var(--mf-good); }
.mf-kpi.bad   b { color: var(--mf-red);  }
.mf-kpi.red   b { color: var(--mf-red);  }

/* ── Spinner ── */
.stSpinner > div { border-top-color: var(--mf-red) !important; }
</style>
"""


# ============================================================
# 9. STREAMLIT APP
# ============================================================
st.set_page_config(
    page_title="Maternity Foundation · Analytics Dashboard",
    page_icon="🩺",
    layout="wide",
)

st.markdown(MF_CSS, unsafe_allow_html=True)

# ── Brand top bar ──
st.markdown("""
<div class="mf-topbar">
  <div>
    <div class="mf-topbar-logo">
      <span>maternity</span> FOUNDATION
    </div>
    <div class="mf-topbar-sub">
      Safe Delivery App &nbsp;·&nbsp; User Analytics &amp; Journey Intelligence
    </div>
  </div>
  <div class="mf-topbar-badge">Analytics Dashboard</div>
</div>
""", unsafe_allow_html=True)

journey_engine = UserJourneyEngine(PG_KWARGS)

# ── Sidebar ──
with st.sidebar:
    st.markdown("### Analytics Controls")
    model_name = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    st.caption(f"Model: `{model_name}`")
    st.caption(f"Database: `{os.getenv('POSTGRES_DATABASE', 'vanna_demo')}`")

    st.divider()
    st.markdown("### Execution Engine")
    engine_choice = st.radio(
        "Text-to-SQL Engine",
        ["Direct Fast Pipeline", "Vanna AI Agent"],
        index=0,
        help="Select whether queries run via Vanna's multi-step Agent with ToolRegistry or the Direct single-shot optimized pipeline.",
    )
    if engine_choice == "Vanna AI Agent":
        st.info("🟢 **Vanna AI Agent active**: Running through official `vanna.Agent` orchestrator (`ToolRegistry` + `RunSqlTool`).")
    else:
        st.caption("⚡ **Direct Pipeline active**: High-speed direct prompt with dynamic schema enhancer & SQL sanitizer.")

    st.divider()
    st.markdown("### Cache")
    cache_count, cache_hits = get_cache_stats()
    st.write(f"Cached questions: **{cache_count}**")
    st.write(f"Cache hits served: **{cache_hits}**")
    if st.button("Clear Cache", key="clear_cache_btn"):
        clear_cache()
        st.success("Cache cleared.")
        st.rerun()

    st.divider()
    st.markdown("### Quick Questions")
    sample_queries = [
        "Create a report of all users of 2026-09-21 and show me their user journey",
        "How many users said yes to the shared phone question?",
        "Show user footfall and drop-off breakpoints",
        "Which languages had the most downloads?",
        "Average duration spent on onboarding screens?",
    ]
    for sq in sample_queries:
        if st.button(sq, key=f"sq_{sq[:22]}"):
            st.session_state["user_query"] = sq
            st.rerun()

# ── Tabs ──
tab_chat, tab_debugger, tab_data = st.tabs([
    "Ask AI & Journey Reports",
    "User Journey & Flow Debugger",
    "Data Management & Health",
])

# ────────────────────────────────────────────────────────────
# TAB 1 — ASK AI
# ────────────────────────────────────────────────────────────
with tab_chat:
    st.markdown("#### Natural Language Analytics & Journey Analysis")
    st.caption("Ask questions about user journeys, drop-offs, footfall, or run natural language SQL queries against the Safe Delivery database.")

    available_dates = journey_engine.get_available_dates()
    preset_query = st.session_state.pop("user_query", "")

    with st.form("ask_form", clear_on_submit=False):
        question = st.text_input(
            "Your question:",
            value=preset_query,
            placeholder="e.g. Create a report of all users of 2026-09-21 and show me their user journey",
        )
        c1, c2 = st.columns([1, 4])
        with c1:
            submitted = st.form_submit_button("Run Query", type="primary")
        with c2:
            force_refresh = st.checkbox("Force refresh (bypass cache)", value=False)

    if (submitted and question) or (preset_query and question):
        is_journey, target_date, target_pid = parse_journey_query(question, available_dates)

        if is_journey:
            with st.spinner(f"Analysing event streams and generating user journey report for {target_date or 'all dates'}…"):
                rep_data = journey_engine.generate_journey_report(date_filter=target_date, profile_id=target_pid)
                md_summary = journey_engine.generate_markdown_summary(rep_data)
                html_report = journey_engine.render_html_report(rep_data)
                st.session_state["last_journey_result"] = {
                    "question": question,
                    "summary": md_summary,
                    "html": html_report,
                    "date": target_date or "all",
                    "rep": rep_data,
                }
                store_cached_answer(question, md_summary, [])
        else:
            cached = None if force_refresh else get_cached_answer(question, engine=engine_choice)
            if cached is not None:
                answer_text, rows = cached
                st.session_state["last_sql_result"] = {
                    "question": question,
                    "answer_text": answer_text,
                    "rows": rows,
                    "from_cache": True,
                    "engine": engine_choice,
                }
            else:
                spinner_msg = (
                    "Running Vanna AI Agent (`ToolRegistry` + `RunSqlTool`)..."
                    if engine_choice == "Vanna AI Agent"
                    else "Asking AI · generating SQL and analysing results..."
                )
                with st.spinner(spinner_msg):
                    try:
                        if engine_choice == "Vanna AI Agent":
                            answer_text, rows = ask_vanna_agent(question)
                        else:
                            answer_text, rows = ask_qwen_sql(question)
                        store_cached_answer(question, answer_text, rows, engine=engine_choice)
                        st.session_state["last_sql_result"] = {
                            "question": question,
                            "answer_text": answer_text,
                            "rows": rows,
                            "from_cache": False,
                            "engine": engine_choice,
                        }
                    except Exception as exc:
                        st.error(f"Error ({engine_choice}): {exc}")

    # Render results
    last_journey = st.session_state.get("last_journey_result")
    if last_journey and last_journey.get("question") == question:
        st.markdown(last_journey["summary"])
        col_dl, _ = st.columns([1, 3])
        with col_dl:
            st.download_button(
                label="Download Full HTML Report",
                data=last_journey["html"],
                file_name=f"safe_delivery_journey_{last_journey['date']}.html",
                mime="text/html",
                type="primary",
            )
        st.markdown("---")
        st.markdown("#### Interactive Onboarding Flow Debugger")
        components.html(last_journey["html"], height=950, scrolling=True)

    elif st.session_state.get("last_sql_result"):
        last_sql = st.session_state["last_sql_result"]
        engine_used = last_sql.get("engine", "Direct Fast Pipeline")
        badge = "🟢 Vanna AI Agent" if engine_used == "Vanna AI Agent" else "⚡ Direct Fast Pipeline"
        cache_note = " · Answered from local cache" if last_sql.get("from_cache") else ""
        st.caption(f"Engine: **{badge}**{cache_note}")

        raw_ans = last_sql.get("answer_text", "")
        sql_match = re.search(r"```sql\s*([\s\S]+?)\s*```", raw_ans)
        explanation_only = re.sub(r"```sql[\s\S]+?```", "", raw_ans).strip()
        extracted_sql = sql_match.group(1).strip() if sql_match else None

        if explanation_only:
            st.markdown(explanation_only)

        rows = last_sql.get("rows", [])
        if rows:
            df = pd.DataFrame(rows)
            st.markdown("---")
            v_tab_chart, v_tab_table, v_tab_sql = st.tabs([
                "📊 PowerBI Visual",
                "📋 Data Table",
                "🔍 Generated SQL",
            ])
            with v_tab_chart:
                display_chart(df, key_prefix="tab1_main")
            with v_tab_table:
                st.dataframe(df, use_container_width=True)
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 Export Results (CSV)",
                    data=csv_bytes,
                    file_name="query_results.csv",
                    mime="text/csv",
                    key="dl_main_csv",
                )
            with v_tab_sql:
                if extracted_sql:
                    st.code(extracted_sql, language="sql")
                else:
                    st.info("No SQL captured for this query.")
        elif extracted_sql:
            with st.expander("🔍 View Generated SQL", expanded=False):
                st.code(extracted_sql, language="sql")


# ────────────────────────────────────────────────────────────
# TAB 2 — USER JOURNEY DEBUGGER
# ────────────────────────────────────────────────────────────
with tab_debugger:
    st.markdown("#### Safe Delivery — User Journey & Funnel Debugger")
    st.caption("Reconstruct end-to-end user footfall, session durations, screen paths, and telemetry anomalies from PostgreSQL event data.")

    available_dates = journey_engine.get_available_dates()
    date_options = ["All days"] + available_dates if available_dates else ["All days"]

    f1, f2, f3 = st.columns(3)
    with f1:
        selected_date = st.selectbox("Date Cohort", options=date_options, index=0)
    with f2:
        selected_version = st.selectbox("App Version", options=["All versions", "4.0.0"], index=1)
    with f3:
        user_pid_filter = st.text_input("Profile ID (optional):", placeholder="e.g. BWT-ZQP-UCW-SRJ-U1")

    chosen_date_arg = "all" if selected_date == "All days" else selected_date
    chosen_ver_arg  = "all" if selected_version == "All versions" else selected_version

    rep_tab = journey_engine.generate_journey_report(
        date_filter=chosen_date_arg,
        version_filter=chosen_ver_arg,
        profile_id=user_pid_filter.strip() if user_pid_filter else None,
    )

    if not rep_tab["users"]:
        st.warning(f"No user events found for date '{selected_date}' and version '{selected_version}'.")
    else:
        html_tab = journey_engine.render_html_report(rep_tab)
        total_u = len(rep_tab["users"])
        comp_u  = len([u for u in rep_tab["users"] if u["outcome"] == "Completed"])
        drop_u  = total_u - comp_u
        conv    = f"{(comp_u / total_u * 100):.1f}%" if total_u else "0%"

        # KPI strip
        st.markdown(f"""
        <div class="mf-kpi-row">
          <div class="mf-kpi">
            <small>Total Users</small>
            <b>{total_u}</b>
            <em>in selected cohort</em>
          </div>
          <div class="mf-kpi good">
            <small>Reached Home</small>
            <b>{comp_u}</b>
            <em>completed onboarding</em>
          </div>
          <div class="mf-kpi bad">
            <small>Dropped Off</small>
            <b>{drop_u}</b>
            <em>stopped before Home</em>
          </div>
          <div class="mf-kpi red">
            <small>Conversion Rate</small>
            <b>{conv}</b>
            <em>{comp_u} of {total_u} users</em>
          </div>
        </div>
        """, unsafe_allow_html=True)

        col_dl, _ = st.columns([1, 3])
        with col_dl:
            st.download_button(
                label="Download HTML Report",
                data=html_tab,
                file_name=f"safe_delivery_flow_{chosen_date_arg}.html",
                mime="text/html",
                type="primary",
            )

        st.divider()
        components.html(html_tab, height=1050, scrolling=True)


# ────────────────────────────────────────────────────────────
# TAB 3 — DATA MANAGEMENT
# ────────────────────────────────────────────────────────────
with tab_data:
    st.markdown("#### Data Management & Ingestion Health")
    st.caption(
        "Upload CSV exports directly, manage imports, verify B-Tree indexes, "
        "and review table statistics to ensure your dashboard scales with production data."
    )

    # ── Upload section ──
    st.markdown('<div class="mf-section-heading">Upload & Ingest CSV Files</div>', unsafe_allow_html=True)
    st.write(
        "Select one or more CSV files to upload into the `csv_data/` folder and automatically "
        "ingest into PostgreSQL. Multiple files sharing the same base name will be merged into one table."
    )

    uploaded_files = st.file_uploader(
        "Choose CSV files",
        type=["csv"],
        accept_multiple_files=True,
        help="Upload Safe Delivery export CSV files.",
        key="csv_uploader",
    )

    col_up, _ = st.columns([2, 4])
    with col_up:
        upload_clicked = st.button(
            "Upload & Ingest into PostgreSQL",
            type="primary",
            disabled=(not uploaded_files),
            key="upload_ingest_btn",
        )

    if upload_clicked and uploaded_files:
        with st.spinner(f"Uploading {len(uploaded_files)} file(s) and ingesting into PostgreSQL…"):
            try:
                result = ingest_uploaded_csvs(uploaded_files)
                if result.get("status") == "ok":
                    st.success(
                        f"Successfully ingested **{result['files_processed']} file(s)** "
                        f"into **{len(result['tables'])} table(s)** "
                        f"({result['total_rows']:,} total rows)."
                    )
                    for tname, rcount in result.get("row_counts", {}).items():
                        st.caption(f"  {tname}  →  {rcount:,} rows")
                    journey_engine._schema_cache = None
                    st.rerun()
                else:
                    st.error(f"Ingestion failed: {result.get('message', 'Unknown error')}")
            except Exception as e:
                st.error(f"Upload error: {e}")

    if uploaded_files and not upload_clicked:
        st.info(f"{len(uploaded_files)} file(s) selected. Click the button above to ingest.")
        for f in uploaded_files:
            size_kb = round(len(f.getvalue()) / 1024, 1)
            st.caption(f"  {f.name}  ({size_kb} KB)")

    st.divider()

    # ── Re-sync section ──
    st.markdown('<div class="mf-section-heading">Re-sync from csv_data/ Folder</div>', unsafe_allow_html=True)
    st.write("Re-run ingestion on all CSV files already present in the `csv_data/` directory on disk.")

    col_rs, _ = st.columns([2, 4])
    with col_rs:
        if st.button("Sync & Re-Index All CSVs", key="resync_btn"):
            with st.spinner("Ingesting CSVs and building B-tree indexes…"):
                try:
                    import subprocess
                    venv_python = ".venv/bin/python" if os.path.exists(".venv/bin/python") else sys.executable
                    result = subprocess.run(
                        [venv_python, "load_csvs_to_postgres.py"],
                        capture_output=True, text=True,
                        cwd=os.path.dirname(os.path.abspath(__file__)) or ".",
                    )
                    st.code(result.stdout or "(no output)")
                    if result.returncode == 0:
                        st.success("CSVs synchronised with PostgreSQL successfully.")
                    else:
                        st.error(f"Ingestion failed: {result.stderr}")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()

    # ── Table status ──
    st.markdown('<div class="mf-section-heading">Database Table Status</div>', unsafe_allow_html=True)
    try:
        conn = psycopg2.connect(**PG_KWARGS)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name,
                   (SELECT count(*) FROM information_schema.columns WHERE table_name = t.table_name) as col_count
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        table_info = cur.fetchall()

        table_stats = []
        for tname, ccount in table_info:
            try:
                cur.execute(f'SELECT count(*) FROM "{tname}"')
                rcount = cur.fetchone()[0]
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tname}' AND column_name = 'profile_id'")
                if cur.fetchone():
                    cur.execute(f'SELECT count(DISTINCT profile_id) FROM "{tname}"')
                    upids = cur.fetchone()[0]
                else:
                    upids = "—"
                cur.execute(f"SELECT indexname FROM pg_indexes WHERE tablename = '{tname}'")
                idxs = [r[0] for r in cur.fetchall()]
                has_key = any("prof" in ix or "time" in ix for ix in idxs)
                idx_health = "✓ Indexed" if has_key else ("✗ None" if not idxs else "~ Partial")
                table_stats.append({
                    "Table": tname,
                    "Rows": f"{rcount:,}",
                    "Unique Users": upids,
                    "Columns": ccount,
                    "Index Health": idx_health,
                })
            except Exception:
                pass

        cur.close()
        conn.close()

        if table_stats:
            df_t = pd.DataFrame(table_stats)
            st.dataframe(df_t, use_container_width=True, hide_index=True)

            total_rows = sum(int(r["Rows"].replace(",", "")) for r in table_stats)
            well_idx   = sum(1 for r in table_stats if "✓" in r["Index Health"])
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Tables", len(table_stats))
            c2.metric("Total Rows", f"{total_rows:,}")
            c3.metric("Well-Indexed", f"{well_idx}/{len(table_stats)}")

    except Exception as exc:
        st.warning(f"Could not load database status: {exc}")