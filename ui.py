import asyncio
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from sqlalchemy import create_engine

from vanna import Agent
from vanna.core.enhancer import LlmContextEnhancer
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


def _normalize_question(question: str) -> str:
    return " ".join(question.strip().lower().split())


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


def get_cached_answer(question: str):
    key = _normalize_question(question)
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


def store_cached_answer(question: str, answer_text: str, rows: list):
    if not answer_text and not rows:
        return
    key = _normalize_question(question)
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
# 4. DYNAMIC SCHEMA ENHANCER
# ============================================================
class DynamicSchemaEnhancer(LlmContextEnhancer):
    TABLE_DESCRIPTIONS = {
        "onboarding_events": "Survey questions and answers.",
        "app_lifecycle_events": "App installations, launches, device hardware, OS, app version, language, and GPS coords.",
        "download_category_events": "Category selection toggles and batch download triggers.",
        "download_language_events": "Language pack selections, download confirmations, and cancellations.",
        "interaction_events": "Screen views, modals, and user navigation through the UI.",
        "terms_conditions_events": "Terms and conditions acceptance.",
        "app_background_events": "App backgrounding and lifecycle interruptions.",
        "video_events": "Offline video playbacks and completion tracking.",
        "download_video_events": "Module video download batches.",
        "learning_attempts": "Quiz question interactions.",
        "learning_results": "Quiz score summaries.",
        "clinical_content_events": "Procedures and drug reference card views.",
        "v_all_events": "Unified fast indexed view across all event tables.",
    }

    _SKIP_ENUM_COLUMNS = {
        "selected_categories", "selected_category_titles", "extra",
        "id", "session_id", "app_installation_id", "category_id",
        "language_id", "source_file", "ingested_at", "event_timestamp",
    }

    def __init__(self, pg_kwargs, enum_threshold=30):
        self.pg_kwargs = pg_kwargs
        self.enum_threshold = enum_threshold
        self._cache = None

    def _discover_tables(self, cur):
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name NOT LIKE 'pg_%'
            ORDER BY table_name
        """)
        return [r[0] for r in cur.fetchall()]

    def _load_schema(self):
        if self._cache is not None:
            return self._cache
        conn = cur = None
        try:
            conn = psycopg2.connect(**self.pg_kwargs)
            cur = conn.cursor()
            tables = self._discover_tables(cur)
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
                    if dtype in TEXT_TYPES and name not in self._SKIP_ENUM_COLUMNS and not table.startswith("v_"):
                        try:
                            cur.execute(f'SELECT COUNT(DISTINCT "{name}") FROM "{table}"')
                            row = cur.fetchone()
                            distinct_count = row[0] if row is not None else None
                            if distinct_count is not None and 0 < distinct_count <= self.enum_threshold:
                                cur.execute(f'SELECT DISTINCT "{name}" FROM "{table}" WHERE "{name}" IS NOT NULL ORDER BY 1')
                                values = [str(r[0]) for r in cur.fetchall()]
                                part += f" [values: {', '.join(repr(v) for v in values)}]"
                        except Exception:
                            pass
                    col_parts.append(part)
                lines.append(f"- {table}({', '.join(col_parts)})")
            self._cache = "\n".join(lines)
        except Exception as exc:
            self._cache = f"(schema lookup failed: {exc})"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        return self._cache

    async def enhance_system_prompt(self, system_prompt, user_message, user):
        schema = self._load_schema()
        return (
            system_prompt
            + "\n\n## Database schema (PostgreSQL production data)\n"
            + schema
            + "\n\n## Important Table Guidelines\n"
            + "- `profile_id` is the shared user identifier across all event tables.\n"
            + "- `event_time` (timestamp) is the primary time column for date filtering.\n"
            + "- Always use `COUNT(DISTINCT profile_id)` when counting users.\n"
            + "- Use `v_all_events` when querying across multiple event tables.\n"
            + "- When asked for numbers, execute SQL via `run_sql` and cite exact returned values.\n"
        )

    async def enhance_user_messages(self, messages, user):
        return messages


class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context):
        return User(id="local-user", username="local-user", group_memberships=["user"])


# ============================================================
# 5. DIRECT QWEN SQL PIPELINE  (replaces Vanna agent loop)
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


TABLE_NOTES = """
## CRITICAL ARCHITECTURE & RELATIONSHIP RULES:
1. `profile_id` is the shared user identifier across ALL tables (app_lifecycle_events, download_language_events, download_category_events, onboarding_events, terms_conditions_events, interaction_events, v_all_events).
2. Joining multiple tables:
   - When a question requires filtering or aggregating across multiple tables (e.g. users on Android who downloaded a category), JOIN the tables on "profile_id".
   - Example: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a JOIN "download_category_events" c ON a."profile_id" = c."profile_id" WHERE a."device_os" = 'Android' AND c."event_type" = 'downloaded';
3. `event_time` (timestamp) is the primary time column for date filtering. For daily breakdowns or filtering by day, use: DATE("event_time") = 'YYYY-MM-DD' or "event_time"::date = 'YYYY-MM-DD'.
4. Always use `COUNT(DISTINCT "profile_id")` when counting users.
5. Do NOT use LIMIT unless specifically asked.
6. Always qualify table names with double-quotes e.g. "download_category_events".

## SCREEN & EVENT LOGGING RULES (MANDATORY):
- **Category Screen (`category_list_screen`)**:
  - Logged in table `"download_category_events"` (and in `"v_all_events"` where `screen_name = 'category_list_screen'`).
  - Its event_type values are ONLY 'selected' and 'downloaded'.
  - IMPORTANT: There is NO 'viewed' event_type for category screen! When the user asks "how many users viewed / visited / reached / opened category screen", query:
    SELECT COUNT(DISTINCT "profile_id") FROM "download_category_events";
    (or SELECT COUNT(DISTINCT "profile_id") FROM "v_all_events" WHERE "screen_name" = 'category_list_screen';).
    NEVER add `AND event_type = 'viewed'`.

- **Language Screen (`language_list_screen`)**:
  - Logged in table `"download_language_events"` (and in `"v_all_events"` where `screen_name = 'language_list_screen'`).
  - Its event_type values are 'selected', 'downloaded', 'cancelled'.
  - IMPORTANT: There is NO 'viewed' event_type for language screen! When the user asks "how many users viewed / visited / reached language screen", query:
    SELECT COUNT(DISTINCT "profile_id") FROM "download_language_events" WHERE "screen_name" = 'language_list_screen';
    NEVER add `AND event_type = 'viewed'`.

- **App Install / Launch (`splash_screen`)**:
  - Logged in table `"app_lifecycle_events"`.
  - Contains device_model, device_os, app_version, app_id, location_lat, location_long, language_version.
  - Event type: 'installed'.

- **Terms & Conditions (`onboarding_screen`)**:
  - Logged in table `"terms_conditions_events"`.
  - Event type: 'accepted', `is_accepted = '1'`.

- **Onboarding Survey (`onboarding_survey_screen`)**:
  - Logged in table `"onboarding_events"`.
  - Event type: 'answered'.
  - `question_id` contains the English question string (e.g. 'Is this device a shared phone/tablet?', 'Are you a healthcare professional (or studying to become one)?', 'Where do you work?', 'What is your profession?', 'How did you hear about the Safe Delivery app?', 'How many years of experience do you have as a healthcare professional?').
  - `answer_id` contains normalized answer code ('yes', 'no', 'student', 'other', '6_to_10_years', etc.).
  - Always use `"question_id" ILIKE '%keyword%'` and `"answer_id" = '...'`.

- **Interaction Events**:
  - Table `"interaction_events"` only logs modal popups (like "Why we need your data" modal) on onboarding carousel.
  - Do NOT use `"interaction_events"` for category screen or language screen.

- **Unified Cross-Table View (`v_all_events`)**:
  - Unified view combining all event tables with columns: `profile_id`, `event_time`, `event_timestamp`, `event_type`, `description`, `screen_name`, `log_source`, `session_id`, `autonym_script`.
  - Ideal for cross-screen sequences, funnels, and user timelines.

- **Drop-offs & Funnels**:
  - To find users who did action A but not action B, use `WHERE "profile_id" NOT IN (SELECT DISTINCT "profile_id" FROM ...)` or compare stage counts.

FEW-SHOT EXAMPLES:
User: How many users viewed category screen
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "download_category_events";

User: How many users viewed language screen
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "download_language_events" WHERE "screen_name" = 'language_list_screen';

User: How many users on Android downloaded a category?
SQL: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a JOIN "download_category_events" c ON a."profile_id" = c."profile_id" WHERE a."device_os" = 'Android' AND c."event_type" = 'downloaded';

User: How many users answered yes to the shared phone question?
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "onboarding_events" WHERE "question_id" ILIKE '%shared phone%' AND "answer_id" = 'yes';

User: How many users accepted terms and conditions?
SQL: SELECT COUNT(DISTINCT "profile_id") FROM "terms_conditions_events" WHERE "event_type" = 'accepted';

User: How many users installed the app but dropped off before category screen?
SQL: SELECT COUNT(DISTINCT a."profile_id") FROM "app_lifecycle_events" a WHERE a."profile_id" NOT IN (SELECT DISTINCT "profile_id" FROM "download_category_events");
"""


def sanitize_generated_sql(sql: str) -> str:
    """Sanitizes LLM-generated SQL against known false assumptions."""
    # 1. If querying interaction_events for category or language screens, redirect to proper tables
    if "interaction_events" in sql:
        if "category_list_screen" in sql or "category" in sql.lower():
            sql = re.sub(r'["\']?interaction_events["\']?', '"download_category_events"', sql)
            sql = re.sub(r"""\s+AND\s+["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"""\s+WHERE\s+["']?event_type["']?\s*=\s*['"]viewed['"]\s+AND\s+""", " WHERE ", sql, flags=re.IGNORECASE)
            sql = re.sub(r"""WHERE\s+["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)
        elif "language_list_screen" in sql or "language" in sql.lower():
            sql = re.sub(r'["\']?interaction_events["\']?', '"download_language_events"', sql)
            sql = re.sub(r"""\s+AND\s+["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"""\s+WHERE\s+["']?event_type["']?\s*=\s*['"]viewed['"]\s+AND\s+""", " WHERE ", sql, flags=re.IGNORECASE)
            sql = re.sub(r"""WHERE\s+["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)

    # 2. Remove erroneous event_type = 'viewed' from category/language tables or v_all_events with category/language screen
    if any(k in sql for k in ["download_category_events", "category_list_screen", "download_language_events", "language_list_screen"]):
        sql = re.sub(r"""\s+AND\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"""\s+WHERE\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]\s+AND\s+""", " WHERE ", sql, flags=re.IGNORECASE)
        sql = re.sub(r"""WHERE\s+([a-zA-Z0-9_".]+\.)?["']?event_type["']?\s*=\s*['"]viewed['"]""", "", sql, flags=re.IGNORECASE)

    return sql.strip()


def _call_qwen(messages: list, max_tokens: int = 400) -> str:
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
    with urllib.request.urlopen(req, timeout=60) as resp:
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
                "Given a user question, return ONLY a valid PostgreSQL SELECT query — no explanation, "
                "no markdown, no backticks. Just the raw SQL ending with a semicolon.\n\n"
                f"Database schema:\n{schema}\n\n"
                f"{TABLE_NOTES}"
            ),
        },
        {"role": "user", "content": question},
    ], max_tokens=300)

    # Clean the SQL — strip markdown code fences if model adds them
    sql_clean = re.sub(r"```(?:sql)?\s*", "", sql_raw, flags=re.IGNORECASE)
    sql_clean = re.sub(r"```", "", sql_clean).strip().rstrip(";")

    # Extract first SELECT statement
    sel_match = re.search(r"(SELECT[\s\S]+)", sql_clean, re.IGNORECASE)
    if not sel_match:
        return ("I couldn't generate a valid SQL query for that question. Try rephrasing it.", [])
    sql = sel_match.group(1).strip()
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
                        "the corrected SQL query, no explanation, no markdown.\n\n"
                        f"Database schema:\n{schema}\n{TABLE_NOTES}"
                    ),
                },
                {"role": "user", "content": f"Original question: {question}\n\nFailed SQL:\n{sql}\n\nError: {sql_err}\n\nFixed SQL:"},
            ], max_tokens=300)
            fix_sql = re.sub(r"```(?:sql)?\s*", "", fix_raw, flags=re.IGNORECASE)
            fix_sql = re.sub(r"```", "", fix_sql).strip().rstrip(";")
            fix_match = re.search(r"(SELECT[\s\S]+)", fix_sql, re.IGNORECASE)
            if fix_match:
                sql = fix_match.group(1).strip()
                sql = sanitize_generated_sql(sql)
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

    # ── Step 3: Explain results in plain English ──────────────
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
    if series.dtype == object:
        try:
            return pd.to_datetime(series, errors="coerce").notna().mean() >= 0.8
        except Exception:
            return False
    return False


def style_fig(fig):
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Montserrat, Inter, sans-serif", size=13, color="#1D1D1B"),
        title_font=dict(size=15, color="#1D1D1B", family="Montserrat, Inter, sans-serif"),
        margin=dict(l=30, r=30, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
        bargap=0.3,
        xaxis=dict(showgrid=False, showline=True, linecolor="#E0E0E0"),
        yaxis=dict(showgrid=True, gridcolor="#F2F2F2", zeroline=False),
        plot_bgcolor="#FFFFFF",
        paper_bgcolor="#FFFFFF",
    )
    return fig


def display_chart(df):
    if df is None or df.empty:
        return
    chart_df = df.copy()
    for col in chart_df.columns:
        if chart_df[col].dtype == object:
            converted = pd.to_numeric(chart_df[col], errors="coerce")
            if pd.Series(converted).notna().mean() >= 0.8:
                chart_df[col] = converted

    numeric_cols = chart_df.select_dtypes(include="number").columns.tolist()
    all_cols = chart_df.columns.tolist()
    non_numeric = [c for c in all_cols if c not in numeric_cols]
    date_cols = [c for c in non_numeric if looks_like_date_column(chart_df[c])]
    cat_cols = [c for c in non_numeric if c not in date_cols]

    if len(chart_df) == 1 and len(numeric_cols) == 1 and not cat_cols and not date_cols:
        val = chart_df[numeric_cols[0]].iloc[0]
        lbl = clean_column_name(numeric_cols[0])
        st.metric(label=lbl, value=f"{val:,.2f}".rstrip("0").rstrip(".") if isinstance(val, float) else f"{val:,}")
        return

    if not numeric_cols and cat_cols:
        cat_col = cat_cols[0]
        cat_counts = chart_df[cat_col].value_counts().reset_index()
        cat_counts.columns = [cat_col, "Count"]
        fig = px.bar(cat_counts, x=cat_col, y="Count", text="Count",
                     color_discrete_sequence=PALETTE,
                     title=f"Occurrences by {clean_column_name(cat_col)}")
        style_fig(fig)
        st.plotly_chart(fig, use_container_width=True)
        return

    if not numeric_cols:
        return

    default_x = (cat_cols + date_cols + numeric_cols)[0]
    default_y = numeric_cols[0] if numeric_cols[0] != default_x else (numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0])
    fig = px.bar(chart_df, x=default_x, y=default_y,
                 color_discrete_sequence=PALETTE,
                 title=f"{clean_column_name(default_y)} by {clean_column_name(default_x)}")
    style_fig(fig)
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
        dest.write_bytes(f.read())
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
            cached = None if force_refresh else get_cached_answer(question)
            if cached is not None:
                answer_text, rows = cached
                st.session_state["last_sql_result"] = {
                    "question": question,
                    "answer_text": answer_text,
                    "rows": rows,
                    "from_cache": True,
                }
            else:
                with st.spinner("Asking AI · generating SQL and analysing results…"):
                    try:
                        answer_text, rows = ask_qwen_sql(question)
                        store_cached_answer(question, answer_text, rows)
                        st.session_state["last_sql_result"] = {
                            "question": question,
                            "answer_text": answer_text,
                            "rows": rows,
                            "from_cache": False,
                        }
                    except Exception as exc:
                        st.error(f"Error: {exc}")

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
        if last_sql.get("from_cache"):
            st.caption("Answered from local cache.")
        st.markdown(last_sql["answer_text"])
        if last_sql.get("rows"):
            df = pd.DataFrame(last_sql["rows"])
            st.dataframe(df, use_container_width=True)
            display_chart(df)


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