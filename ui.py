import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

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
# 1. STREAMLIT + ASYNCIO FIX
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
PALETTE = ["#0F6E86", "#23734A", "#B42318", "#9A5B00", "#6366F1", "#EC4899", "#8B5CF6", "#06B6D4"]

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
        "onboarding_events": "Survey questions and answers. question_id = full question text; answer_id = user answer string.",
        "app_lifecycle_events": "App installations, launches, device hardware, OS, app version, language, and GPS coords.",
        "download_category_events": "Category selection toggles and batch download triggers with total sizes in MB.",
        "download_language_events": "Language pack selections, download confirmations, and cancellations.",
        "interaction_events": "Screen views, modals, and user navigation through the UI.",
        "terms_conditions_events": "Terms and conditions acceptance on carousel slides (is_accepted = '1').",
        "app_background_events": "App backgrounding, minimizing, and lifecycle interruptions.",
        "video_events": "Offline video playbacks, seeks, and video completion tracking.",
        "download_video_events": "Module video download batches and network status/failure messages.",
        "learning_attempts": "Quiz question interactions, option clicks, and response timing.",
        "learning_results": "Quiz score summaries, levels passed or failed, star ratings.",
        "clinical_content_events": "Procedures, drug reference cards, and action card chapter views.",
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

        conn = None
        cur = None
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
                                values_str = ", ".join(f"'{v}'" for v in values)
                                part += f" [values: {values_str}]"
                        except Exception:
                            pass
                    col_parts.append(part)
                lines.append(f"- {table}({', '.join(col_parts)})")

            self._cache = "\n".join(lines)
        except Exception as exc:
            self._cache = f"(schema lookup failed: {exc})"
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
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
            + "- `event_time` (timestamp) is the primary time column for date filtering. Do not use event_timestamp text.\n"
            + "- Always use `COUNT(DISTINCT profile_id)` when counting users.\n"
            + "- Use `v_all_events` when querying across multiple event tables to avoid repeated joins.\n"
            + "- In `onboarding_events`, `question_id` has the full text of the question, and `answer_id` is the literal answer.\n"
            + "- When asked for numbers or metrics, execute SQL via `run_sql` and cite the exact returned values.\n"
        )

    async def enhance_user_messages(self, messages, user):
        return messages


class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context):
        return User(id="local-user", username="local-user", group_memberships=["user"])


# ============================================================
# 5. VANNA AGENT
# ============================================================
@st.cache_resource
def get_agent():
    model_name = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    llm = OllamaLlmService(
        model=model_name,
        host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
        temperature=0.1,
    )
    db = PostgresRunner(**PG_KWARGS)
    tools = ToolRegistry()
    tools.register_local_tool(RunSqlTool(sql_runner=db), access_groups=["user"])
    schema_enhancer = DynamicSchemaEnhancer(PG_KWARGS)

    return Agent(
        llm_service=llm,
        tool_registry=tools,
        user_resolver=SimpleUserResolver(),
        agent_memory=DemoAgentMemory(),
        llm_context_enhancer=schema_enhancer,
    )


def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


async def ask_agent(agent, question):
    request_context = RequestContext(metadata={"user_id": "local-user"})
    final_text = ""
    all_row_batches = []

    async for component in agent.send_message(request_context, question):
        rich = getattr(component, "rich_component", None)
        simple = getattr(component, "simple_component", None)

        if rich is not None and hasattr(rich, "content") and rich.content:
            final_text = rich.content
        elif simple is not None and hasattr(simple, "text") and simple.text:
            if not final_text:
                final_text = simple.text

        candidate_rows = None
        if rich is not None and hasattr(rich, "rows") and rich.rows:
            candidate_rows = rich.rows
        elif hasattr(component, "rows") and component.rows:
            candidate_rows = component.rows

        if candidate_rows:
            all_row_batches.append(candidate_rows)

    table_rows = []
    if all_row_batches:
        if len(all_row_batches) == 1:
            table_rows = all_row_batches[0]
        else:
            merged_df = pd.concat(
                [pd.DataFrame(batch) for batch in all_row_batches],
                ignore_index=True,
            ).drop_duplicates().reset_index(drop=True)
            table_rows = merged_df.to_dict("records")

    if not table_rows and final_text:
        sql_match = re.search(r"```(?:sql)?\s*(SELECT\b[\s\S]+?)\s*```", final_text, re.IGNORECASE)
        if not sql_match:
            sql_match = re.search(r"\b(SELECT\s+[\s\S]+?FROM\s+[\s\S]+?;)", final_text, re.IGNORECASE)
        if sql_match:
            sql_query = sql_match.group(1).strip().rstrip(";")
            try:
                conn = psycopg2.connect(**PG_KWARGS)
                cur = conn.cursor()
                cur.execute(sql_query)
                if cur.description:
                    col_names = [d[0] for d in cur.description]
                    fetched = cur.fetchall()
                    table_rows = [dict(zip(col_names, row)) for row in fetched]
                cur.close()
                conn.close()
            except Exception:
                pass

    return final_text, table_rows


# ============================================================
# 6. CHART HELPERS
# ============================================================
def clean_column_name(column):
    return str(column).replace("_", " ").replace("-", " ").title()


def looks_like_date_column(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    if series.dtype == object:
        try:
            converted = pd.to_datetime(series, errors="coerce")
            return converted.notna().mean() >= 0.8
        except Exception:
            return False
    return False


def style_fig(fig):
    fig.update_layout(
        template="plotly_white",
        font=dict(family="Segoe UI, Helvetica, Arial, sans-serif", size=13, color="#1f2937"),
        title_font=dict(size=16, color="#111827"),
        margin=dict(l=30, r=30, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, title=None),
        bargap=0.3,
        xaxis=dict(showgrid=False, showline=True, linecolor="#E5E7EB"),
        yaxis=dict(showgrid=True, gridcolor="#F0F1F3", zeroline=False),
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
    non_numeric_cols = [c for c in all_cols if c not in numeric_cols]
    date_cols = [c for c in non_numeric_cols if looks_like_date_column(chart_df[c])]
    category_cols = [c for c in non_numeric_cols if c not in date_cols]

    if len(chart_df) == 1 and len(numeric_cols) == 1 and not category_cols and not date_cols:
        val = chart_df[numeric_cols[0]].iloc[0]
        col_label = clean_column_name(numeric_cols[0])
        st.metric(label=col_label, value=f"{val:,.2f}".rstrip("0").rstrip(".") if isinstance(val, float) else f"{val:,}")
        return

    if not numeric_cols and category_cols:
        cat_col = category_cols[0]
        cat_counts = chart_df[cat_col].value_counts().reset_index()
        cat_counts.columns = [cat_col, "Count"]
        fig = px.bar(cat_counts, x=cat_col, y="Count", text="Count", color_discrete_sequence=PALETTE,
                     title=f"Occurrences by {clean_column_name(cat_col)}")
        style_fig(fig)
        st.plotly_chart(fig, use_container_width=True)
        return

    if not numeric_cols:
        return

    default_x = (category_cols + date_cols + numeric_cols)[0]
    default_y = numeric_cols[0] if numeric_cols[0] != default_x else (numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0])

    fig = px.bar(chart_df, x=default_x, y=default_y, color_discrete_sequence=PALETTE,
                 title=f"{clean_column_name(default_y)} by {clean_column_name(default_x)}")
    style_fig(fig)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# 7. STREAMLIT APP
# ============================================================
st.set_page_config(
    page_title="Safe Delivery AI & User Journey Debugger",
    page_icon="🧭",
    layout="wide",
)

st.markdown("""
<style>
.main-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-bottom: 12px;
    border-bottom: 1px solid #E5E7EB;
    margin-bottom: 20px;
}
.report-badge {
    background-color: #E1F0F4;
    color: #0F6E86;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 13px;
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

journey_engine = UserJourneyEngine(PG_KWARGS)

with st.sidebar:
    st.title("⚙️ Analytics Controls")
    model_name = os.getenv("OLLAMA_MODEL", "qwen3:8b")
    st.caption(f"🧠 Local LLM: `{model_name}`")
    st.caption(f"🗄️ Database: `{os.getenv('POSTGRES_DATABASE', 'vanna_demo')}`")

    st.divider()
    st.subheader("⚡ Cache Statistics")
    cache_count, cache_hits = get_cache_stats()
    st.write(f"• **Questions Cached**: {cache_count}")
    st.write(f"• **Cache Hits Served**: {cache_hits}")
    if st.button("🗑️ Clear Cache"):
        clear_cache()
        st.success("Query cache cleared!")
        st.rerun()

    st.divider()
    st.subheader("💡 Quick Sample Questions")
    sample_queries = [
        "Create a report of all the users of 2026-09-21 and show me their user journey",
        "How many users said yes to the shared phone question?",
        "Show user footfall and drop-off breakpoints",
        "Which languages had the most downloads?",
        "What is the average duration spent by users on the onboarding screens?",
    ]
    for sq in sample_queries:
        if st.button(sq, key=f"sq_{sq[:20]}"):
            st.session_state["user_query"] = sq
            st.rerun()

# Top level tabs
tab_chat, tab_debugger, tab_data = st.tabs([
    "💬 Ask AI & Journey Reports",
    "🧭 User Journey & Flow Debugger",
    "⚡ Production Data & Telemetry Health",
])

# ------------------------------------------------------------
# TAB 1: ASK AI (NATURAL LANGUAGE & INTENT-DRIVEN REPORTS)
# ------------------------------------------------------------
with tab_chat:
    st.subheader("Intelligent Natural Language & Journey Analysis")
    st.caption("Ask questions about user journeys, drop-offs, footfall, or run natural language SQL queries.")

    available_dates = journey_engine.get_available_dates()

    preset_query = st.session_state.pop("user_query", "")
    with st.form("ask_form", clear_on_submit=False):
        question = st.text_input(
            "Enter your question or request:",
            value=preset_query,
            placeholder="e.g. Can you create a report of all the users of 2026-09-21 and show me their user journey?",
        )
        c_sub1, c_sub2 = st.columns([1, 4])
        with c_sub1:
            submitted = st.form_submit_button("Ask Question", type="primary")
        with c_sub2:
            force_refresh = st.checkbox("Force refresh (bypass cache)", value=False)

    if (submitted and question) or (preset_query and question):
        is_journey, target_date, target_pid = parse_journey_query(question, available_dates)

        if is_journey:
            with st.spinner(f"🔍 Analyzing event streams and generating User Journey Report for {target_date or 'all dates'}..."):
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
                # Also store in persistent cache
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
                with st.spinner("🤖 Vanna is analyzing schema and generating SQL query..."):
                    try:
                        agent = get_agent()
                        answer_text, rows = run_async(ask_agent(agent, question))
                        store_cached_answer(question, answer_text, rows)
                        st.session_state["last_sql_result"] = {
                            "question": question,
                            "answer_text": answer_text,
                            "rows": rows,
                            "from_cache": False,
                        }
                    except Exception as exc:
                        st.error(f"Error executing agent query: {exc}")

    # Render results
    last_journey = st.session_state.get("last_journey_result")
    if last_journey and last_journey.get("question") == question:
        st.markdown(last_journey["summary"])

        col_dl, col_blank = st.columns([1, 3])
        with col_dl:
            st.download_button(
                label="⬇️ Download Full Interactive HTML Report",
                data=last_journey["html"],
                file_name=f"safe_delivery_user_journey_{last_journey['date']}.html",
                mime="text/html",
                type="primary",
            )

        st.subheader("Interactive Onboarding Flow Debugger")
        components.html(last_journey["html"], height=950, scrolling=True)

    elif st.session_state.get("last_sql_result"):
        last_sql = st.session_state["last_sql_result"]
        if last_sql.get("from_cache"):
            st.caption("⚡ Answered from local cache.")
        st.markdown(last_sql["answer_text"])
        if last_sql.get("rows"):
            df = pd.DataFrame(last_sql["rows"])
            st.dataframe(df, use_container_width=True)
            display_chart(df)

# ------------------------------------------------------------
# TAB 2: STANDALONE USER JOURNEY & FLOW DEBUGGER
# ------------------------------------------------------------
with tab_debugger:
    st.subheader("🧭 Safe Delivery User Journey & Funnel Debugger")
    st.caption("Reconstruct end-to-end user footfall, session durations, screen paths, and telemetry anomalies.")

    available_dates = journey_engine.get_available_dates()
    date_options = ["All days"] + available_dates if available_dates else ["All days"]

    f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
    with f_col1:
        selected_date = st.selectbox("Select Date Cohort", options=date_options, index=0)
    with f_col2:
        selected_version = st.selectbox("App Version Filter", options=["All versions", "4.0.0"], index=1)
    with f_col3:
        user_pid_filter = st.text_input("Optional Profile ID Filter:", placeholder="e.g. BWT-ZQP-UCW-SRJ-U1")

    chosen_date_arg = "all" if selected_date == "All days" else selected_date
    chosen_ver_arg = "all" if selected_version == "All versions" else selected_version

    rep_tab = journey_engine.generate_journey_report(
        date_filter=chosen_date_arg,
        version_filter=chosen_ver_arg,
        profile_id=user_pid_filter.strip() if user_pid_filter else None,
    )

    if not rep_tab["users"]:
        st.warning(f"No user events found matching date '{selected_date}' and version '{selected_version}'.")
    else:
        html_tab = journey_engine.render_html_report(rep_tab)
        c_kpi1, c_kpi2, c_kpi3, c_kpi4, c_dl = st.columns([1.5, 1.5, 1.5, 1.5, 2])
        total_u = len(rep_tab["users"])
        comp_u = len([u for u in rep_tab["users"] if u["outcome"] == "Completed"])
        drop_u = total_u - comp_u
        with c_kpi1:
            st.metric("Total Users", total_u)
        with c_kpi2:
            st.metric("Reached Home", comp_u)
        with c_kpi3:
            st.metric("Dropped Off", drop_u)
        with c_kpi4:
            st.metric("Conversion Rate", f"{(comp_u/total_u*100):.1f}%" if total_u else "0%")
        with c_dl:
            st.download_button(
                label="⬇️ Download Standalone HTML",
                data=html_tab,
                file_name=f"safe_delivery_flow_report_{chosen_date_arg}.html",
                mime="text/html",
                type="primary",
            )

        st.divider()
        components.html(html_tab, height=1050, scrolling=True)

# ------------------------------------------------------------
# TAB 3: BIG DATA & INGESTION HEALTH
# ------------------------------------------------------------
with tab_data:
    st.subheader("⚡ Production Data & Ingestion Health")
    st.write(
        "Manage CSV imports, verify B-Tree indexes, and review table statistics to ensure "
        "your dashboard scales smoothly with production data."
    )

    col_btn1, col_btn2 = st.columns([2, 4])
    with col_btn1:
        if st.button("🔄 Sync & Re-Index All CSVs in csv_data/"):
            with st.spinner("Ingesting CSVs and building high-performance B-tree indexes..."):
                try:
                    import subprocess
                    result = subprocess.run(
                        [sys.executable if "sys" in locals() else ".venv/bin/python", "load_csvs_to_postgres.py"],
                        capture_output=True,
                        text=True,
                    )
                    st.code(result.stdout)
                    if result.returncode == 0:
                        st.success("Successfully synchronized CSVs with PostgreSQL and created B-tree indexes!")
                    else:
                        st.error(f"Ingestion failed: {result.stderr}")
                except Exception as e:
                    st.error(f"Execution error: {e}")

    # Inspect tables in postgres
    try:
        conn = psycopg2.connect(**PG_KWARGS)
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                t.table_name,
                (SELECT count(*) FROM information_schema.columns WHERE table_name = t.table_name) as col_count
            FROM information_schema.tables t
            WHERE t.table_schema = 'public'
            ORDER BY t.table_name
        """)
        table_info = cur.fetchall()

        table_stats = []
        for tname, ccount in table_info:
            try:
                cur.execute(f'SELECT count(*) FROM "{tname}"')
                rcount = cur.fetchone()[0]
                has_pid = False
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tname}' AND column_name = 'profile_id'")
                if cur.fetchone():
                    cur.execute(f'SELECT count(DISTINCT profile_id) FROM "{tname}"')
                    upids = cur.fetchone()[0]
                else:
                    upids = "—"

                # Check indexes
                cur.execute(f"SELECT indexname FROM pg_indexes WHERE tablename = '{tname}'")
                idxs = [r[0] for r in cur.fetchall()]

                table_stats.append({
                    "Table Name": tname,
                    "Total Rows": rcount,
                    "Unique Users": upids,
                    "Columns": ccount,
                    "Indexes": ", ".join(idxs) if idxs else "None (unindexed)",
                })
            except Exception:
                pass

        cur.close()
        conn.close()

        if table_stats:
            st.subheader("Database Table Status")
            st.dataframe(pd.DataFrame(table_stats), use_container_width=True)

    except Exception as exc:
        st.warning(f"Could not load database status: {exc}")