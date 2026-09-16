# import asyncio
# import os

# import pandas as pd
# import psycopg2
# import streamlit as st
# from dotenv import load_dotenv

# from vanna import Agent
# from vanna.core.registry import ToolRegistry
# from vanna.core.user import User
# from vanna.core.user.resolver import UserResolver
# from vanna.core.user.request_context import RequestContext
# from vanna.core.enhancer import LlmContextEnhancer
# from vanna.integrations.ollama import OllamaLlmService
# from vanna.integrations.postgres import PostgresRunner
# from vanna.integrations.local.agent_memory import DemoAgentMemory
# from vanna.tools import RunSqlTool


# # ============================================================
# # 1. STREAMLIT + ASYNCIO FIX
# # ============================================================

# # Streamlit runs the application in its own ScriptRunner thread.
# # Python 3.9 does not always create an asyncio event loop there.
# try:
#     asyncio.get_event_loop()
# except RuntimeError:
#     asyncio.set_event_loop(asyncio.new_event_loop())


# # ============================================================
# # 2. LOAD ENVIRONMENT VARIABLES
# # ============================================================

# load_dotenv()


# # ============================================================
# # 3. POSTGRES CONFIGURATION
# # ============================================================

# PG_KWARGS = dict(
#     host=os.getenv("POSTGRES_HOST", "localhost"),
#     port=int(os.getenv("POSTGRES_PORT", "5432")),
#     database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
#     user=os.getenv("POSTGRES_USER", "vanna_readonly"),
#     password=os.getenv("POSTGRES_PASSWORD"),
# )


# # ============================================================
# # 4. DATABASE TABLES KNOWN TO THE AGENT
# # ============================================================

# KNOWN_TABLES = [
#     "onboarding_events"
# ]


# # ============================================================
# # 5. TEXT TYPES
# # ============================================================

# TEXT_TYPES = (
#     "text",
#     "character varying",
#     "varchar",
#     "char",
#     "character",
# )


# # ============================================================
# # 6. SCHEMA ENHANCER
# # ============================================================

# class SchemaEnhancer(LlmContextEnhancer):

#     def __init__(
#         self,
#         pg_kwargs,
#         tables,
#         enum_threshold=50
#     ):
#         self.pg_kwargs = pg_kwargs
#         self.tables = tables
#         self.enum_threshold = enum_threshold
#         self._cache = None

#     def _load_schema(self):

#         # Return cached schema if we already loaded it
#         if self._cache is not None:
#             return self._cache

#         conn = None
#         cur = None

#         try:

#             conn = psycopg2.connect(**self.pg_kwargs)
#             cur = conn.cursor()

#             lines = []

#             for table in self.tables:

#                 # ------------------------------------------------
#                 # Get columns and data types
#                 # ------------------------------------------------

#                 cur.execute(
#                     """
#                     SELECT
#                         column_name,
#                         data_type
#                     FROM information_schema.columns
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                     """,
#                     (table,),
#                 )

#                 cols = cur.fetchall()

#                 if not cols:
#                     lines.append(
#                         f"- {table}: "
#                         "(no columns found — check the table name)"
#                     )
#                     continue

#                 col_parts = []

#                 for name, dtype in cols:

#                     part = f"{name} ({dtype})"

#                     # ------------------------------------------------
#                     # For text columns, get real values if low-cardinality
#                     # ------------------------------------------------

#                     if dtype in TEXT_TYPES:

#                         cur.execute(
#                             f'''
#                             SELECT COUNT(DISTINCT "{name}")
#                             FROM "{table}"
#                             '''
#                         )

#                         distinct_count = cur.fetchone()[0]

#                         if (
#                             distinct_count is not None
#                             and 0 < distinct_count <= self.enum_threshold
#                         ):

#                             cur.execute(
#                                 f'''
#                                 SELECT DISTINCT "{name}"
#                                 FROM "{table}"
#                                 ORDER BY 1
#                                 '''
#                             )

#                             values = [
#                                 str(row[0])
#                                 for row in cur.fetchall()
#                                 if row[0] is not None
#                             ]

#                             values_str = ", ".join(
#                                 f"'{value}'"
#                                 for value in values
#                             )

#                             part += (
#                                 f" [actual values: {values_str}]"
#                             )

#                     col_parts.append(part)

#                 lines.append(
#                     f"- {table}({', '.join(col_parts)})"
#                 )

#             self._cache = "\n".join(lines)

#         except Exception as exc:

#             self._cache = (
#                 f"(schema lookup failed: {exc})"
#             )

#         finally:

#             if cur is not None:
#                 cur.close()

#             if conn is not None:
#                 conn.close()

#         return self._cache

#     async def enhance_system_prompt(
#         self,
#         system_prompt,
#         user_message,
#         user
#     ):

#         schema = self._load_schema()

#         return (
#             system_prompt

#             + "\n\n"
#             + "## Database schema "
#             + "(this is the ONLY schema that exists)\n"

#             + schema

#             + "\n\n"
#             + "## Important notes on this schema\n"

#             + (
#                 "- `onboarding_events` stores ONE ROW PER QUESTION "
#                 "a user (`profile_id`) answered.\n"
#             )

#             + (
#                 "- Despite the column names, `question_id` holds "
#                 "the FULL TEXT of the question "
#                 "(not a numeric id), and `answer_id` holds "
#                 "the literal answer text "
#                 "(not a numeric id).\n"
#             )

#             + (
#                 "- To count how many users gave a specific answer "
#                 "to a specific question, filter with an exact "
#                 "string match on BOTH `question_id` and `answer_id`, "
#                 "then COUNT(DISTINCT profile_id).\n"
#             )

#             + (
#                 "- Example:\n"
#                 "  SELECT COUNT(DISTINCT profile_id)\n"
#                 "  FROM onboarding_events\n"
#                 "  WHERE question_id = "
#                 "'Is this device a shared phone/tablet?'\n"
#                 "  AND answer_id = 'yes';\n"
#             )

#             + (
#                 "- Only use the exact question/answer strings "
#                 "listed in the schema above — never paraphrase "
#                 "or guess the wording.\n"
#             )
#         )

#     async def enhance_user_messages(
#         self,
#         messages,
#         user
#     ):
#         return messages


# # ============================================================
# # 7. USER RESOLVER
# # ============================================================

# class SimpleUserResolver(UserResolver):

#     async def resolve_user(self, request):

#         return User(
#             id="local-user",
#             username="local-user",
#             group_memberships=["user"],
#         )


# # ============================================================
# # 8. CREATE Vanna AGENT
# # ============================================================

# @st.cache_resource
# def get_agent():

#     # --------------------------------------------------------
#     # Ollama
#     # --------------------------------------------------------

#     llm = OllamaLlmService(
#         model=os.getenv(
#             "OLLAMA_MODEL",
#             "qwen3:14b"
#         ),
#         host=os.getenv(
#             "OLLAMA_HOST",
#             "http://localhost:11434"
#         ),
#     )

#     # --------------------------------------------------------
#     # PostgreSQL
#     # --------------------------------------------------------

#     db = PostgresRunner(
#         **PG_KWARGS
#     )

#     # --------------------------------------------------------
#     # Tool registry
#     # --------------------------------------------------------

#     tools = ToolRegistry()

#     sql_tool = RunSqlTool(
#         sql_runner=db
#     )

#     tools.register_local_tool(
#         sql_tool,
#         access_groups=["user"]
#     )

#     # --------------------------------------------------------
#     # Schema enhancer
#     # --------------------------------------------------------

#     schema_enhancer = SchemaEnhancer(
#         PG_KWARGS,
#         KNOWN_TABLES
#     )

#     # --------------------------------------------------------
#     # Agent
#     # --------------------------------------------------------

#     agent = Agent(
#         llm_service=llm,
#         tool_registry=tools,
#         user_resolver=SimpleUserResolver(),
#         agent_memory=DemoAgentMemory(),
#         llm_context_enhancer=schema_enhancer,
#     )

#     return agent


# # ============================================================
# # 9. RUN ASYNC Vanna REQUEST
# # ============================================================

# def run_async(coro):

#     """
#     Run a Vanna async operation using the event loop
#     associated with Streamlit's current thread.
#     """

#     try:

#         loop = asyncio.get_event_loop()

#     except RuntimeError:

#         loop = asyncio.new_event_loop()

#         asyncio.set_event_loop(loop)

#     return loop.run_until_complete(coro)


# # ============================================================
# # 10. ASK VANNA
# # ============================================================

# async def ask_agent(
#     agent,
#     question
# ):

#     request_context = RequestContext(
#         metadata={
#             "user_id": "local-user"
#         }
#     )

#     final_text = ""

#     table_rows = None

#     # --------------------------------------------------------
#     # Send question to Vanna
#     # --------------------------------------------------------

#     async for component in agent.send_message(
#         request_context,
#         question
#     ):

#         # ----------------------------------------------------
#         # Get rich component
#         # ----------------------------------------------------

#         rich = getattr(
#             component,
#             "rich_component",
#             None
#         )

#         if rich is None:
#             continue

#         # ----------------------------------------------------
#         # Text answer
#         # ----------------------------------------------------

#         if hasattr(rich, "content"):

#             if rich.content:

#                 final_text = rich.content

#         # ----------------------------------------------------
#         # Table rows
#         # ----------------------------------------------------

#         if hasattr(rich, "rows"):

#             if rich.rows:

#                 table_rows = rich.rows

#     return final_text, table_rows


# # ============================================================
# # 11. STREAMLIT PAGE CONFIG
# # ============================================================

# st.set_page_config(
#     page_title="Ask Your Data",
#     page_icon="📊",
#     layout="centered",
# )


# # ============================================================
# # 12. UI
# # ============================================================

# st.title("📊 Ask Your Data")

# st.write(
#     "Ask questions about your PostgreSQL database "
#     "using natural language."
# )


# # ============================================================
# # 13. CREATE AGENT
# # ============================================================

# try:

#     agent = get_agent()

# except Exception as exc:

#     st.error(
#         f"Failed to initialize Vanna: {exc}"
#     )

#     st.stop()


# # ============================================================
# # 14. QUESTION INPUT
# # ============================================================

# question = st.text_input(
#     "Ask a question",
#     placeholder=(
#         "Example: How many users said yes "
#         "to the shared phone question?"
#     ),
# )


# # ============================================================
# # 15. ASK BUTTON
# # ============================================================

# if st.button(
#     "Ask",
#     type="primary"
# ) and question:

#     with st.spinner(
#         "Vanna is thinking..."
#     ):

#         try:

#             answer_text, rows = run_async(
#                 ask_agent(
#                     agent,
#                     question
#                 )
#             )

#         except Exception as exc:

#             st.error(
#                 f"Error while asking Vanna: {exc}"
#             )

#             st.exception(exc)

#             st.stop()

#     # ========================================================
#     # ANSWER
#     # ========================================================

#     if answer_text:

#         st.subheader("Answer")

#         st.markdown(
#             answer_text
#         )

#     # ========================================================
#     # DATA TABLE
#     # ========================================================

#     if rows:

#         st.subheader("Data")

#         try:

#             df = pd.DataFrame(
#                 rows
#             )

#             st.dataframe(
#                 df,
#                 use_container_width=True
#             )

#         except Exception as exc:

#             st.warning(
#                 f"Could not display table: {exc}"
#             )

#             df = None

#         # ====================================================
#         # CHARTS
#         # ====================================================

#         if df is not None and not df.empty:

#             numeric_cols = (
#                 df.select_dtypes(
#                     include="number"
#                 )
#                 .columns
#                 .tolist()
#             )

#             non_numeric_cols = [
#                 column
#                 for column in df.columns
#                 if column not in numeric_cols
#             ]

#             # ------------------------------------------------
#             # Multiple rows + numeric column
#             # ------------------------------------------------

#             if (
#                 numeric_cols
#                 and len(df) > 1
#             ):

#                 if non_numeric_cols:

#                     chart_df = (
#                         df
#                         .set_index(
#                             non_numeric_cols[0]
#                         )[numeric_cols]
#                     )

#                     st.subheader("Chart")

#                     st.bar_chart(
#                         chart_df
#                     )

#                 else:

#                     st.subheader("Chart")

#                     st.bar_chart(
#                         df[numeric_cols]
#                     )

#             # ------------------------------------------------
#             # Single numeric result
#             # ------------------------------------------------

#             elif (
#                 numeric_cols
#                 and len(df) == 1
#             ):

#                 column = numeric_cols[0]

#                 st.subheader("Result")

#                 st.metric(
#                     label=column,
#                     value=df[column].iloc[0]
#                 )

#     # ========================================================
#     # NO DATA
#     # ========================================================

#     elif not answer_text:

#         st.info(
#             "Vanna did not return a displayable result."
#         )



import asyncio
import os

import pandas as pd
import psycopg2
import streamlit as st
import plotly.express as px

from dotenv import load_dotenv

from vanna import Agent
from vanna.core.registry import ToolRegistry
from vanna.core.user import User
from vanna.core.user.resolver import UserResolver
from vanna.core.user.request_context import RequestContext
from vanna.core.enhancer import LlmContextEnhancer
from vanna.integrations.ollama import OllamaLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.integrations.local.agent_memory import DemoAgentMemory
from vanna.tools import RunSqlTool


# ============================================================
# 1. STREAMLIT + ASYNCIO FIX
# ============================================================

try:

    asyncio.get_event_loop()

except RuntimeError:

    asyncio.set_event_loop(
        asyncio.new_event_loop()
    )


# ============================================================
# 2. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# 3. POSTGRES CONFIGURATION
# ============================================================

PG_KWARGS = dict(
    host=os.getenv(
        "POSTGRES_HOST",
        "localhost"
    ),
    port=int(
        os.getenv(
            "POSTGRES_PORT",
            "5432"
        )
    ),
    database=os.getenv(
        "POSTGRES_DATABASE",
        "vanna_demo"
    ),
    user=os.getenv(
        "POSTGRES_USER",
        "vanna_readonly"
    ),
    password=os.getenv(
        "POSTGRES_PASSWORD"
    ),
)


# ============================================================
# 4. KNOWN TABLES
# ============================================================

KNOWN_TABLES = [
    "onboarding_events"
]


# ============================================================
# 5. TEXT TYPES
# ============================================================

TEXT_TYPES = (
    "text",
    "character varying",
    "varchar",
    "char",
    "character",
)


# ============================================================
# 6. SCHEMA ENHANCER
# ============================================================

class SchemaEnhancer(
    LlmContextEnhancer
):

    def __init__(
        self,
        pg_kwargs,
        tables,
        enum_threshold=50
    ):

        self.pg_kwargs = pg_kwargs
        self.tables = tables
        self.enum_threshold = enum_threshold
        self._cache = None

    def _load_schema(self):

        if self._cache is not None:
            return self._cache

        conn = None
        cur = None

        try:

            conn = psycopg2.connect(
                **self.pg_kwargs
            )

            cur = conn.cursor()

            lines = []

            for table in self.tables:

                cur.execute(
                    """
                    SELECT
                        column_name,
                        data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table,),
                )

                cols = cur.fetchall()

                if not cols:

                    lines.append(
                        f"- {table}: "
                        "(no columns found — "
                        "check the table name)"
                    )

                    continue

                col_parts = []

                for name, dtype in cols:

                    part = (
                        f"{name} ({dtype})"
                    )

                    if dtype in TEXT_TYPES:

                        cur.execute(
                            f'''
                            SELECT COUNT(DISTINCT "{name}")
                            FROM "{table}"
                            '''
                        )

                        distinct_count = (
                            cur.fetchone()[0]
                        )

                        if (
                            distinct_count is not None
                            and
                            0 < distinct_count
                            <= self.enum_threshold
                        ):

                            cur.execute(
                                f'''
                                SELECT DISTINCT "{name}"
                                FROM "{table}"
                                ORDER BY 1
                                '''
                            )

                            values = [
                                str(row[0])
                                for row in cur.fetchall()
                                if row[0] is not None
                            ]

                            values_str = ", ".join(
                                f"'{value}'"
                                for value in values
                            )

                            part += (
                                f" [actual values: "
                                f"{values_str}]"
                            )

                    col_parts.append(
                        part
                    )

                lines.append(
                    f"- {table}("
                    f"{', '.join(col_parts)}"
                    f")"
                )

            self._cache = (
                "\n".join(lines)
            )

        except Exception as exc:

            self._cache = (
                f"(schema lookup failed: "
                f"{exc})"
            )

        finally:

            if cur is not None:
                cur.close()

            if conn is not None:
                conn.close()

        return self._cache

    async def enhance_system_prompt(
        self,
        system_prompt,
        user_message,
        user
    ):

        schema = (
            self._load_schema()
        )

        return (
            system_prompt

            + "\n\n"
            + "## Database schema "
            + "(this is the ONLY schema "
              "that exists)\n"

            + schema

            + "\n\n"
            + "## Important notes on this schema\n"

            + (
                "- `onboarding_events` stores "
                "ONE ROW PER QUESTION a user "
                "(`profile_id`) answered.\n"
            )

            + (
                "- Despite the column names, "
                "`question_id` holds the FULL TEXT "
                "of the question "
                "(not a numeric id), and "
                "`answer_id` holds the literal "
                "answer text "
                "(not a numeric id).\n"
            )

            + (
                "- To count how many users gave "
                "a specific answer to a specific "
                "question, filter with an exact "
                "string match on BOTH "
                "`question_id` and `answer_id`, "
                "then COUNT(DISTINCT profile_id).\n"
            )

            + (
                "- Example:\n"
                "  SELECT COUNT(DISTINCT profile_id)\n"
                "  FROM onboarding_events\n"
                "  WHERE question_id = "
                "'Is this device a shared phone/tablet?'\n"
                "  AND answer_id = 'yes';\n"
            )

            + (
                "- Only use the exact "
                "question/answer strings "
                "listed in the schema above — "
                "never paraphrase or guess "
                "the wording.\n"
            )
        )

    async def enhance_user_messages(
        self,
        messages,
        user
    ):

        return messages


# ============================================================
# 7. USER RESOLVER
# ============================================================

class SimpleUserResolver(
    UserResolver
):

    async def resolve_user(
        self,
        request
    ):

        return User(
            id="local-user",
            username="local-user",
            group_memberships=["user"],
        )


# ============================================================
# 8. CREATE VANNA AGENT
# ============================================================

@st.cache_resource
def get_agent():

    # --------------------------------------------------------
    # Ollama
    # --------------------------------------------------------

    llm = OllamaLlmService(
        model=os.getenv(
            "OLLAMA_MODEL",
            "qwen3:14b"
        ),
        host=os.getenv(
            "OLLAMA_HOST",
            "http://localhost:11434"
        ),
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    db = PostgresRunner(
        **PG_KWARGS
    )

    # --------------------------------------------------------
    # Tool registry
    # --------------------------------------------------------

    tools = ToolRegistry()

    sql_tool = RunSqlTool(
        sql_runner=db
    )

    tools.register_local_tool(
        sql_tool,
        access_groups=["user"]
    )

    # --------------------------------------------------------
    # Schema enhancer
    # --------------------------------------------------------

    schema_enhancer = SchemaEnhancer(
        PG_KWARGS,
        KNOWN_TABLES
    )

    # --------------------------------------------------------
    # Agent
    # --------------------------------------------------------

    agent = Agent(
        llm_service=llm,
        tool_registry=tools,
        user_resolver=SimpleUserResolver(),
        agent_memory=DemoAgentMemory(),
        llm_context_enhancer=schema_enhancer,
    )

    return agent


# ============================================================
# 9. ASYNC HELPER
# ============================================================

def run_async(coro):

    try:

        loop = asyncio.get_event_loop()

    except RuntimeError:

        loop = asyncio.new_event_loop()

        asyncio.set_event_loop(
            loop
        )

    return loop.run_until_complete(
        coro
    )


# ============================================================
# 10. ASK VANNA
# ============================================================

async def ask_agent(
    agent,
    question
):

    request_context = RequestContext(
        metadata={
            "user_id": "local-user"
        }
    )

    final_text = ""

    table_rows = None

    async for component in (
        agent.send_message(
            request_context,
            question
        )
    ):

        rich = getattr(
            component,
            "rich_component",
            None
        )

        if rich is None:
            continue

        # ----------------------------------------------------
        # Answer text
        # ----------------------------------------------------

        if hasattr(
            rich,
            "content"
        ):

            if rich.content:

                final_text = (
                    rich.content
                )

        # ----------------------------------------------------
        # Table rows
        # ----------------------------------------------------

        if hasattr(
            rich,
            "rows"
        ):

            if rich.rows:

                table_rows = (
                    rich.rows
                )

    return (
        final_text,
        table_rows
    )


# ============================================================
# 11. CHART HELPERS
# ============================================================

def clean_column_name(
    column
):
    """
    Converts database-style column names into
    readable chart labels.
    """

    return (
        str(column)
        .replace("_", " ")
        .replace("-", " ")
        .title()
    )


def looks_like_date_column(
    series
):
    """
    Determines whether a column appears to contain
    dates/timestamps.
    """

    if pd.api.types.is_datetime64_any_dtype(
        series
    ):
        return True

    if series.dtype == object:

        try:

            converted = pd.to_datetime(
                series,
                errors="coerce"
            )

            valid_ratio = (
                converted.notna().mean()
            )

            return valid_ratio >= 0.8

        except Exception:

            return False

    return False


def display_chart(df):
    """
    Automatically selects an appropriate chart based
    on the structure of the SQL result.

    Supported:
        - Single number -> metric
        - Category + number -> bar chart
        - Date + number -> line chart
        - Two numeric columns -> scatter chart
        - Category + multiple numbers -> line chart
        - Yes/no/small category distributions -> pie chart
    """

    if df is None:
        return

    if df.empty:
        return

    # --------------------------------------------------------
    # Identify numeric columns
    # --------------------------------------------------------

    numeric_cols = (
        df
        .select_dtypes(
            include="number"
        )
        .columns
        .tolist()
    )

    # --------------------------------------------------------
    # Identify non-numeric columns
    # --------------------------------------------------------

    non_numeric_cols = [
        col
        for col in df.columns
        if col not in numeric_cols
    ]

    # ========================================================
    # CASE 1
    # Single numeric result
    # ========================================================

    if (
        len(df) == 1
        and len(numeric_cols) == 1
    ):

        col = numeric_cols[0]

        st.subheader(
            "Result"
        )

        st.metric(
            label=clean_column_name(
                col
            ),
            value=f"{df[col].iloc[0]:,}"
        )

        return

    # ========================================================
    # CASE 2
    # Date/time + one numeric
    #
    # Example:
    #
    # date       | users
    # 2026-01-01 | 100
    # 2026-02-01 | 120
    #
    # -> LINE CHART
    # ========================================================

    if (
        len(non_numeric_cols) >= 1
        and len(numeric_cols) == 1
    ):

        x_col = non_numeric_cols[0]
        y_col = numeric_cols[0]

        if looks_like_date_column(
            df[x_col]
        ):

            chart_df = df[
                [x_col, y_col]
            ].copy()

            chart_df[x_col] = (
                pd.to_datetime(
                    chart_df[x_col],
                    errors="coerce"
                )
            )

            chart_df = chart_df.dropna(
                subset=[x_col]
            )

            chart_df = chart_df.sort_values(
                x_col
            )

            st.subheader(
                "Trend"
            )

            fig = px.line(
                chart_df,
                x=x_col,
                y=y_col,
                markers=True,
                title=(
                    f"{clean_column_name(y_col)} "
                    f"over time"
                ),
                labels={
                    x_col: clean_column_name(
                        x_col
                    ),
                    y_col: clean_column_name(
                        y_col
                    ),
                },
            )

            fig.update_layout(
                xaxis_title=clean_column_name(
                    x_col
                ),
                yaxis_title=clean_column_name(
                    y_col
                ),
                hovermode="x unified",
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

            return

    # ========================================================
    # CASE 3
    # Yes/no/small category distribution
    #
    # Example:
    #
    # answer | user_count
    # yes    | 350
    # no     | 120
    #
    # -> PIE CHART
    # ========================================================

    if (
        len(non_numeric_cols) >= 1
        and len(numeric_cols) == 1
        and 2 <= len(df) <= 5
    ):

        x_col = non_numeric_cols[0]
        y_col = numeric_cols[0]

        categories = (
            df[x_col]
            .astype(str)
            .str.lower()
            .tolist()
        )

        simple_categories = {
            "yes",
            "no",
            "true",
            "false",
            "male",
            "female",
        }

        if all(
            category in simple_categories
            for category in categories
        ):

            st.subheader(
                "Distribution"
            )

            fig = px.pie(
                df,
                names=x_col,
                values=y_col,
                title=(
                    f"{clean_column_name(y_col)} "
                    "by "
                    f"{clean_column_name(x_col)}"
                ),
            )

            st.plotly_chart(
                fig,
                use_container_width=True
            )

            return

    # ========================================================
    # CASE 4
    # Category + one numeric
    #
    # Example:
    #
    # profession | user_count
    # Doctor     | 120
    # Nurse      | 250
    #
    # -> BAR CHART
    # ========================================================

    if (
        len(non_numeric_cols) >= 1
        and len(numeric_cols) == 1
    ):

        x_col = non_numeric_cols[0]
        y_col = numeric_cols[0]

        chart_df = df[
            [x_col, y_col]
        ].copy()

        chart_df[x_col] = (
            chart_df[x_col]
            .astype(str)
        )

        chart_df = chart_df.sort_values(
            y_col,
            ascending=True
        )

        st.subheader(
            "Comparison"
        )

        # Horizontal bar chart for many categories
        if len(chart_df) > 6:

            fig = px.bar(
                chart_df,
                x=y_col,
                y=x_col,
                orientation="h",
                title=(
                    f"{clean_column_name(y_col)} "
                    f"by "
                    f"{clean_column_name(x_col)}"
                ),
                labels={
                    x_col: clean_column_name(
                        x_col
                    ),
                    y_col: clean_column_name(
                        y_col
                    ),
                },
            )

            fig.update_layout(
                xaxis_title=clean_column_name(
                    y_col
                ),
                yaxis_title=clean_column_name(
                    x_col
                ),
            )

        else:

            fig = px.bar(
                chart_df,
                x=x_col,
                y=y_col,
                title=(
                    f"{clean_column_name(y_col)} "
                    f"by "
                    f"{clean_column_name(x_col)}"
                ),
                labels={
                    x_col: clean_column_name(
                        x_col
                    ),
                    y_col: clean_column_name(
                        y_col
                    ),
                },
            )

            fig.update_layout(
                xaxis_title=clean_column_name(
                    x_col
                ),
                yaxis_title=clean_column_name(
                    y_col
                ),
            )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

        return

    # ========================================================
    # CASE 5
    # Two numeric columns
    #
    # Example:
    #
    # age | user_count
    # 20  | 100
    # 30  | 200
    #
    # -> SCATTER
    # ========================================================

    if (
        len(numeric_cols) == 2
        and len(df) > 1
    ):

        x_col = numeric_cols[0]
        y_col = numeric_cols[1]

        st.subheader(
            "Relationship"
        )

        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            title=(
                f"{clean_column_name(y_col)} "
                f"vs "
                f"{clean_column_name(x_col)}"
            ),
            labels={
                x_col: clean_column_name(
                    x_col
                ),
                y_col: clean_column_name(
                    y_col
                ),
            },
        )

        fig.update_layout(
            xaxis_title=clean_column_name(
                x_col
            ),
            yaxis_title=clean_column_name(
                y_col
            ),
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

        return

    # ========================================================
    # CASE 6
    # Category + multiple numeric columns
    #
    # -> LINE CHART
    # ========================================================

    if (
        len(non_numeric_cols) >= 1
        and len(numeric_cols) >= 2
    ):

        x_col = non_numeric_cols[0]

        chart_df = df[
            [x_col] + numeric_cols
        ].copy()

        st.subheader(
            "Trend"
        )

        fig = px.line(
            chart_df,
            x=x_col,
            y=numeric_cols,
            markers=True,
            title=(
                f"Measures by "
                f"{clean_column_name(x_col)}"
            ),
            labels={
                x_col: clean_column_name(
                    x_col
                )
            },
        )

        fig.update_layout(
            xaxis_title=clean_column_name(
                x_col
            ),
            yaxis_title="Value",
            hovermode="x unified",
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

        return

    # ========================================================
    # CASE 7
    # Multiple numeric columns without category
    # ========================================================

    if len(numeric_cols) >= 2:

        st.subheader(
            "Data visualization"
        )

        fig = px.line(
            df,
            y=numeric_cols,
            markers=True,
            title="Numeric results",
        )

        fig.update_layout(
            xaxis_title="Row",
            yaxis_title="Value",
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )

        return


# ============================================================
# 12. STREAMLIT PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Ask Your Data",
    page_icon="📊",
    layout="centered",
)


# ============================================================
# 13. UI
# ============================================================

st.title(
    "📊 Ask Your Data"
)

st.write(
    "Ask questions about your PostgreSQL "
    "database using natural language."
)


# ============================================================
# 14. CREATE AGENT
# ============================================================

try:

    agent = get_agent()

except Exception as exc:

    st.error(
        f"Failed to initialize Vanna: {exc}"
    )

    st.stop()


# ============================================================
# 15. QUESTION INPUT
# ============================================================

question = st.text_input(
    "Ask a question",
    placeholder=(
        "Example: How many users said yes "
        "to the shared phone question?"
    ),
)


# ============================================================
# 16. ASK BUTTON
# ============================================================

if (
    st.button(
        "Ask",
        type="primary"
    )
    and question
):

    with st.spinner(
        "Vanna is thinking..."
    ):

        try:

            answer_text, rows = (
                run_async(
                    ask_agent(
                        agent,
                        question
                    )
                )
            )

        except Exception as exc:

            st.error(
                f"Error while asking Vanna: "
                f"{exc}"
            )

            st.exception(exc)

            st.stop()

    # ========================================================
    # ANSWER
    # ========================================================

    if answer_text:

        st.subheader(
            "Answer"
        )

        st.markdown(
            answer_text
        )

    # ========================================================
    # DATA TABLE
    # ========================================================

    if rows:

        st.subheader(
            "Data"
        )

        try:

            df = pd.DataFrame(
                rows
            )

            st.dataframe(
                df,
                use_container_width=True
            )

        except Exception as exc:

            st.warning(
                f"Could not display table: "
                f"{exc}"
            )

            df = None

        # ====================================================
        # VISUALIZATION
        # ====================================================

        if (
            df is not None
            and not df.empty
        ):

            display_chart(
                df
            )

    # ========================================================
    # NO DISPLAYABLE RESULT
    # ========================================================

    elif not answer_text:

        st.info(
            "Vanna did not return a "
            "displayable result."
        )

