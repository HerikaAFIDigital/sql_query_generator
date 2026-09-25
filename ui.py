# import asyncio
# import os

# import pandas as pd
# import psycopg2
# import streamlit as st
# import plotly.express as px

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

# try:

#     asyncio.get_event_loop()

# except RuntimeError:

#     asyncio.set_event_loop(
#         asyncio.new_event_loop()
#     )


# # ============================================================
# # 2. LOAD ENVIRONMENT VARIABLES
# # ============================================================

# load_dotenv()


# # ============================================================
# # 3. POSTGRES CONFIGURATION
# # ============================================================

# PG_KWARGS = dict(
#     host=os.getenv(
#         "POSTGRES_HOST",
#         "localhost"
#     ),
#     port=int(
#         os.getenv(
#             "POSTGRES_PORT",
#             "5432"
#         )
#     ),
#     database=os.getenv(
#         "POSTGRES_DATABASE",
#         "vanna_demo"
#     ),
#     user=os.getenv(
#         "POSTGRES_USER",
#         "vanna_readonly"
#     ),
#     password=os.getenv(
#         "POSTGRES_PASSWORD"
#     ),
# )


# # ============================================================
# # 4. KNOWN TABLES
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

# class SchemaEnhancer(
#     LlmContextEnhancer
# ):

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

#         if self._cache is not None:
#             return self._cache

#         conn = None
#         cur = None

#         try:

#             conn = psycopg2.connect(
#                 **self.pg_kwargs
#             )

#             cur = conn.cursor()

#             lines = []

#             for table in self.tables:

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
#                         "(no columns found — "
#                         "check the table name)"
#                     )

#                     continue

#                 col_parts = []

#                 for name, dtype in cols:

#                     part = (
#                         f"{name} ({dtype})"
#                     )

#                     if dtype in TEXT_TYPES:

#                         cur.execute(
#                             f'''
#                             SELECT COUNT(DISTINCT "{name}")
#                             FROM "{table}"
#                             '''
#                         )

#                         row = cur.fetchone()
#                         distinct_count = (
#                             row[0] if row is not None else None
#                         )

#                         if (
#                             distinct_count is not None
#                             and
#                             0 < distinct_count
#                             <= self.enum_threshold
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
#                                 f" [actual values: "
#                                 f"{values_str}]"
#                             )

#                     col_parts.append(
#                         part
#                     )

#                 lines.append(
#                     f"- {table}("
#                     f"{', '.join(col_parts)}"
#                     f")"
#                 )

#             self._cache = (
#                 "\n".join(lines)
#             )

#         except Exception as exc:

#             self._cache = (
#                 f"(schema lookup failed: "
#                 f"{exc})"
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

#         schema = (
#             self._load_schema()
#         )

#         return (
#             system_prompt

#             + "\n\n"
#             + "## Database schema "
#             + "(this is the ONLY schema "
#               "that exists)\n"

#             + schema

#             + "\n\n"
#             + "## Important notes on this schema\n"

#             + (
#                 "- `onboarding_events` stores "
#                 "ONE ROW PER QUESTION a user "
#                 "(`profile_id`) answered.\n"
#             )

#             + (
#                 "- Despite the column names, "
#                 "`question_id` holds the FULL TEXT "
#                 "of the question "
#                 "(not a numeric id), and "
#                 "`answer_id` holds the literal "
#                 "answer text "
#                 "(not a numeric id).\n"
#             )

#             + (
#                 "- To count how many users gave "
#                 "a specific answer to a specific "
#                 "question, filter with an exact "
#                 "string match on BOTH "
#                 "`question_id` and `answer_id`, "
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
#                 "- Only use the exact "
#                 "question/answer strings "
#                 "listed in the schema above — "
#                 "never paraphrase or guess "
#                 "the wording.\n"
#             )

#             + (
#                 "- MANDATORY: You MUST execute the SQL query using the `run_sql` tool "
#                 "for EVERY question so that full data rows and interactive charts are generated. "
#                 "Never answer without running `run_sql`.\n"
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

# class SimpleUserResolver(
#     UserResolver
# ):

#     async def resolve_user(
#         self,
#         request_context
#     ):

#         return User(
#             id="local-user",
#             username="local-user",
#             group_memberships=["user"],
#         )


# # ============================================================
# # 8. CREATE VANNA AGENT
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

#     raw_port = PG_KWARGS.get("port")
#     port = int(raw_port) if raw_port is not None else 5432
#     raw_pwd = PG_KWARGS.get("password")
#     password = str(raw_pwd) if raw_pwd is not None else None

#     db = PostgresRunner(
#         host=str(PG_KWARGS.get("host", "localhost")),
#         port=port,
#         database=str(PG_KWARGS.get("database", "vanna_demo")),
#         user=str(PG_KWARGS.get("user", "vanna_readonly")),
#         password=password,
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
# # 9. ASYNC HELPER
# # ============================================================

# def run_async(coro):

#     try:

#         loop = asyncio.get_event_loop()

#     except RuntimeError:

#         loop = asyncio.new_event_loop()

#         asyncio.set_event_loop(
#             loop
#         )

#     return loop.run_until_complete(
#         coro
#     )


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

#     table_rows: list = []  # keep the result with the most rows

#     async for component in (
#         agent.send_message(
#             request_context,
#             question
#         )
#     ):

#         rich = getattr(
#             component,
#             "rich_component",
#             None
#         )

#         simple = getattr(
#             component,
#             "simple_component",
#             None
#         )

#         # ----------------------------------------------------
#         # Answer text
#         # ----------------------------------------------------

#         if rich is not None and hasattr(rich, "content") and rich.content:
#             final_text = rich.content
#         elif simple is not None and hasattr(simple, "text") and simple.text:
#             if not final_text:
#                 final_text = simple.text

#         # ----------------------------------------------------
#         # Table rows – always keep the result with the most rows
#         # ----------------------------------------------------

#         candidate_rows = None
#         if rich is not None and hasattr(rich, "rows") and rich.rows:
#             candidate_rows = rich.rows
#         elif hasattr(component, "rows") and component.rows:
#             candidate_rows = component.rows

#         if candidate_rows is not None:
#             # Replace current result only if the new one is larger
#             if table_rows is None or len(candidate_rows) > len(table_rows):
#                 table_rows = candidate_rows

#     # --------------------------------------------------------
#     # Fallback: If no rows extracted, check if answer contains SQL
#     # --------------------------------------------------------
#     if not table_rows and final_text:
#         import re
#         sql_match = re.search(r"```(?:sql)?\s*(SELECT\b[\s\S]+?)\s*```", final_text, re.IGNORECASE)
#         if not sql_match:
#             sql_match = re.search(r"\b(SELECT\s+[\s\S]+?FROM\s+[\s\S]+?;)", final_text, re.IGNORECASE)
#         if sql_match:
#             sql_query = sql_match.group(1).strip().rstrip(";")
#             try:
#                 _fb_port_raw = PG_KWARGS.get("port", 5432)
#                 _fb_port: int = int(_fb_port_raw) if _fb_port_raw is not None else 5432
#                 conn = psycopg2.connect(
#                     host=str(PG_KWARGS.get("host", "localhost")),
#                     port=_fb_port,
#                     database=str(PG_KWARGS.get("database", "vanna_demo")),
#                     user=str(PG_KWARGS.get("user", "vanna_readonly")),
#                     password=str(PG_KWARGS.get("password")) if PG_KWARGS.get("password") else None,
#                 )
#                 cur = conn.cursor()
#                 cur.execute(sql_query)
#                 if cur.description:
#                     col_names = [d[0] for d in cur.description]
#                     fetched = cur.fetchall()
#                     table_rows = [dict(zip(col_names, row)) for row in fetched]
#                 cur.close()
#                 conn.close()
#             except Exception:
#                 pass

#     return (
#         final_text,
#         table_rows
#     )


# # ============================================================
# # 11. CHART HELPERS
# # ============================================================

# def clean_column_name(
#     column
# ):
#     """
#     Converts database-style column names into
#     readable chart labels.
#     """

#     return (
#         str(column)
#         .replace("_", " ")
#         .replace("-", " ")
#         .title()
#     )


# def looks_like_date_column(
#     series
# ):
#     """
#     Determines whether a column appears to contain
#     dates/timestamps.
#     """

#     if pd.api.types.is_datetime64_any_dtype(
#         series
#     ):
#         return True

#     if series.dtype == object:

#         try:

#             converted = pd.to_datetime(
#                 series,
#                 errors="coerce"
#             )

#             valid_ratio = (
#                 converted.notna().mean()
#             )

#             return valid_ratio >= 0.8

#         except Exception:

#             return False

#     return False


# def display_chart(df):
#     """
#     Intelligently determines and displays the best chart for the SQL result,
#     providing interactive chart type switching and axis customization.
#     """
#     if df is None or df.empty:
#         return

#     chart_df = df.copy()

#     # --------------------------------------------------------
#     # 1. Coerce object columns that are mostly numeric
#     # --------------------------------------------------------
#     for col in chart_df.columns:
#         if chart_df[col].dtype == object:
#             converted = pd.to_numeric(chart_df[col], errors="coerce")
#             if pd.Series(converted).notna().mean() >= 0.8:
#                 chart_df[col] = converted

#     # --------------------------------------------------------
#     # 2. Classify columns
#     # --------------------------------------------------------
#     numeric_cols = chart_df.select_dtypes(include="number").columns.tolist()
#     all_cols = chart_df.columns.tolist()
#     non_numeric_cols = [col for col in all_cols if col not in numeric_cols]

#     # Detect date/time columns among non-numeric
#     date_cols = [
#         col for col in non_numeric_cols
#         if looks_like_date_column(chart_df[col])
#     ]

#     # --------------------------------------------------------
#     # 3. Case: Single numeric value (KPI Metric Card + Bar Graph)
#     # --------------------------------------------------------
#     if len(chart_df) == 1 and len(numeric_cols) == 1 and len(non_numeric_cols) == 0:
#         val = chart_df[numeric_cols[0]].iloc[0]
#         col_label = clean_column_name(numeric_cols[0])
        
#         st.subheader("Key Metric")
#         if isinstance(val, float):
#             formatted_val = f"{val:,.2f}".rstrip("0").rstrip(".")
#         else:
#             formatted_val = f"{val:,}"
#         st.metric(label=col_label, value=formatted_val)

#         st.subheader("📊 Visualization")
#         bar_df = pd.DataFrame({
#             "Metric": [col_label],
#             "Value": [val]
#         })
#         fig = px.bar(
#             bar_df,
#             x="Metric",
#             y="Value",
#             text="Value",
#             title=f"{col_label} Summary",
#             color_discrete_sequence=px.colors.qualitative.Prism,
#         )
#         fig.update_traces(textposition="outside")
#         fig.update_layout(
#             template="plotly_white",
#             margin=dict(l=20, r=20, t=50, b=20),
#             yaxis_title=col_label,
#             xaxis_title="",
#         )
#         st.plotly_chart(fig, use_container_width=True)
#         return

#     # --------------------------------------------------------
#     # 3b. Case: Purely text/category data (Frequency Bar Graph)
#     # --------------------------------------------------------
#     if not numeric_cols:
#         if non_numeric_cols:
#             cat_col = non_numeric_cols[0]
#             cat_counts = chart_df[cat_col].value_counts().reset_index()
#             cat_counts.columns = [cat_col, "Count"]
#             st.subheader("📊 Category Frequency Chart")
#             st.caption(f"Count of occurrences for each `{clean_column_name(cat_col)}`")
#             fig = px.bar(
#                 cat_counts,
#                 x=cat_col,
#                 y="Count",
#                 text="Count",
#                 color_discrete_sequence=px.colors.qualitative.Prism,
#                 title=f"Occurrences by {clean_column_name(cat_col)}",
#             )
#             fig.update_traces(textposition="outside")
#             fig.update_layout(
#                 template="plotly_white",
#                 margin=dict(l=20, r=20, t=50, b=20),
#                 xaxis_title=clean_column_name(cat_col),
#                 yaxis_title="Count",
#             )
#             st.plotly_chart(fig, use_container_width=True)
#             return
#         else:
#             st.info("ℹ️ No data available to visualize.")
#             return

#     # --------------------------------------------------------
#     # 4. Smart Auto-Detection Heuristic
#     # --------------------------------------------------------
#     # X-axis: always prefer a category/text column (dimension)
#     # Y-axis: always prefer the first numeric column (measure)
#     # This ensures e.g. answer_id → X and count → Y (both 'yes' and 'no'
#     # appear as separate bars on the same chart)
#     if non_numeric_cols:
#         default_x = non_numeric_cols[0]
#         default_y = numeric_cols[0]  # guaranteed to exist (checked above)
#     else:
#         # All numeric: use index order
#         default_x = numeric_cols[0]
#         default_y = numeric_cols[1] if len(numeric_cols) > 1 else numeric_cols[0]

#     recommended_type = "Bar"
#     reason = ""

#     if date_cols:
#         default_x = date_cols[0]
#         default_y = numeric_cols[0]
#         recommended_type = "Line"
#         reason = f"Trend detected over time ({clean_column_name(default_x)})"
#     elif non_numeric_cols and 2 <= len(chart_df) <= 7:
#         recommended_type = "Donut / Pie"
#         reason = f"Category distribution ({len(chart_df)} groups for '{clean_column_name(default_x)}')"
#     elif non_numeric_cols:
#         recommended_type = "Bar"
#         reason = f"Category comparison — X: {clean_column_name(default_x)}, Y: {clean_column_name(default_y)}"
#     elif len(numeric_cols) == 2:
#         recommended_type = "Scatter"
#         reason = f"Relationship between {clean_column_name(numeric_cols[0])} and {clean_column_name(numeric_cols[1])}"
#     elif len(numeric_cols) >= 2:
#         recommended_type = "Line"
#         reason = "Multiple numeric measures"

#     st.subheader("📊 Visualization")

#     # --------------------------------------------------------
#     # 5. Interactive Chart Controls
#     # --------------------------------------------------------
#     col_type, col_space = st.columns([3, 1])
#     with col_type:
#         chart_options = [
#             "Auto",
#             "Bar",
#             "Donut / Pie",
#             "Line",
#             "Area",
#         ]
#         if recommended_type == "Scatter":
#             chart_options.append("Scatter")

#         selected_view = st.segmented_control(
#             "Select Chart View",
#             options=chart_options,
#             default="Auto",
#             key="chart_view_selector",
#         )

#     active_type = (
#         recommended_type
#         if (selected_view is None or selected_view == "Auto")
#         else selected_view
#     )

#     # Auto-detection badge
#     st.caption(
#         f"💡 **Recommended View:** `{recommended_type}` | X-Axis = `{clean_column_name(default_x)}` | "
#         f"Y-Axis = `{clean_column_name(default_y)}` — *{reason}*"
#     )

#     # Customization expander
#     with st.expander("⚙️ Customize Graph (Axes & Settings)", expanded=False):
#         c1, c2, c3 = st.columns(3)
#         with c1:
#             chosen_x = st.selectbox(
#                 "X-Axis (Dimension / Category)",
#                 options=all_cols,
#                 index=all_cols.index(default_x) if default_x in all_cols else 0,
#                 key="chart_custom_x",
#             )
#         with c2:
#             chosen_y = st.selectbox(
#                 "Y-Axis (Metric / Value)",
#                 options=numeric_cols,
#                 index=numeric_cols.index(default_y) if default_y in numeric_cols else 0,
#                 key="chart_custom_y",
#             )
#         with c3:
#             show_labels = st.checkbox(
#                 "Show Values on Chart",
#                 value=True,
#                 key="chart_show_labels",
#             )
#             sort_values = st.checkbox(
#                 "Sort by Value (High to Low)",
#                 value=(active_type == "Bar"),
#                 key="chart_sort_values",
#             )

#     plot_df = chart_df.copy()

#     # Apply sorting if requested
#     if sort_values and chosen_y in plot_df.columns:
#         plot_df = plot_df.sort_values(chosen_y, ascending=True)

#     # --------------------------------------------------------
#     # 6. Render the Chosen Chart Type
#     # --------------------------------------------------------
#     fig = None
#     palette = px.colors.qualitative.Prism

#     if active_type == "Donut / Pie":
#         fig = px.pie(
#             plot_df,
#             names=chosen_x,
#             values=chosen_y,
#             hole=0.45,
#             color_discrete_sequence=palette,
#             title=f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}",
#         )
#         fig.update_traces(
#             textposition="inside",
#             textinfo="percent+label" if show_labels else "percent",
#             hovertemplate="<b>%{label}</b><br>Value: %{value:,}<br>Share: %{percent}<extra></extra>",
#         )

#     elif active_type == "Bar":
#         is_horizontal = len(plot_df) > 8
#         if is_horizontal:
#             fig = px.bar(
#                 plot_df,
#                 x=chosen_y,
#                 y=chosen_x,
#                 orientation="h",
#                 text_auto=True if show_labels else False,
#                 color_discrete_sequence=palette,
#                 title=f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}",
#                 labels={
#                     chosen_x: clean_column_name(chosen_x),
#                     chosen_y: clean_column_name(chosen_y),
#                 },
#             )
#             fig.update_layout(
#                 xaxis_title=clean_column_name(chosen_y),
#                 yaxis_title=clean_column_name(chosen_x),
#             )
#         else:
#             fig = px.bar(
#                 plot_df,
#                 x=chosen_x,
#                 y=chosen_y,
#                 text_auto=True if show_labels else False,
#                 color_discrete_sequence=palette,
#                 title=f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}",
#                 labels={
#                     chosen_x: clean_column_name(chosen_x),
#                     chosen_y: clean_column_name(chosen_y),
#                 },
#             )
#             fig.update_layout(
#                 xaxis_title=clean_column_name(chosen_x),
#                 yaxis_title=clean_column_name(chosen_y),
#             )

#     elif active_type == "Line":
#         if chosen_x in date_cols:
#             plot_df[chosen_x] = pd.to_datetime(plot_df[chosen_x], errors="coerce")
#             plot_df = plot_df.dropna(subset=[chosen_x]).sort_values(chosen_x)

#         fig = px.line(
#             plot_df,
#             x=chosen_x,
#             y=chosen_y,
#             markers=True,
#             color_discrete_sequence=palette,
#             title=f"{clean_column_name(chosen_y)} over {clean_column_name(chosen_x)}",
#             labels={
#                 chosen_x: clean_column_name(chosen_x),
#                 chosen_y: clean_column_name(chosen_y),
#             },
#         )
#         fig.update_layout(hovermode="x unified")

#     elif active_type == "Area":
#         if chosen_x in date_cols:
#             plot_df[chosen_x] = pd.to_datetime(plot_df[chosen_x], errors="coerce")
#             plot_df = plot_df.dropna(subset=[chosen_x]).sort_values(chosen_x)

#         fig = px.area(
#             plot_df,
#             x=chosen_x,
#             y=chosen_y,
#             markers=True,
#             color_discrete_sequence=palette,
#             title=f"{clean_column_name(chosen_y)} Area over {clean_column_name(chosen_x)}",
#             labels={
#                 chosen_x: clean_column_name(chosen_x),
#                 chosen_y: clean_column_name(chosen_y),
#             },
#         )
#         fig.update_layout(hovermode="x unified")

#     elif active_type == "Scatter":
#         fig = px.scatter(
#             plot_df,
#             x=chosen_x,
#             y=chosen_y,
#             color_discrete_sequence=palette,
#             title=f"{clean_column_name(chosen_y)} vs {clean_column_name(chosen_x)}",
#             labels={
#                 chosen_x: clean_column_name(chosen_x),
#                 chosen_y: clean_column_name(chosen_y),
#             },
#         )

#     if fig is None:
#         fig = px.bar(
#             plot_df,
#             x=chosen_x,
#             y=chosen_y,
#             text_auto=True if show_labels else False,
#             color_discrete_sequence=palette,
#             title=f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}",
#         )

#     fig.update_layout(
#         template="plotly_white",
#         margin=dict(l=20, r=20, t=50, b=20),
#         title_font=dict(size=16),
#     )
#     st.plotly_chart(fig, use_container_width=True)


# # ============================================================
# # 12. STREAMLIT PAGE CONFIG
# # ============================================================

# st.set_page_config(
#     page_title="Ask Your Data",
#     page_icon="📊",
#     layout="wide",
# )


# # ============================================================
# # 13. UI HEADER
# # ============================================================

# st.title("📊 Ask Your Data")
# st.write(
#     "Ask questions about your PostgreSQL database using natural language. "
#     "Answers and query results are automatically transformed into interactive charts."
# )


# # ============================================================
# # 14. CREATE AGENT
# # ============================================================

# try:
#     agent = get_agent()
# except Exception as exc:
#     st.error(f"Failed to initialize Vanna: {exc}")
#     st.stop()


# # ============================================================
# # 15. QUESTION INPUT & FORM
# # ============================================================

# if "last_result" not in st.session_state:
#     st.session_state["last_result"] = None

# with st.form("ask_form", clear_on_submit=False):
#     question = st.text_input(
#         "Ask a question",
#         placeholder="Example: How many users said yes to the shared phone question?",
#     )
#     submitted = st.form_submit_button("Ask", type="primary")

# if submitted and question:
#     with st.spinner("Vanna is thinking..."):
#         try:
#             answer_text, rows = run_async(
#                 ask_agent(
#                     agent,
#                     question
#                 )
#             )
#             st.session_state["last_result"] = {
#                 "question": question,
#                 "answer_text": answer_text,
#                 "rows": rows,
#             }
#         except Exception as exc:
#             st.error(f"Error while asking Vanna: {exc}")
#             st.exception(exc)
#             st.stop()


# # ============================================================
# # 16. DISPLAY RESULTS (PERSISTENT ACROSS CHART INTERACTIONS)
# # ============================================================

# last_result = st.session_state.get("last_result")

# if last_result is not None:
#     answer_text = last_result.get("answer_text")
#     rows = last_result.get("rows")

#     if answer_text:
#         st.subheader("Answer")
#         st.markdown(answer_text)

#     if rows:
#         st.subheader("Data Table")
#         try:
#             df = pd.DataFrame(rows)
#             st.dataframe(df, use_container_width=True)
#         except Exception as exc:
#             st.warning(f"Could not display table: {exc}")
#             df = None

#         if df is not None and not df.empty:
#             display_chart(df)

#     elif not answer_text:
#         st.info("Vanna did not return a displayable result.")



import asyncio
import json
import os
import sqlite3
import time

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
    asyncio.set_event_loop(asyncio.new_event_loop())


# ============================================================
# 2. LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# 3. POSTGRES CONFIGURATION
# ============================================================

PG_KWARGS = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
    user=os.getenv("POSTGRES_USER", "vanna_readonly"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

KNOWN_TABLES = ["onboarding_events", "app_lifecycle_events", "download_category_events", "download_language_events", "interaction_events", "terms_conditions_events"]

TEXT_TYPES = ("text", "character varying", "varchar", "char", "character")

PALETTE = ["#6366F1", "#F59E0B", "#10B981", "#EF4444", "#06B6D4", "#EC4899", "#8B5CF6", "#84CC16"]


# ============================================================
# 4. PERSISTENT ANSWER CACHE (SQLite — survives restarts)
# ============================================================

CACHE_DB_PATH = "query_cache.db"
CACHE_TTL_SECONDS = 60 * 60 * 24 * 7  # answers older than 1 week are treated as stale; set to None to never expire


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
    # Don't cache empty/failed results.
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
# 5. SCHEMA ENHANCER
# ============================================================

class SchemaEnhancer(LlmContextEnhancer):

    TABLE_DESCRIPTIONS = {
        "onboarding_events": "one row per survey question a user (profile_id) answered during onboarding. question_id = full text of the question; answer_id = literal answer text.",
        "app_lifecycle_events": "one row per app install/open event per user, with device model, OS, app version, and language info.",
        "download_category_events": "one row per category-selection or download action on the content category screen. category_title = human-readable name of the category selected.",
        "download_language_events": "one row per language-selection or download action. autonym_script = human-readable language name.",
        "interaction_events": "general screen-view and UI-interaction events (e.g. viewing the onboarding screen or a modal).",
        "terms_conditions_events": "terms & conditions acceptance events; is_accepted = '1' means accepted.",
    }

    # Columns whose values are large JSON arrays or UUIDs — skip enum dump for these
    _SKIP_ENUM_COLUMNS = {
        "selected_categories", "selected_category_titles", "extra",
        "id", "session_id", "app_installation_id", "category_id",
        "language_id", "source_file", "ingested_at", "event_timestamp",
    }

    def __init__(self, pg_kwargs, tables, enum_threshold=30):
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
            conn = psycopg2.connect(**self.pg_kwargs)
            cur = conn.cursor()
            lines = []

            for table in self.tables:
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position
                """, (table,))
                cols = cur.fetchall()

                if not cols:
                    lines.append(f"- {table}: (no columns found — check the table name)")
                    continue

                col_parts = []
                for name, dtype in cols:
                    part = f"{name} ({dtype})"
                    if dtype in TEXT_TYPES and name not in self._SKIP_ENUM_COLUMNS:
                        cur.execute(f'SELECT COUNT(DISTINCT "{name}") FROM "{table}"')
                        row = cur.fetchone()
                        distinct_count = row[0] if row is not None else None
                        if distinct_count is not None and 0 < distinct_count <= self.enum_threshold:
                            cur.execute(f'SELECT DISTINCT "{name}" FROM "{table}" ORDER BY 1')
                            values = [str(r[0]) for r in cur.fetchall() if r[0] is not None]
                            values_str = ", ".join(f"'{v}'" for v in values)
                            part += f" [actual values: {values_str}]"
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

        table_descriptions = "\n".join(
            f"- `{name}`: {desc}"
            for name, desc in self.TABLE_DESCRIPTIONS.items()
            if name in self.tables
        )

        return (
            system_prompt
            + "\n\n## Database schema (these are the ONLY tables that exist)\n"
            + schema
            + "\n\n## What each table represents\n"
            + table_descriptions
            + "\n\n## Critical schema notes\n"

            # --- onboarding_events ---
            + "### onboarding_events\n"
            + "- profile_id is a text code like 'BWT-WIF-ABW-LAY-U1' (NOT an integer).\n"
            + "- question_id holds the FULL TEXT of the survey question (not a numeric id).\n"
            + "- answer_id holds the literal answer text (not a numeric id), e.g. 'yes', 'no', 'doctor'.\n"
            + "- event_time (timestamp) is the correct column for time-based filtering — NOT event_timestamp (which is a Unix-ms text string).\n"
            + "- ONE ROW = ONE question answered by one user. To count distinct users, always use COUNT(DISTINCT profile_id).\n"
            + "- Only use the exact question/answer strings shown in the schema above — never paraphrase.\n"

            # --- app_lifecycle_events ---
            + "\n### app_lifecycle_events\n"
            + "- event_type is always 'installed' in the current data.\n"
            + "- device_model, device_os, app_version, autonym_script contain device/app metadata.\n"
            + "- event_time is the correct timestamp column for time filtering.\n"
            + "- profile_id is a text code (same format as other tables).\n"

            # --- download_category_events ---
            + "\n### download_category_events\n"
            + "- event_type values: 'selected' (user toggled a category), 'downloaded' (user pressed download).\n"
            + "- status values: 'selected', 'completed'.\n"
            + "- category_title = human-readable category name (e.g. 'Infection prevention', 'Newborn health').\n"
            + "- To find which categories were downloaded, filter event_type = 'downloaded' and parse selected_category_titles, OR join on category_title where event_type = 'selected'.\n"
            + "- autonym_script = language name (e.g. 'Ethiopia - English', 'English').\n"
            + "- event_time is the correct timestamp column.\n"

            # --- download_language_events ---
            + "\n### download_language_events\n"
            + "- event_type values: 'selected', 'downloaded', 'cancelled'.\n"
            + "- status values: 'selected', 'completed', 'cancelled'.\n"
            + "- autonym_script = human-readable language name (e.g. 'Ethiopia - English', 'Bangladesh - Bangla').\n"
            + "- is_selective_download = '1' means download-without-videos was chosen.\n"
            + "- event_time is the correct timestamp column.\n"

            # --- interaction_events ---
            + "\n### interaction_events\n"
            + "- Tracks screen views and modal views during onboarding.\n"
            + "- event_type = 'viewed'; screen_name = 'onboarding_screen'.\n"
            + "- autonym_script = language in use during the interaction.\n"

            # --- terms_conditions_events ---
            + "\n### terms_conditions_events\n"
            + "- event_type = 'accepted' means user accepted terms & conditions.\n"
            + "- is_accepted = '1' for accepted events.\n"
            + "- autonym_script = language name during acceptance.\n"

            # --- cross-table joins ---
            + "\n## Joining tables\n"
            + "- Every table uses profile_id (text) as the shared user identifier.\n"
            + "- To correlate events across tables, JOIN ON profile_id. Example:\n"
            + "  SELECT COUNT(DISTINCT a.profile_id)\n"
            + "  FROM app_lifecycle_events a\n"
            + "  JOIN terms_conditions_events t ON a.profile_id = t.profile_id\n"
            + "  WHERE a.event_type = 'installed';\n"

            # --- query rules ---
            + "\n## Rules for writing queries\n"
            + "- Always use COUNT(DISTINCT profile_id) when counting users.\n"
            + "- For breakdowns ('by profession', 'yes vs no', 'per language'), use a single query with GROUP BY — NEVER multiple queries per value.\n"
            + "- Example of a breakdown:\n"
            + "  SELECT answer_id, COUNT(DISTINCT profile_id) AS users\n"
            + "  FROM onboarding_events\n"
            + "  WHERE question_id = 'Is this device a shared phone/tablet?'\n"
            + "  GROUP BY answer_id;\n"
            + "- For cross-question breakdowns within onboarding_events, self-join on profile_id:\n"
            + "  SELECT p.answer_id AS profession, d.answer_id AS shared_device, COUNT(DISTINCT p.profile_id) AS users\n"
            + "  FROM onboarding_events p\n"
            + "  JOIN onboarding_events d ON p.profile_id = d.profile_id\n"
            + "  WHERE p.question_id = 'What is your profession?'\n"
            + "    AND d.question_id = 'Is this device a shared phone/tablet?'\n"
            + "  GROUP BY p.answer_id, d.answer_id;\n"
            + "- Run exactly ONE run_sql call per user question (retry only on SQL error).\n"
            + "- You MUST execute the SQL using the run_sql tool for EVERY question. Never answer without running run_sql.\n"
            + "- NEVER use placeholder text like '[X]', '[value]', '[X seconds]', '[calculated]' in your answer. "
              "Always embed the EXACT numeric or text value returned by the SQL query into your response.\n"
            + "- After running run_sql and getting results, your answer MUST include the real numbers from those results. "
              "Example: say '8 minutes and 9 seconds (489 seconds)' NOT 'X minutes'.\n"
            + "- If the question asks for a duration, always present it in human-readable form: "
              "e.g. '8 min 9 sec' or '2 hours 5 min'. Never leave the unit as a raw epoch number without explanation.\n"

            # --- analytical reasoning ---
            + "\n## Analytical reasoning — deriving metrics from available data\n"
            + "NEVER refuse to answer a question just because an exact column for the metric does not exist.\n"
            + "These are real production event logs. Every event has an event_time timestamp. Use that to derive any time-based metric.\n"

            + "\n### Derived metric: time spent on app / session duration\n"
            + "There is no explicit 'duration' column, but you CAN derive it:\n"
            + "- A user's onboarding session spans from their FIRST event_time to their LAST event_time across ALL tables.\n"
            + "- Use a UNION ALL of all six tables to get every event_time for a user, then compute MAX - MIN.\n"
            + "- Example — user who spent the most time on app:\n"
            + "  WITH all_events AS (\n"
            + "    SELECT profile_id, event_time FROM onboarding_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM app_lifecycle_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM download_category_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM download_language_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM interaction_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM terms_conditions_events\n"
            + "  )\n"
            + "  SELECT profile_id,\n"
            + "         MIN(event_time) AS session_start,\n"
            + "         MAX(event_time) AS session_end,\n"
            + "         ROUND(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time)))) AS duration_seconds,\n"
            + "         ROUND(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) / 60.0, 2) AS duration_minutes,\n"
            + "         COUNT(*) AS total_events\n"
            + "  FROM all_events\n"
            + "  GROUP BY profile_id\n"
            + "  ORDER BY duration_seconds DESC\n"
            + "  LIMIT 1;\n"
            + "- The result will contain duration_seconds and duration_minutes. Use BOTH in your answer for clarity.\n"

            + "\n### Derived metric: number of steps / engagement depth\n"
            + "- Count total events (across all tables) per user to measure how many onboarding steps they went through.\n"
            + "- More events = more engaged / deeper into the onboarding funnel.\n"

            + "\n### Derived metric: onboarding funnel / completion\n"
            + "- A user who has rows in app_lifecycle_events AND terms_conditions_events AND onboarding_events has completed more steps.\n"
            + "- Use EXISTS or JOIN patterns across tables to identify which stage each user reached.\n"

            + "\n### Derived metric: most popular / least popular categories or languages\n"
            + "- For downloads: filter download_category_events WHERE event_type = 'downloaded', group by category_title or autonym_script.\n"
            + "- For language preferences: group download_language_events by autonym_script WHERE event_type = 'downloaded'.\n"

            + "\n### Derived metric: filtering by app version\n"
            + "- app_version is only in app_lifecycle_events. To filter other tables by app version, JOIN them to app_lifecycle_events ON profile_id.\n"
            + "- Example — users with version 4.0.0 who spent the most time on the app:\n"
            + "  WITH versioned_users AS (\n"
            + "    SELECT DISTINCT profile_id FROM app_lifecycle_events WHERE app_version = '4.0.0'\n"
            + "  ),\n"
            + "  all_events AS (\n"
            + "    SELECT profile_id, event_time FROM onboarding_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM app_lifecycle_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM download_category_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM download_language_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM interaction_events\n"
            + "    UNION ALL SELECT profile_id, event_time FROM terms_conditions_events\n"
            + "  )\n"
            + "  SELECT ae.profile_id,\n"
            + "         ROUND(EXTRACT(EPOCH FROM (MAX(ae.event_time) - MIN(ae.event_time)))) AS duration_seconds,\n"
            + "         ROUND(EXTRACT(EPOCH FROM (MAX(ae.event_time) - MIN(ae.event_time))) / 60.0, 2) AS duration_minutes,\n"
            + "         COUNT(*) AS total_events\n"
            + "  FROM all_events ae\n"
            + "  INNER JOIN versioned_users vu ON ae.profile_id = vu.profile_id\n"
            + "  GROUP BY ae.profile_id\n"
            + "  ORDER BY duration_seconds DESC\n"
            + "  LIMIT 1;\n"

            + "\n### General principle\n"
            + "- Before saying a metric cannot be computed, ask yourself: can I approximate it using event_time, COUNT(*), or by joining tables on profile_id?\n"
            + "- If yes — write the SQL and return real numbers. NEVER return a refusal when data-driven approximation is possible.\n"
            + "- Only say 'cannot be computed' if the required raw data is genuinely absent from ALL tables (e.g. GPS coordinates when none of the tables have lat/long values).\n"
            + "- Always label derived columns clearly (e.g. 'duration_seconds', 'duration_minutes') so the user knows it is inferred.\n"
            + "- CRITICAL: After running SQL, read the actual result row and embed the EXACT values into your answer. "
              "NEVER write placeholder text like '[X]', '[value]', '[calculated]', '[X seconds]', or 'X minutes'. "
              "If the query returned 489 seconds, your answer MUST say '489 seconds (approx. 8 minutes 9 seconds)'.\n"
            + "- Do NOT ask the user if they want the result in different units BEFORE giving the answer. "
              "Give the result first in both seconds AND minutes/hours. Follow-up questions are fine after.\n"
        )

    async def enhance_user_messages(self, messages, user):
        return messages

# ============================================================
# 6. USER RESOLVER
# ============================================================

class SimpleUserResolver(UserResolver):

    async def resolve_user(self, request_context):
        return User(id="local-user", username="local-user", group_memberships=["user"])


# ============================================================
# 7. CREATE VANNA AGENT
# ============================================================

@st.cache_resource
def get_agent():
    llm = OllamaLlmService(
        model=os.getenv("OLLAMA_MODEL", "qwen3:14b"),
        host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
        temperature=0.1,
    )

    db = PostgresRunner(**PG_KWARGS)

    tools = ToolRegistry()
    tools.register_local_tool(RunSqlTool(sql_runner=db), access_groups=["user"])

    schema_enhancer = SchemaEnhancer(PG_KWARGS, KNOWN_TABLES)

    return Agent(
        llm_service=llm,
        tool_registry=tools,
        user_resolver=SimpleUserResolver(),
        agent_memory=DemoAgentMemory(),
        llm_context_enhancer=schema_enhancer,
    )


# ============================================================
# 8. ASYNC HELPER
# ============================================================

def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)


# ============================================================
# 9. ASK VANNA
# ============================================================

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
        import re
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
# 10. CHART HELPERS
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
        title_font=dict(size=17, color="#111827"),
        margin=dict(l=30, r=30, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.03, xanchor="right", x=1, title=None),
        bargap=0.3,
        bargroupgap=0.12,
        xaxis=dict(showgrid=False, showline=True, linecolor="#E5E7EB"),
        yaxis=dict(showgrid=True, gridcolor="#F0F1F3", zeroline=False),
        hoverlabel=dict(bgcolor="white", font_size=13),
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
        st.subheader("Key metric")
        formatted_val = f"{val:,.2f}".rstrip("0").rstrip(".") if isinstance(val, float) else f"{val:,}"
        st.metric(label=col_label, value=formatted_val)

        st.subheader("Visualization")
        bar_df = pd.DataFrame({"Metric": [col_label], "Value": [val]})
        fig = px.bar(bar_df, x="Metric", y="Value", text="Value", color_discrete_sequence=PALETTE,
                     title=f"{col_label} summary")
        fig.update_traces(textposition="outside", marker_line_width=0)
        style_fig(fig)
        st.plotly_chart(fig, use_container_width=True)
        return

    if not numeric_cols:
        if category_cols:
            cat_col = category_cols[0]
            cat_counts = chart_df[cat_col].value_counts().reset_index()
            cat_counts.columns = [cat_col, "Count"]
            st.subheader("Category frequency")
            fig = px.bar(cat_counts, x=cat_col, y="Count", text="Count", color_discrete_sequence=PALETTE,
                         title=f"Occurrences by {clean_column_name(cat_col)}")
            fig.update_traces(textposition="outside", marker_line_width=0)
            style_fig(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available to visualize.")
        return

    if len(category_cols) == 2 and len(numeric_cols) == 1:
        counts = {c: chart_df[c].nunique() for c in category_cols}
        ordered = sorted(category_cols, key=lambda c: -counts[c])
        default_x, color_col = ordered[0], ordered[1]
        default_y = numeric_cols[0]
        recommended_type = "Bar"
        reason = f"{clean_column_name(default_x)} broken down by {clean_column_name(color_col)}"
    elif date_cols:
        default_x = date_cols[0]
        default_y = numeric_cols[0]
        color_col = category_cols[0] if category_cols else None
        recommended_type = "Line"
        reason = f"Trend over time ({clean_column_name(default_x)})"
    elif category_cols:
        default_x = category_cols[0]
        default_y = numeric_cols[0]
        color_col = None
        recommended_type = "Donut / Pie" if 2 <= len(chart_df) <= 7 else "Bar"
        reason = f"Category comparison — X: {clean_column_name(default_x)}, Y: {clean_column_name(default_y)}"
    elif len(numeric_cols) == 2:
        default_x, default_y = numeric_cols
        color_col = None
        recommended_type = "Scatter"
        reason = f"Relationship between {clean_column_name(numeric_cols[0])} and {clean_column_name(numeric_cols[1])}"
    else:
        default_x, default_y = numeric_cols[0], numeric_cols[-1]
        color_col = None
        recommended_type = "Line"
        reason = "Multiple numeric measures"

    st.subheader("Visualization")
    chart_options = ["Auto", "Bar", "Donut / Pie", "Line", "Area"]
    if recommended_type == "Scatter":
        chart_options.append("Scatter")
    selected_view = st.segmented_control("Select chart view", options=chart_options, default="Auto", key="chart_view_selector")
    active_type = recommended_type if selected_view in (None, "Auto") else selected_view

    st.caption(f"Recommended view: `{recommended_type}` — {reason}")

    with st.expander("Customize graph", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            chosen_x = st.selectbox("X-axis", options=all_cols,
                                     index=all_cols.index(default_x) if default_x in all_cols else 0,
                                     key="chart_custom_x")
        with c2:
            chosen_y = st.selectbox("Y-axis", options=numeric_cols,
                                     index=numeric_cols.index(default_y) if default_y in numeric_cols else 0,
                                     key="chart_custom_y")
        with c3:
            color_options = ["None"] + [c for c in category_cols if c != chosen_x]
            default_color_idx = color_options.index(color_col) if color_col in color_options else 0
            chosen_color_label = st.selectbox("Group / color by", options=color_options,
                                               index=default_color_idx, key="chart_custom_color")
            chosen_color = None if chosen_color_label == "None" else chosen_color_label
        with c4:
            show_labels = st.checkbox("Show values", value=True, key="chart_show_labels")
            sort_values = st.checkbox("Sort by value", value=(active_type == "Bar" and chosen_color is None),
                                       key="chart_sort_values")

    plot_df = chart_df.copy()
    if sort_values and chosen_color is None and chosen_y in plot_df.columns:
        plot_df = plot_df.sort_values(chosen_y, ascending=True)

    fig = None

    if active_type == "Donut / Pie":
        fig = px.pie(plot_df, names=chosen_x, values=chosen_y, hole=0.45, color_discrete_sequence=PALETTE,
                     title=f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}")
        fig.update_traces(textposition="inside",
                           textinfo="percent+label" if show_labels else "percent",
                           hovertemplate="<b>%{label}</b><br>Value: %{value:,}<br>Share: %{percent}<extra></extra>")

    elif active_type == "Bar":
        is_horizontal = len(plot_df) > 8 and chosen_color is None
        title = f"{clean_column_name(chosen_y)} by {clean_column_name(chosen_x)}"
        if chosen_color:
            title += f", grouped by {clean_column_name(chosen_color)}"
        labels = {chosen_x: clean_column_name(chosen_x), chosen_y: clean_column_name(chosen_y)}
        if chosen_color:
            labels[chosen_color] = clean_column_name(chosen_color)

        if is_horizontal:
            fig = px.bar(plot_df, x=chosen_y, y=chosen_x, orientation="h", color=chosen_color,
                         text_auto=True if show_labels else False, color_discrete_sequence=PALETTE,
                         title=title, labels=labels, barmode="group")
        else:
            fig = px.bar(plot_df, x=chosen_x, y=chosen_y, color=chosen_color,
                         text_auto=True if show_labels else False, color_discrete_sequence=PALETTE,
                         title=title, labels=labels, barmode="group")
        fig.update_traces(textposition="outside", marker_line_width=0)

    elif active_type == "Line":
        line_df = plot_df.copy()
        if chosen_x in date_cols:
            line_df[chosen_x] = pd.to_datetime(line_df[chosen_x], errors="coerce")
            line_df = line_df.dropna(subset=[chosen_x]).sort_values(chosen_x)
        fig = px.line(line_df, x=chosen_x, y=chosen_y, color=chosen_color, markers=True,
                      color_discrete_sequence=PALETTE,
                      title=f"{clean_column_name(chosen_y)} over {clean_column_name(chosen_x)}",
                      labels={chosen_x: clean_column_name(chosen_x), chosen_y: clean_column_name(chosen_y)})
        fig.update_layout(hovermode="x unified")

    elif active_type == "Area":
        area_df = plot_df.copy()
        if chosen_x in date_cols:
            area_df[chosen_x] = pd.to_datetime(area_df[chosen_x], errors="coerce")
            area_df = area_df.dropna(subset=[chosen_x]).sort_values(chosen_x)
        fig = px.area(area_df, x=chosen_x, y=chosen_y, color=chosen_color, markers=True,
                      color_discrete_sequence=PALETTE,
                      title=f"{clean_column_name(chosen_y)} area over {clean_column_name(chosen_x)}",
                      labels={chosen_x: clean_column_name(chosen_x), chosen_y: clean_column_name(chosen_y)})
        fig.update_layout(hovermode="x unified")

    elif active_type == "Scatter":
        fig = px.scatter(plot_df, x=chosen_x, y=chosen_y, color=chosen_color, color_discrete_sequence=PALETTE,
                         title=f"{clean_column_name(chosen_y)} vs {clean_column_name(chosen_x)}",
                         labels={chosen_x: clean_column_name(chosen_x), chosen_y: clean_column_name(chosen_y)})

    if fig is None:
        fig = px.bar(plot_df, x=chosen_x, y=chosen_y, color=chosen_color, color_discrete_sequence=PALETTE,
                     barmode="group")

    style_fig(fig)
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# 11. STREAMLIT PAGE
# ============================================================

st.set_page_config(page_title="Ask Your Data", page_icon="📊", layout="wide")
st.title("Ask your data")
st.write(
    "Ask questions about your PostgreSQL database using natural language. "
    "Answers and query results are automatically transformed into interactive charts."
)

with st.sidebar:
    st.subheader("Answer cache")
    cache_count, cache_hits = get_cache_stats()
    st.caption(f"{cache_count} question(s) cached, {cache_hits} cache hit(s) served")
    if st.button("Clear cache"):
        clear_cache()
        st.rerun()

try:
    agent = get_agent()
except Exception as exc:
    st.error(f"Failed to initialize Vanna: {exc}")
    st.stop()

if "last_result" not in st.session_state:
    st.session_state["last_result"] = None

with st.form("ask_form", clear_on_submit=False):
    question = st.text_input(
        "Ask a question",
        placeholder="Example: How many users said yes to the shared phone question?",
    )
    force_refresh = st.checkbox("Force refresh (ignore cache, re-run against the LLM)", value=False)
    submitted = st.form_submit_button("Ask", type="primary")

if submitted and question:
    cached = None if force_refresh else get_cached_answer(question)

    if cached is not None:
        answer_text, rows = cached
        st.session_state["last_result"] = {
            "question": question,
            "answer_text": answer_text,
            "rows": rows,
            "from_cache": True,
        }
    else:
        with st.spinner("Vanna is thinking..."):
            try:
                answer_text, rows = run_async(ask_agent(agent, question))
                store_cached_answer(question, answer_text, rows)
                st.session_state["last_result"] = {
                    "question": question,
                    "answer_text": answer_text,
                    "rows": rows,
                    "from_cache": False,
                }
            except Exception as exc:
                st.error(f"Error while asking Vanna: {exc}")
                st.exception(exc)
                st.stop()

last_result = st.session_state.get("last_result")

if last_result is not None:
    answer_text = last_result.get("answer_text")
    rows = last_result.get("rows")

    if answer_text:
        st.subheader("Answer")
        if last_result.get("from_cache"):
            st.caption("⚡ answered instantly from cache — check \"Force refresh\" if the underlying data changed")
        st.markdown(answer_text)

    if rows:
        st.subheader("Data table")
        try:
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)
        except Exception as exc:
            st.warning(f"Could not display table: {exc}")
            df = None

        if df is not None and not df.empty:
            display_chart(df)
    elif not answer_text:
        st.info("Vanna did not return a displayable result.")