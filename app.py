import os
import asyncio
from dotenv import load_dotenv
import psycopg2

from vanna import Agent
from vanna.core.registry import ToolRegistry
from vanna.core.user import User
from vanna.core.user.resolver import UserResolver
from vanna.core.user.request_context import RequestContext
from vanna.core.lifecycle import LifecycleHook
from vanna.core.enhancer import LlmContextEnhancer
from vanna.integrations.ollama import OllamaLlmService
from vanna.integrations.postgres import PostgresRunner
from vanna.integrations.local.agent_memory import DemoAgentMemory
from vanna.tools import RunSqlTool
from journey_engine import UserJourneyEngine, parse_journey_query

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

PG_KWARGS = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
    user=os.getenv("POSTGRES_USER", "vanna_readonly"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

TEXT_TYPES = (
    "text",
    "character varying",
    "varchar",
    "char",
    "character",
)

# --------------------------------------------------
# 1. Ollama LLM Service
# --------------------------------------------------
llm = OllamaLlmService(
    model=os.getenv("OLLAMA_MODEL", "qwen3:8b"),
    host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
    temperature=0.1,
)

# --------------------------------------------------
# 2. PostgreSQL Runner & SQL Tool
# --------------------------------------------------
db = PostgresRunner(**PG_KWARGS)
sql_tool = RunSqlTool(sql_runner=db)

# --------------------------------------------------
# 3. Tool Registry
# --------------------------------------------------
tools = ToolRegistry()
tools.register_local_tool(sql_tool, access_groups=["user"])


# --------------------------------------------------
# 4. User Resolver & Memory
# --------------------------------------------------
class SimpleUserResolver(UserResolver):
    async def resolve_user(self, request_context):
        return User(
            id="local-user",
            username="local-user",
            group_memberships=["user"],
        )


user_resolver = SimpleUserResolver()
agent_memory = DemoAgentMemory()


# --------------------------------------------------
# 5. Dynamic Schema Enhancer
# --------------------------------------------------
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


class DynamicSchemaEnhancer(LlmContextEnhancer):
    """Pulls real column names/types from Postgres dynamically across all production tables."""

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
            + "\n\n"
            + TABLE_NOTES
        )

    async def enhance_user_messages(self, messages, user):
        return messages


schema_enhancer = DynamicSchemaEnhancer(PG_KWARGS)


# --------------------------------------------------
# 6. Debug Hook
# --------------------------------------------------
class DebugHook(LifecycleHook):
    async def before_tool(self, tool, context):
        print(f"\n[DEBUG] Calling tool: {tool.name}")

    async def after_tool(self, result):
        print(f"[DEBUG] Tool finished. success={getattr(result, 'success', None)}")
        return None


# --------------------------------------------------
# 7. Vanna Agent
# --------------------------------------------------
agent = Agent(
    llm_service=llm,
    tool_registry=tools,
    user_resolver=user_resolver,
    agent_memory=agent_memory,
    llm_context_enhancer=schema_enhancer,
    lifecycle_hooks=[DebugHook()],
)

# --------------------------------------------------
# 8. User Journey Engine
# --------------------------------------------------
journey_engine = UserJourneyEngine(PG_KWARGS)

print("Vanna agent created successfully.")
print("Ollama model:", os.getenv("OLLAMA_MODEL", "qwen3:8b"))
print("Database:", os.getenv("POSTGRES_DATABASE", "vanna_demo"))
print("Ready.")


# --------------------------------------------------
# 9. Main runner
# --------------------------------------------------
async def main():
    available_dates = journey_engine.get_available_dates()
    question = "Can you create a report of all the users of 2026-09-21 and show me their user journey?"

    print(f"\nUser question:\n{question}\n")

    is_journey, target_date, target_pid = parse_journey_query(question, available_dates)

    if is_journey:
        print(f"[Engine] Detected Journey Report request for date: {target_date or 'all'}")
        rep = journey_engine.generate_journey_report(date_filter=target_date, profile_id=target_pid)
        summary = journey_engine.generate_markdown_summary(rep)
        print("\n=== JOURNEY REPORT SUMMARY ===")
        print(summary)
        html_out = "user_journey_report.html"
        html_content = journey_engine.render_html_report(rep)
        with open(html_out, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"\n[Engine] Saved standalone interactive HTML report ({len(html_content):,} bytes) -> {html_out}")
    else:
        request_context = RequestContext(metadata={"user_id": "local-user"})
        print("Asking Vanna...")
        async for component in agent.send_message(request_context, question):
            if hasattr(component, "simple_component") and component.simple_component:
                print(component.simple_component.text)


if __name__ == "__main__":
    asyncio.run(main())
