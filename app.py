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
            + "\n\n## Important Guidelines\n"
            + "- `profile_id` is the shared user identifier across all event tables.\n"
            + "- `event_time` (timestamp) is the primary time column for date filtering.\n"
            + "- Always use `COUNT(DISTINCT profile_id)` when counting users.\n"
            + "- Use `v_all_events` when querying across all event tables.\n"
            + "- In `onboarding_events`, `question_id` has the full text of the question, and `answer_id` is the literal answer.\n"
            + "- Always execute SQL via `run_sql` and cite real numbers from the output.\n"
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
