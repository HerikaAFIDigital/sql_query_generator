# import os
# import asyncio

# from dotenv import load_dotenv
# import psycopg2

# from vanna import Agent
# from vanna.core.registry import ToolRegistry
# from vanna.core.user import User
# from vanna.core.user.resolver import UserResolver
# from vanna.core.user.request_context import RequestContext
# from vanna.core.lifecycle import LifecycleHook
# from vanna.core.enhancer import LlmContextEnhancer
# from vanna.integrations.ollama import OllamaLlmService
# from vanna.integrations.postgres import PostgresRunner
# from vanna.integrations.local.agent_memory import DemoAgentMemory
# from vanna.tools import RunSqlTool


# # --------------------------------------------------
# # Load environment variables
# # --------------------------------------------------

# load_dotenv()

# PG_KWARGS = dict(
#     host=os.getenv("POSTGRES_HOST", "localhost"),
#     port=int(os.getenv("POSTGRES_PORT", "5432")),
#     database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
#     user=os.getenv("POSTGRES_USER", "vanna_readonly"),
#     password=os.getenv("POSTGRES_PASSWORD"),
# )

# # Add every table you want the model to know about here.
# KNOWN_TABLES = ["onboarding_events"]


# # --------------------------------------------------
# # 1. Ollama
# # --------------------------------------------------

# llm = OllamaLlmService(
#     model=os.getenv("OLLAMA_MODEL", "qwen3:14b"),
#     host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
# )


# # --------------------------------------------------
# # 2. PostgreSQL
# # --------------------------------------------------

# db = PostgresRunner(**PG_KWARGS)


# # --------------------------------------------------
# # 3. SQL tool
# # --------------------------------------------------

# sql_tool = RunSqlTool(
#     sql_runner=db
# )


# # --------------------------------------------------
# # 4. Tool registry
# # --------------------------------------------------

# tools = ToolRegistry()

# tools.register_local_tool(
#     sql_tool,
#     access_groups=["user"]
# )


# # --------------------------------------------------
# # 5. User resolver
# # --------------------------------------------------

# class SimpleUserResolver(UserResolver):

#     async def resolve_user(self, request):
#         return User(
#             id="local-user",
#             username="local-user",
#             group_memberships=["user"],
#         )


# user_resolver = SimpleUserResolver()


# # --------------------------------------------------
# # 6. Agent memory
# # --------------------------------------------------

# agent_memory = DemoAgentMemory()


# # --------------------------------------------------
# # 7. Schema-awareness: teach the LLM columns AND their real values
# # --------------------------------------------------

# TEXT_TYPES = ("text", "character varying", "varchar", "char", "character")


# class SchemaEnhancer(LlmContextEnhancer):
#     """Pulls real column names/types from Postgres, and for any low-cardinality
#     text column (like an EAV question/answer column), also pulls the actual
#     distinct values so the model doesn't have to guess them."""

#     def __init__(self, pg_kwargs, tables, enum_threshold=50):
#         self.pg_kwargs = pg_kwargs
#         self.tables = tables
#         self.enum_threshold = enum_threshold
#         self._cache = None

#     def _load_schema(self):
#         if self._cache is not None:
#             return self._cache
#         try:
#             conn = psycopg2.connect(**self.pg_kwargs)
#             cur = conn.cursor()
#             lines = []
#             for table in self.tables:
#                 cur.execute(
#                     """
#                     SELECT column_name, data_type
#                     FROM information_schema.columns
#                     WHERE table_name = %s
#                     ORDER BY ordinal_position
#                     """,
#                     (table,),
#                 )
#                 cols = cur.fetchall()
#                 if not cols:
#                     lines.append(f"- {table}: (no columns found — check the table name)")
#                     continue

#                 col_parts = []
#                 for name, dtype in cols:
#                     part = f"{name} ({dtype})"
#                     if dtype in TEXT_TYPES:
#                         cur.execute(f'SELECT COUNT(DISTINCT "{name}") FROM "{table}"')
#                         distinct_count = cur.fetchone()[0]
#                         if 0 < distinct_count <= self.enum_threshold:
#                             cur.execute(f'SELECT DISTINCT "{name}" FROM "{table}" ORDER BY 1')
#                             values = [str(r[0]) for r in cur.fetchall()]
#                             values_str = ", ".join(f"'{v}'" for v in values)
#                             part += f" [actual values: {values_str}]"
#                     col_parts.append(part)

#                 lines.append(f"- {table}({', '.join(col_parts)})")

#             cur.close()
#             conn.close()
#             self._cache = "\n".join(lines)
#         except Exception as exc:
#             self._cache = f"(schema lookup failed: {exc})"
#         return self._cache

#     async def enhance_system_prompt(self, system_prompt, user_message, user):
#         schema = self._load_schema()
#         return (
#             system_prompt
#             + "\n\n## Database schema (this is the ONLY schema that exists)\n"
#             + schema
#             + "\n\n## Important notes on this schema\n"
#               "- `onboarding_events` stores ONE ROW PER QUESTION a user (profile_id) answered.\n"
#               "- Despite the column names, `question_id` holds the FULL TEXT of the question "
#               "(not a numeric id), and `answer_id` holds the literal answer text (not a numeric id).\n"
#               "- To count how many users gave a specific answer to a specific question, filter with "
#               "an exact string match on BOTH `question_id` and `answer_id`, then "
#               "COUNT(DISTINCT profile_id). Example:\n"
#               "  SELECT COUNT(DISTINCT profile_id) FROM onboarding_events "
#               "WHERE question_id = 'Is this device a shared phone/tablet?' AND answer_id = 'yes';\n"
#               "- Only use the exact question/answer strings listed in the schema above — never "
#               "paraphrase or guess the wording.\n"
#         )

#     async def enhance_user_messages(self, messages, user):
#         return messages


# schema_enhancer = SchemaEnhancer(PG_KWARGS, KNOWN_TABLES)


# # --------------------------------------------------
# # 8. Debug hook: prints the real error/result if a tool call fails
# # --------------------------------------------------

# class DebugHook(LifecycleHook):

#     async def before_tool(self, tool, context):
#         print(f"\n[DEBUG] Calling tool: {tool.name}")
#         try:
#             print(f"[DEBUG] context: {vars(context)}")
#         except Exception:
#             pass

#     async def after_tool(self, result):
#         print(f"\n[DEBUG] Tool finished. success={getattr(result, 'success', None)}")
#         try:
#             print(f"[DEBUG] full result: {vars(result)}")
#         except Exception:
#             print(f"[DEBUG] result repr: {result!r}")
#         return None


# # --------------------------------------------------
# # 9. Vanna Agent
# # --------------------------------------------------

# agent = Agent(
#     llm_service=llm,
#     tool_registry=tools,
#     user_resolver=user_resolver,
#     agent_memory=agent_memory,
#     llm_context_enhancer=schema_enhancer,
#     lifecycle_hooks=[DebugHook()],
# )


# # --------------------------------------------------
# # 10. Startup information
# # --------------------------------------------------

# print("Vanna agent created successfully.")
# print("Ollama model:", os.getenv("OLLAMA_MODEL", "qwen3:14b"))
# print("Database:", os.getenv("POSTGRES_DATABASE", "vanna_demo"))
# print("Database user:", os.getenv("POSTGRES_USER", "vanna_readonly"))
# print("Ready.")


# # --------------------------------------------------
# # 11. Test natural-language query
# # --------------------------------------------------

# async def main():

#     request_context = RequestContext(
#         metadata={
#             "user_id": "local-user"
#         }
#     )

#     question = "How many users said yes to the shared phone question?"

#     print("\nUser question:")
#     print(question)

#     print("\nAsking Vanna...\n")

#     async for component in agent.send_message(
#         request_context,
#         question
#     ):
#         print("\n--- COMPONENT ---")
#         print(component)

#         if hasattr(component, "model_dump"):
#             print("MODEL DUMP:")
#             print(component.model_dump())

#         if hasattr(component, "metadata"):
#             print("METADATA:")
#             print(component.metadata)

#         if hasattr(component, "simple_component"):
#             print("SIMPLE:")
#             print(component.simple_component)

#         if hasattr(component, "rich_component"):
#             print("RICH:")
#             print(component.rich_component)


# # --------------------------------------------------
# # 12. Run
# # --------------------------------------------------

# if __name__ == "__main__":
#     asyncio.run(main())


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

# Add every table you want the model to know about here.
KNOWN_TABLES = ["onboarding_events"]


# --------------------------------------------------
# 1. Ollama
# --------------------------------------------------

llm = OllamaLlmService(
    model=os.getenv("OLLAMA_MODEL", "qwen3:14b"),
    host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
)


# --------------------------------------------------
# 2. PostgreSQL
# --------------------------------------------------

db = PostgresRunner(**PG_KWARGS)


# --------------------------------------------------
# 3. SQL tool
# --------------------------------------------------

sql_tool = RunSqlTool(
    sql_runner=db
)


# --------------------------------------------------
# 4. Tool registry
# --------------------------------------------------

tools = ToolRegistry()

tools.register_local_tool(
    sql_tool,
    access_groups=["user"]
)


# --------------------------------------------------
# 5. User resolver
# --------------------------------------------------

class SimpleUserResolver(UserResolver):

    async def resolve_user(self, request):
        return User(
            id="local-user",
            username="local-user",
            group_memberships=["user"],
        )


user_resolver = SimpleUserResolver()


# --------------------------------------------------
# 6. Agent memory
# --------------------------------------------------

agent_memory = DemoAgentMemory()


# --------------------------------------------------
# 7. Schema-awareness
# --------------------------------------------------

TEXT_TYPES = (
    "text",
    "character varying",
    "varchar",
    "char",
    "character",
)


class SchemaEnhancer(LlmContextEnhancer):
    """
    Pulls real column names/types from Postgres.

    For low-cardinality text columns, also pulls the
    actual distinct values so the model doesn't have
    to guess them.
    """

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
                    SELECT column_name, data_type
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
                        "(no columns found — check the table name)"
                    )

                    continue

                col_parts = []

                for name, dtype in cols:

                    part = f"{name} ({dtype})"

                    if dtype in TEXT_TYPES:

                        cur.execute(
                            f'''
                            SELECT COUNT(DISTINCT "{name}")
                            FROM "{table}"
                            '''
                        )

                        distinct_count = cur.fetchone()[0]

                        if (
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
                                str(r[0])
                                for r in cur.fetchall()
                            ]

                            values_str = ", ".join(
                                f"'{v}'"
                                for v in values
                            )

                            part += (
                                f" [actual values: {values_str}]"
                            )

                    col_parts.append(part)

                lines.append(
                    f"- {table}({', '.join(col_parts)})"
                )

            self._cache = "\n".join(lines)

        except Exception as exc:

            self._cache = (
                f"(schema lookup failed: {exc})"
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

        schema = self._load_schema()

        return (
            system_prompt

            + "\n\n## Database schema "
              "(this is the ONLY schema that exists)\n"

            + schema

            + "\n\n## Important notes on this schema\n"

              "- `onboarding_events` stores ONE ROW PER QUESTION "
              "a user (profile_id) answered.\n"

              "- Despite the column names, `question_id` holds "
              "the FULL TEXT of the question "
              "(not a numeric id), and `answer_id` holds "
              "the literal answer text "
              "(not a numeric id).\n"

              "- To count how many users gave a specific answer "
              "to a specific question, filter with an exact "
              "string match on BOTH `question_id` and `answer_id`, "
              "then COUNT(DISTINCT profile_id).\n"

              "- Example:\n"
              "  SELECT COUNT(DISTINCT profile_id) "
              "FROM onboarding_events "
              "WHERE question_id = "
              "'Is this device a shared phone/tablet?' "
              "AND answer_id = 'yes';\n"

              "- Only use the exact question/answer strings "
              "listed in the schema above — never "
              "paraphrase or guess the wording.\n"
        )

    async def enhance_user_messages(
        self,
        messages,
        user
    ):
        return messages


schema_enhancer = SchemaEnhancer(
    PG_KWARGS,
    KNOWN_TABLES
)


# --------------------------------------------------
# 8. Debug hook
# --------------------------------------------------

class DebugHook(LifecycleHook):

    async def before_tool(
        self,
        tool,
        context
    ):

        print(
            f"\n[DEBUG] Calling tool: {tool.name}"
        )

        try:
            print(
                f"[DEBUG] context: {vars(context)}"
            )
        except Exception:
            pass

    async def after_tool(
        self,
        result
    ):

        print(
            "\n[DEBUG] Tool finished. "
            f"success={getattr(result, 'success', None)}"
        )

        try:

            print(
                f"[DEBUG] full result: "
                f"{vars(result)}"
            )

        except Exception:

            print(
                f"[DEBUG] result repr: "
                f"{result!r}"
            )

        return None


# --------------------------------------------------
# 9. Vanna Agent
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
# 10. Startup information
# --------------------------------------------------

print(
    "Vanna agent created successfully."
)

print(
    "Ollama model:",
    os.getenv(
        "OLLAMA_MODEL",
        "qwen3:14b"
    )
)

print(
    "Database:",
    os.getenv(
        "POSTGRES_DATABASE",
        "vanna_demo"
    )
)

print(
    "Database user:",
    os.getenv(
        "POSTGRES_USER",
        "vanna_readonly"
    )
)

print("Ready.")


# --------------------------------------------------
# 11. Test natural-language query
# --------------------------------------------------

async def main():

    request_context = RequestContext(
        metadata={
            "user_id": "local-user"
        }
    )

    question = (
        "How many users said yes "
        "to the shared phone question?"
    )

    print("\nUser question:")
    print(question)

    print("\nAsking Vanna...\n")

    async for component in agent.send_message(
        request_context,
        question
    ):

        print(
            "\n--- COMPONENT ---"
        )

        print(component)

        if hasattr(
            component,
            "model_dump"
        ):

            print(
                "MODEL DUMP:"
            )

            print(
                component.model_dump()
            )

        if hasattr(
            component,
            "metadata"
        ):

            print(
                "METADATA:"
            )

            print(
                component.metadata
            )

        if hasattr(
            component,
            "simple_component"
        ):

            print(
                "SIMPLE:"
            )

            print(
                component.simple_component
            )

        if hasattr(
            component,
            "rich_component"
        ):

            print(
                "RICH:"
            )

            print(
                component.rich_component
            )


# --------------------------------------------------
# 12. Run
# --------------------------------------------------

if __name__ == "__main__":
    asyncio.run(main())
