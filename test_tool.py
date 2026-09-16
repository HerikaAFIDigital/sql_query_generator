import os
import asyncio

from dotenv import load_dotenv

from vanna.integrations.postgres import PostgresRunner
from vanna.tools import RunSqlTool
from vanna.capabilities.sql_runner.models import RunSqlToolArgs
from vanna.core.user.request_context import RequestContext


load_dotenv()


async def main():

    db = PostgresRunner(
        host="localhost",
        port=5432,
        database="vanna_demo",
        user="vanna_readonly",
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    sql_tool = RunSqlTool(
        sql_runner=db
    )

    args = RunSqlToolArgs(
        sql="""
        SELECT COUNT(DISTINCT profile_id) AS user_count
        FROM onboarding_events
        WHERE question_id = 'Is this device a shared phone/tablet?'
        AND LOWER(answer_id) = 'yes';
        """
    )

    context = RequestContext(
        metadata={
            "user_id": "local-user"
        }
    )

    result = await sql_tool.execute(
        context,
        args
    )

    print("\n========== TOOL RESULT ==========")
    print(result)

    print("\n========== SUCCESS ==========")
    print(result.success)

    print("\n========== ERROR ==========")
    print(result.error)

    print("\n========== RESULT FOR LLM ==========")
    print(result.result_for_llm)

    print("\n========== METADATA ==========")
    print(result.metadata)


if __name__ == "__main__":
    asyncio.run(main())