import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    database=os.getenv("POSTGRES_DATABASE", "vanna_demo"),
    user=os.getenv("POSTGRES_USER", "vanna_readonly"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
cur = conn.cursor()
cur.execute("""
    SELECT answer_id, COUNT(DISTINCT profile_id)
    FROM onboarding_events
    WHERE question_id = 'Is this device a shared phone/tablet?'
    GROUP BY answer_id
""")
for answer, count in cur.fetchall():
    print(answer, count)
cur.close()
conn.close()