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

print("=== Columns in onboarding_events ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'onboarding_events'
    ORDER BY ordinal_position
""")
columns = cur.fetchall()
for name, dtype in columns:
    print(f"{name}: {dtype}")

print("\n=== Sample rows (first 10) ===")
cur.execute("SELECT * FROM onboarding_events LIMIT 10")
colnames = [desc[0] for desc in cur.description]
print(colnames)
for row in cur.fetchall():
    print(row)

# If there's a column that looks like it names the question/event type,
# show its distinct values so we can find the "shared phone" one.
candidate_cols = [c for c, _ in columns if any(
    k in c.lower() for k in ("event", "question", "type", "name", "key")
)]
for c in candidate_cols:
    print(f"\n=== Distinct values of {c} ===")
    cur.execute(f"SELECT DISTINCT {c} FROM onboarding_events LIMIT 30")
    for (val,) in cur.fetchall():
        print(val)

cur.close()
conn.close()