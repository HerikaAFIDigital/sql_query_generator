import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    database=os.getenv("POSTGRES_DATABASE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)

cursor = conn.cursor()

cursor.execute("""
    SELECT
        column_name,
        data_type
    FROM information_schema.columns
    WHERE table_name = 'onboarding_events'
    ORDER BY ordinal_position;
""")

rows = cursor.fetchall()

for column_name, data_type in rows:
    print(f"{column_name}: {data_type}")

cursor.close()
conn.close()