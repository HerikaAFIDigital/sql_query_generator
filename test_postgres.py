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
    SELECT *
    FROM onboarding_events
    
""")

rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
