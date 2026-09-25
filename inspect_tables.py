import os
import sys
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

# Optional: pass a table name to see ALL of its rows with no limit, e.g.:
#   python inspect_tables.py onboarding_events
only_table = sys.argv[1] if len(sys.argv) > 1 else None
ROW_LIMIT = None if only_table else 10

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")
tables = [r[0] for r in cursor.fetchall()]

if only_table:
    if only_table not in tables:
        print(f"'{only_table}' not found. Available tables: {tables}")
        cursor.close()
        conn.close()
        sys.exit(1)
    tables = [only_table]
else:
    print(f"Found {len(tables)} table(s): {tables}\n")

for table in tables:
    print("=" * 80)
    print(f"TABLE: {table}")
    print("=" * 80)

    cursor.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table,))
    columns = cursor.fetchall()
    print("Columns:")
    for name, dtype in columns:
        print(f"  {name}: {dtype}")

    cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
    total_rows = cursor.fetchone()[0]
    print(f"\nTotal rows: {total_rows}")

    col_names = [c[0] for c in columns]
    limit_clause = f"LIMIT {ROW_LIMIT}" if ROW_LIMIT else ""
    cursor.execute(f'SELECT * FROM "{table}" {limit_clause}')
    rows = cursor.fetchall()

    label = "all" if ROW_LIMIT is None else f"first {len(rows)}"
    print(f"\nShowing {label} row(s):")
    print(col_names)
    for row in rows:
        print(row)

    print()

cursor.close()
conn.close()