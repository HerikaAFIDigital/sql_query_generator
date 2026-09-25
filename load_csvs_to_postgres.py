import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DATABASE = os.getenv("POSTGRES_DATABASE", "vanna_demo")
PG_ADMIN_USER = os.getenv("POSTGRES_ADMIN_USER", "developer01")
PG_ADMIN_PASSWORD = os.getenv("POSTGRES_ADMIN_PASSWORD")
READONLY_USER = os.getenv("POSTGRES_USER", "vanna_readonly")

CSV_FOLDER = "./csv_data"
NA_VALUES = ["", "null", "NULL", "None", "N/A", "n/a"]


def sanitize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "unnamed"


def group_key(stem: str) -> str:
    # "app_lifecycle_events_1" and "app_lifecycle_events_2" -> both group under "app_lifecycle_events"
    return re.sub(r"_\d+$", "", stem)


def detect_separator(csv_path: Path) -> str:
    """Detect whether file uses semicolon or comma delimiter."""
    try:
        with open(csv_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            first_line = f.readline()
            if first_line.count(";") > first_line.count(","):
                return ";"
            return ","
    except Exception:
        return ";"


def load_one_csv(csv_path: Path) -> pd.DataFrame:
    sep = detect_separator(csv_path)
    df = pd.read_csv(
        csv_path,
        sep=sep,
        encoding="utf-8-sig",
        na_values=NA_VALUES,
        keep_default_na=True,
        dtype=str,
        low_memory=False,
    )
    df.columns = [sanitize_name(c) for c in df.columns]

    if "event_timestamp" in df.columns:
        ts_ms = pd.to_numeric(df["event_timestamp"], errors="coerce")
        df["event_time"] = pd.to_datetime(ts_ms, unit="ms", errors="coerce")

    return df


def create_indexes_for_table(engine, table_name: str, columns: list):
    """Create B-tree indexes for fast analytical filtering on big prod data."""
    indexes_to_create = []
    if "profile_id" in columns:
        indexes_to_create.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_prof" ON "{table_name}" ("profile_id")')
    if "event_time" in columns:
        indexes_to_create.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_time" ON "{table_name}" ("event_time")')
    if "profile_id" in columns and "event_time" in columns:
        indexes_to_create.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_proftime" ON "{table_name}" ("profile_id", "event_time")')
    if "session_id" in columns:
        indexes_to_create.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_ses" ON "{table_name}" ("session_id")')
    if "event_type" in columns:
        indexes_to_create.append(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_type" ON "{table_name}" ("event_type")')

    with engine.begin() as conn:
        for idx_sql in indexes_to_create:
            try:
                conn.execute(text(idx_sql))
            except Exception as e:
                print(f"    Index warning: {e}")


def create_unified_view(engine, created_tables: list):
    """Creates a high-performance view v_all_events uniting all event tables."""
    event_tables = [t for t in created_tables if "event" in t]
    if not event_tables:
        return

    queries = []
    with engine.connect() as conn:
        for t in event_tables:
            res = conn.execute(text(f"""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = '{t}' AND table_schema = 'public'
            """))
            cols = set(r[0] for r in res.fetchall())
            if "profile_id" not in cols or "event_time" not in cols:
                continue

            q = f"""
                SELECT 
                    profile_id,
                    event_time,
                    event_timestamp,
                    event_type,
                    description,
                    screen_name,
                    '{t}' AS log_source,
                    {'session_id' if 'session_id' in cols else "NULL::text"} AS session_id,
                    {'autonym_script' if 'autonym_script' in cols else "NULL::text"} AS autonym_script
                FROM "{t}"
                WHERE event_time IS NOT NULL
            """
            queries.append(q)

    if queries:
        view_sql = f"CREATE OR REPLACE VIEW v_all_events AS {' UNION ALL '.join(queries)};"
        with engine.begin() as conn:
            conn.execute(text(view_sql))
            conn.execute(text(f'GRANT SELECT ON v_all_events TO "{READONLY_USER}"'))
        print("  Created unified view 'v_all_events' across all event tables.")


def main():
    if not PG_ADMIN_USER or not PG_ADMIN_PASSWORD:
        sys.exit(
            "Set POSTGRES_ADMIN_USER and POSTGRES_ADMIN_PASSWORD in .env "
            "to a role that can CREATE TABLE — vanna_readonly can't write."
        )

    engine = create_engine(
        f"postgresql+psycopg2://{PG_ADMIN_USER}:{PG_ADMIN_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )

    csv_files = sorted(Path(CSV_FOLDER).glob("*.csv"))
    if not csv_files:
        sys.exit(f"No CSV files found in {CSV_FOLDER}")

    groups = defaultdict(list)
    for csv_path in csv_files:
        groups[group_key(sanitize_name(csv_path.stem))].append(csv_path)

    created_tables = []

    for table_name, parts in groups.items():
        print(f"Loading {[p.name for p in parts]} -> table '{table_name}' ...")
        frames = [load_one_csv(p) for p in parts]
        combined = pd.concat(frames, ignore_index=True)

        combined.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=25000)
        created_tables.append(table_name)
        print(f"  Loaded {len(combined):,} rows.")

        create_indexes_for_table(engine, table_name, list(combined.columns))

    with engine.begin() as conn:
        for table_name in created_tables:
            conn.execute(text(f'GRANT SELECT ON "{table_name}" TO "{READONLY_USER}"'))
    print(f"\nGranted SELECT on {len(created_tables)} table(s) to '{READONLY_USER}'.")

    create_unified_view(engine, created_tables)

    print("\nDatabase is ready for high-scale queries.")


if __name__ == "__main__":
    main()