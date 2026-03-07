"""
Run this once to create all OSAIO tables, then apply any pending migrations.
Usage: python -m db.migrate
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.connection import transaction

SCHEMA_PATH     = Path(__file__).parent / "schema.sql"
MIGRATIONS_DIR  = Path(__file__).parent / "migrations"


def migrate():
    # 1. Apply base schema (all CREATE IF NOT EXISTS — idempotent)
    sql = SCHEMA_PATH.read_text()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("Base schema applied.")

    # 2. Apply numbered migrations in order
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    for mf in migration_files:
        print(f"Applying migration: {mf.name} ...", end=" ")
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(mf.read_text())
        print("done.")

    print("Migration complete.")


if __name__ == "__main__":
    migrate()
