"""
Run this once to create all OSAIO tables.
Usage: python -m db.migrate
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.connection import transaction

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def migrate():
    sql = SCHEMA_PATH.read_text()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print("Migration complete — all OSAIO tables created.")


if __name__ == "__main__":
    migrate()
