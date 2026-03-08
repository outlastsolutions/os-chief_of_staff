#!/usr/bin/env python3
"""
Stage 3 Readiness Check
Outlast Solutions LLC © 2026

Validates Director loop prerequisites before runtime.
Checks: environment variables, DB connectivity, critical tables.

Exit 0 = all prerequisites met.
Exit 1 = one or more checks failed (details printed to stdout).

Usage:
  python3 stage3_check.py
"""
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

# ── ANSI helpers ───────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"

_passed   = 0
_failed   = 0
_failures: list = []
_db_reachable = False


def _ok(name: str) -> None:
    global _passed
    _passed += 1
    print(f"  {GREEN}✓{RESET}  {name}")


def _fail(name: str, reason: str) -> None:
    global _failed
    _failed += 1
    _failures.append({"check": name, "reason": reason})
    print(f"  {RED}✗{RESET}  {name}")
    print(f"    {RED}→ {reason}{RESET}")


# ── Check 1: Required environment variables ────────────────────────────────

def _check_env() -> None:
    print(f"\n{BOLD}[1] Environment / config{RESET}")
    from config.settings import DATABASE_URL, GEMINI_API_KEY, ANTHROPIC_API_KEY, VALID_DOMAINS

    if DATABASE_URL:
        _ok("DATABASE_URL is set")
    else:
        _fail("DATABASE_URL", "not set — Director cannot connect to the database")

    if GEMINI_API_KEY or ANTHROPIC_API_KEY:
        _ok("LLM API key present (GEMINI_API_KEY or ANTHROPIC_API_KEY)")
    else:
        _fail("LLM API key",
              "neither GEMINI_API_KEY nor ANTHROPIC_API_KEY is set — agents cannot call LLMs")

    if VALID_DOMAINS:
        _ok(f"VALID_DOMAINS non-empty: {list(VALID_DOMAINS)}")
    else:
        _fail("VALID_DOMAINS", "empty — Director has no domains to drive")


# ── Check 2: Database connectivity ────────────────────────────────────────

def _check_db() -> None:
    global _db_reachable
    print(f"\n{BOLD}[2] Database connectivity{RESET}")
    try:
        from db.connection import get_conn
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS ping")
            row = cur.fetchone()
        conn.close()
        if row and row.get("ping") == 1:
            _ok("Postgres connection and live query successful")
            _db_reachable = True
        else:
            _fail("Postgres query", "SELECT 1 returned unexpected result")
    except Exception as e:
        _fail("Postgres connection", f"{type(e).__name__}: {str(e)[:120]}")


# ── Check 3: Critical DB tables ────────────────────────────────────────────

REQUIRED_TABLES = [
    "requests",
    "tasks",
    "plans",
    "outbox",
    "director_reports",
    "resource_locks",
]


def _check_tables() -> None:
    print(f"\n{BOLD}[3] Critical DB tables{RESET}")
    if not _db_reachable:
        print(f"  {YELLOW}⚠  Skipped — DB not reachable{RESET}")
        return
    try:
        from db.connection import get_conn
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ANY(%s)
            """, (REQUIRED_TABLES,))
            found = {r["table_name"] for r in cur.fetchall()}
        conn.close()
        for tbl in REQUIRED_TABLES:
            if tbl in found:
                _ok(f"table '{tbl}' exists")
            else:
                _fail(f"table '{tbl}'", "missing — apply db/schema.sql to create it")
    except Exception as e:
        _fail("table check", f"{type(e).__name__}: {str(e)[:120]}")


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"\n{BOLD}{'─' * 64}{RESET}")
    print(f"{BOLD}  Stage 3 Readiness Check — Director loop prerequisites{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}")

    _check_env()
    _check_db()
    _check_tables()

    total = _passed + _failed

    print(f"\n{BOLD}{'─' * 64}{RESET}")
    if _failed == 0:
        print(f"{GREEN}  READY — {_passed}/{total} checks passed{RESET}")
    else:
        print(f"{RED}  NOT READY — {_failed}/{total} check(s) failed:{RESET}")
        for f in _failures:
            print(f"  {RED}✗{RESET}  {f['check']}: {f['reason']}")
    print(f"{BOLD}{'─' * 64}{RESET}\n")

    result = {
        "status":   "ready" if _failed == 0 else "not_ready",
        "passed":   _passed,
        "failed":   _failed,
        "failures": _failures,
    }
    print(json.dumps(result))

    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
