"""
OSAIO Stage 1 Proof — 10 Consecutive Clean Task Runs
Outlast Solutions LLC © 2026

Runs 10 distinct work requests through the full CoS pipeline.
Each has a fixed idempotency key so re-runs are resumable.
All 10 must complete clean (no blocked tasks) to declare Stage 1 proven.

Usage:
  python run10.py           # run all 10
  python run10.py --resume  # skip already-done requests, continue from where we left off
"""

from __future__ import annotations
import sys
import time
import hashlib
import argparse
from db.connection import transaction
from core.pm  import receive_request, scope_request
from core.apm import decompose_request, get_request_status
from core.director import run_domain, DOMAINS

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"


# ── 10 distinct requests ───────────────────────────────────────────────────
# All are lightweight code tasks so Builder can complete them without
# external services. Each gets a fixed idempotency key derived from its title.

def _key(title: str) -> str:
    return "run10-" + hashlib.sha256(title.encode()).hexdigest()[:16]


REQUESTS = [
    {
        "title": "Write a temperature converter utility",
        "description": (
            "Create a Python module `utils/temperature.py` with three functions: "
            "`celsius_to_fahrenheit(c: float) -> float`, "
            "`fahrenheit_to_celsius(f: float) -> float`, "
            "`celsius_to_kelvin(c: float) -> float`. "
            "Each uses the standard formula and includes a docstring. "
            "Write unit tests in `tests/test_temperature.py` with at least 2 assertions per function "
            "(e.g. 0C=32F, 100C=212F, 0C=273.15K). Use `assertAlmostEqual`. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Implement a simple stack data structure",
        "description": (
            "Create a Python module `utils/stack.py` with a `Stack` class that supports "
            "push, pop, peek, is_empty, and size operations. Include docstrings. "
            "Write unit tests in `tests/test_stack.py` covering push, pop on empty stack "
            "(should raise), peek, and size. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Write a number formatting utility",
        "description": (
            "Create a Python module `utils/numformat.py` with two functions: "
            "`format_currency(amount: float, symbol: str = '$') -> str` that returns "
            "e.g. '$1,234.56', and `format_percentage(value: float, decimals: int = 1) -> str` "
            "that returns e.g. '42.5%'. Include docstrings. "
            "Write unit tests in `tests/test_numformat.py` covering positive amounts, zero, "
            "large numbers with commas, and percentage formatting. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Implement a simple queue data structure",
        "description": (
            "Create a Python module `utils/queue_ds.py` with a `Queue` class that supports "
            "`enqueue(item)`, `dequeue() -> item` (raises IndexError if empty), "
            "`peek() -> item` (raises IndexError if empty), `is_empty() -> bool`, and `size() -> int`. "
            "Use a list internally. Include docstrings on the class and all methods. "
            "Write unit tests in `tests/test_queue_ds.py` covering enqueue, dequeue, "
            "dequeue on empty (assert raises IndexError), peek, and size. All tests must pass."
        ),
        "category": "development",
        "business_unit": "cyberlight",
    },
    {
        "title": "Write a simple in-memory key-value cache with TTL",
        "description": (
            "Create a Python module `utils/cache.py` with a `Cache` class that supports "
            "`set(key, value, ttl_seconds)`, `get(key) -> value or None` (returns None if expired), "
            "and `delete(key)`. Use time.time() for expiry. Include docstrings. "
            "Write unit tests in `tests/test_cache.py` covering set/get, expiry, and delete. "
            "All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Implement a retry decorator for Python functions",
        "description": (
            "Create a Python module `utils/retry.py` with a decorator "
            "`retry(max_attempts=3, delay=0, exceptions=(Exception,))` that retries a function "
            "on exception up to max_attempts times with an optional delay. "
            "After all attempts fail, raise the last exception. Include docstrings. "
            "Write unit tests in `tests/test_retry.py` that verify retry count, eventual success, "
            "and final raise. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Write a CSV reader utility",
        "description": (
            "Create a Python module `utils/csv_reader.py` with a function "
            "`read_csv(filepath: str) -> list` that reads a CSV file and returns "
            "a list of dicts (header row as keys). Handle missing files by returning []. "
            "Write a test in `tests/test_csv_reader.py` that creates a temp CSV, reads it, "
            "and asserts the result. Also test missing file returns []. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Create a text truncation utility",
        "description": (
            "Create a Python module `utils/truncate.py` with a function "
            "`truncate(text: str, max_length: int, suffix: str = '...') -> str` "
            "that truncates text to max_length characters and appends the suffix if truncated. "
            "If text is shorter than max_length, return it unchanged. Include docstrings. "
            "Write unit tests in `tests/test_truncate.py` covering short text, exact length, "
            "and truncation with suffix. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
    {
        "title": "Write a simple config file parser",
        "description": (
            "Create a Python module `utils/config_parser.py` with a function "
            "`parse_config(filepath: str) -> dict` that reads a simple KEY=VALUE text file "
            "(one per line, # for comments, blank lines ignored) and returns a dict. "
            "Handle missing file by returning {}. Include docstrings. "
            "Write unit tests in `tests/test_config_parser.py` covering normal config, "
            "comments, blank lines, and missing file. All tests must pass."
        ),
        "category": "development",
        "business_unit": "cyberlight",
    },
    {
        "title": "Implement a word frequency counter",
        "description": (
            "Create a Python module `utils/word_freq.py` with a function "
            "`word_frequency(text: str) -> dict` that returns a dict mapping each word "
            "to its frequency in the text (case-insensitive, punctuation stripped). "
            "Include docstrings. Write unit tests in `tests/test_word_freq.py` covering "
            "normal text, empty string, and repeated words. All tests must pass."
        ),
        "category": "development",
        "business_unit": "one_last",
    },
]

# Stamp fixed idempotency keys and required fields
for r in REQUESTS:
    r["idempotency_key"] = _key(r["title"])
    r["requester"]       = "run10"
    r["source"]          = "cli"
    r["priority"]        = "medium"


# ── Display helpers ────────────────────────────────────────────────────────

def header(title: str) -> None:
    print(f"\n{BOLD}{'─' * 64}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}")


def step(n: int, total: int, label: str) -> None:
    print(f"\n{CYAN}[{n}/{total}] {label}{RESET}")


def ok(msg: str) -> None:
    print(f"  {GREEN}✓{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}⚠{RESET}  {msg}")


def err(msg: str) -> None:
    print(f"  {RED}✗{RESET} {msg}")


# ── Run one request through the full pipeline ─────────────────────────────

def run_one(req_data: dict, index: int, total: int) -> bool:
    """
    Run one request through the full pipeline.
    Returns True if all tasks completed clean (no blocks), False otherwise.
    """
    title = req_data["title"]
    step(index, total, title)

    # Step 1: Receive (idempotent)
    with transaction() as conn:
        req = receive_request(conn, dict(req_data))
    request_id     = req["request_id"]
    current_status = req["status"]

    is_resume = current_status not in ("received",)
    if is_resume:
        print(f"  {YELLOW}↩ Resuming{RESET} {DIM}{request_id}{RESET}  status={current_status}")
    else:
        ok(f"Received  {DIM}{request_id}{RESET}")

    # Step 2: Scope
    if current_status in ("received",):
        with transaction() as conn:
            scoped = scope_request(conn, request_id)
        current_status = scoped["status"]
        ok(f"Scoped    priority={scoped['priority']}")
    else:
        print(f"  {DIM}scope: skipped (already {current_status}){RESET}")

    # Step 3: Decompose
    if current_status in ("scoped",):
        with transaction() as conn:
            tasks = decompose_request(conn, request_id)
        current_status = "in_progress"
        ok(f"Decomposed  {len(tasks)} task(s)")
    else:
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM tasks WHERE request_id = %s ORDER BY created_at",
                    (request_id,)
                )
                tasks = [dict(r) for r in cur.fetchall()]
        if current_status not in ("in_progress",):
            print(f"  {DIM}decompose: skipped (already {current_status}){RESET}")

    if not tasks:
        err("No tasks created.")
        return False

    for t in tasks:
        icon = (f"{GREEN}✓{RESET}" if t["status"] == "done" else
                f"{RED}✗{RESET}" if t["status"] == "blocked" else
                f"{DIM}~{RESET}")
        print(f"    {icon} {DIM}{t['task_id']}{RESET}  [{t.get('complexity','?')}]  "
              f"{t['status']:<10}  {t['title'][:52]}")

    # Step 4: Director pipeline (skip if all tasks already done)
    if not all(t["status"] == "done" for t in tasks):
        for domain in DOMAINS:
            with transaction() as conn:
                results = run_domain(conn, domain, request_id=request_id, max_tasks=10)
            if results["planned"] + results["built"] + results["verified"] > 0:
                color = RED if (results["failed"] or results["blocked"]) else GREEN
                print(f"  {color}[{domain}]{RESET}  "
                      f"planned={results['planned']}  built={results['built']}  "
                      f"verified={results['verified']}  failed={results['failed']}  "
                      f"blocked={results['blocked']}")

    # Step 5: Final status check
    with transaction() as conn:
        status = get_request_status(conn, request_id)

    pct = status["progress_pct"]
    prog_color = GREEN if pct == 100 else (RED if status["blocked"] > 0 else YELLOW)
    print(f"  Progress: {prog_color}{pct}%{RESET}  "
          f"({status['done']} done  {status['blocked']} blocked  "
          f"{status['in_flight']} running  {status['pending']} pending)")

    if status["blocked"] > 0:
        for t in status["tasks"]:
            if t["status"] == "blocked":
                err(f"BLOCKED: {t['task_id']} — {t['title'][:60]}")
        return False

    if status["done"] < len(tasks):
        warn("Not all tasks done — re-run with --resume to continue")
        return False

    ok(f"{GREEN}CLEAN{RESET}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────

def run(resume: bool = False) -> None:
    total = len(REQUESTS)
    header("OSAIO Stage 1 — 10 Consecutive Clean Task Runs")
    print(f"  {total} requests  |  {'resume mode' if resume else 'fresh run'}")
    t0 = time.time()

    passed = []
    failed = []

    for i, req_data in enumerate(REQUESTS, start=1):
        try:
            clean = run_one(req_data, i, total)
            if clean:
                passed.append(req_data["title"])
            else:
                failed.append(req_data["title"])
        except Exception as e:
            import traceback
            err(f"Exception on request {i}: {e}")
            traceback.print_exc()
            failed.append(req_data["title"])

    elapsed = time.time() - t0
    header(f"Results  ({elapsed:.1f}s)")
    print(f"  {GREEN}{len(passed)}/{total} clean{RESET}  |  {RED}{len(failed)} failed{RESET}\n")

    for t in passed:
        print(f"  {GREEN}✓{RESET} {t}")
    for t in failed:
        print(f"  {RED}✗{RESET} {t}")

    print()
    if len(passed) == total:
        print(f"{BOLD}{GREEN}  STAGE 1 PROVEN — 10/10 clean task runs complete.{RESET}")
        print(f"  Next: Slack integration into CoS loop.")
    else:
        print(f"{BOLD}{YELLOW}  {len(failed)}/{total} request(s) need attention.{RESET}")
        print(f"  Fix blockers then re-run: python run10.py --resume")
    print()

    if len(failed) > 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSAIO Stage 1 — 10 consecutive clean task runs")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last position (skip already-done requests)")
    args = parser.parse_args()
    run(resume=args.resume)
