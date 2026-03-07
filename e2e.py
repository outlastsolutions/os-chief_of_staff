"""
OSAIO E2E Loop Test
Outlast Solutions LLC © 2026

Runs a full work request through the complete pipeline:
  receive → scope → decompose → plan → build → verify → done

Usage:
  python e2e.py                          # Uses built-in test request
  python e2e.py "title" "description"   # Custom request
"""

import sys
import json
import time
import hashlib
from db.connection import transaction
from core.pm  import receive_request, scope_request
from core.apm import decompose_request, get_request_status, get_next_ready_tasks
from core.director import run_domain, generate_director_report, DOMAINS

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"


_E2E_TITLE = "Write a Python utility module for string sanitisation"
_E2E_IDEM_KEY = "e2e-" + hashlib.sha256(_E2E_TITLE.encode()).hexdigest()[:16]

TEST_REQUEST = {
    "requester":        "e2e-test",
    "source":           "cli",
    "idempotency_key":  _E2E_IDEM_KEY,   # fixed key → fully idempotent re-runs
    "title":            _E2E_TITLE,
    "description":   (
        "Create a Python module `utils/sanitise.py` with functions to: "
        "(1) strip HTML tags from a string, "
        "(2) truncate a string to N characters with an ellipsis, "
        "(3) slugify a string (lowercase, hyphens, no special chars). "
        "Each function must have a docstring and be covered by unit tests in "
        "`tests/test_sanitise.py`. The tests must all pass."
    ),
    "category":      "development",
    "business_unit": "one_last",
    "priority":      "medium",
}


def header(title: str) -> None:
    print(f"\n{BOLD}{'─' * 60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─' * 60}{RESET}")


def step(label: str) -> None:
    print(f"\n{CYAN}▶ {label}{RESET}")


def ok(msg: str) -> None:
    print(f"  {GREEN}✓{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}⚠{RESET}  {msg}")


def err(msg: str) -> None:
    print(f"  {RED}✗{RESET} {msg}")


def run(title: str = None, description: str = None) -> None:
    req_data = dict(TEST_REQUEST)
    if title:
        req_data["title"] = title
    if description:
        req_data["description"] = description

    header("OSAIO E2E Loop Test")
    print(f"  Request: {BOLD}{req_data['title']}{RESET}")
    t0 = time.time()

    # ── Step 1: PM — receive (idempotent) ─────────────────────────────────
    step("PM — receive_request")
    with transaction() as conn:
        req = receive_request(conn, req_data)
    request_id = req["request_id"]
    current_status = req["status"]
    is_resume = current_status not in ("received",)
    if is_resume:
        print(f"  {YELLOW}↩ Resuming{RESET} {BOLD}{request_id}{RESET}  status={current_status}")
    else:
        ok(f"Created {BOLD}{request_id}{RESET}  status={current_status}")

    # ── Step 2: PM — scope (skip if already done) ─────────────────────────
    step("PM — scope_request  (LLM)")
    if current_status in ("received",):
        with transaction() as conn:
            scoped = scope_request(conn, request_id)
        current_status = scoped["status"]
        ok(f"Scoped → priority={scoped['priority']}  category={scoped['category']}")
    else:
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM requests WHERE request_id = %s", (request_id,))
                scoped = dict(cur.fetchone())
        print(f"  {DIM}skipped (already {current_status}){RESET}")

    # ── Step 3: APM — decompose (skip if already done) ────────────────────
    step("APM — decompose_request  (LLM)")
    if current_status in ("scoped",):
        with transaction() as conn:
            tasks = decompose_request(conn, request_id)
        current_status = "in_progress"
    else:
        # Fetch existing tasks — no re-decomposition
        with transaction() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM tasks WHERE request_id = %s ORDER BY created_at",
                    (request_id,)
                )
                tasks = [dict(r) for r in cur.fetchall()]
        print(f"  {DIM}skipped — {len(tasks)} existing task(s) loaded{RESET}")

    if not tasks:
        err("No tasks found — check APM logs.")
        sys.exit(1)

    ok(f"{len(tasks)} task(s) in pipeline:")
    for t in tasks:
        icon = (f"{GREEN}✓{RESET}" if t["status"] == "done" else
                f"{RED}✗{RESET}" if t["status"] == "blocked" else
                f"{YELLOW}~{RESET}")
        print(f"    {icon} {DIM}{t['task_id']}{RESET}  [{t.get('complexity','?')}]"
              f"  {t['status']:<10}  {t['title'][:48]}")

    # ── Step 4: Director — run each domain ────────────────────────────────
    step("Director — run_domain pipeline  (plan → build → verify)")

    all_results = {}
    for domain in DOMAINS:
        with transaction() as conn:
            results = run_domain(conn, domain, request_id=request_id, max_tasks=10)
        all_results[domain] = results

        if results["planned"] + results["built"] + results["verified"] > 0:
            status_str = (
                f"planned={results['planned']}  "
                f"built={results['built']}  "
                f"verified={results['verified']}  "
                f"failed={results['failed']}  "
                f"blocked={results['blocked']}"
            )
            color = RED if results["failed"] or results["blocked"] else GREEN
            print(f"  {color}[{domain}]{RESET}  {status_str}")

    # ── Step 5: Status summary ────────────────────────────────────────────
    step("APM — request status")
    with transaction() as conn:
        status = get_request_status(conn, request_id)

    bar_done = "█" * (status["progress_pct"] // 5)
    bar_rem  = "░" * (20 - len(bar_done))
    color = GREEN if status["progress_pct"] == 100 else (
            RED if status["blocked"] > 0 else YELLOW)
    print(f"  Progress: [{bar_done}{bar_rem}] {color}{status['progress_pct']}%{RESET}")
    print(f"  {status['done']} done / {status['in_flight']} running / "
          f"{status['pending']} pending / {status['blocked']} blocked")

    for t in status["tasks"]:
        icon = f"{GREEN}✓{RESET}" if t["status"] == "done" else (
               f"{RED}✗{RESET}" if t["status"] == "blocked" else
               f"{YELLOW}~{RESET}")
        print(f"    {icon} {t['task_id']}  {t['status']:<12}  {t['title'][:48]}")

    # ── Step 6: Director reports ──────────────────────────────────────────
    step("Director — generate reports")
    for domain in DOMAINS:
        d = all_results.get(domain, {})
        if d.get("planned", 0) + d.get("built", 0) + d.get("verified", 0) == 0:
            continue
        try:
            with transaction() as conn:
                report = generate_director_report(conn, domain, request_id)
            icon = GREEN if report["overall_status"] == "complete" else (
                   RED if report["overall_status"] == "blocked" else YELLOW)
            print(f"  {icon}[{domain}]{RESET}  {report['overall_status'].upper()}  "
                  f"({report['tasks_completed']} done, {report['tasks_failed']} failed)")
        except Exception as e:
            warn(f"{domain} report skipped: {e}")

    # ── Summary ───────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    header(f"E2E complete  ({elapsed:.1f}s)")
    print(f"  Request:  {request_id}")
    print(f"  Tasks:    {status['done']}/{len(tasks)} done")

    if status["blocked"] > 0:
        warn(f"{status['blocked']} task(s) blocked — check agent_logs for details.")
        for t in status["tasks"]:
            if t["status"] == "blocked":
                err(f"  BLOCKED: {t['task_id']} — {t['title'][:60]}")
        sys.exit(1)
    elif status["done"] < len(tasks):
        warn("Not all tasks reached done — pipeline may need another pass.")
    else:
        ok(f"{GREEN}All tasks done.{RESET}")


if __name__ == "__main__":
    args = sys.argv[1:]
    run(
        title=args[0] if len(args) >= 1 else None,
        description=args[1] if len(args) >= 2 else None,
    )
