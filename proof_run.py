"""
Stage 1 Proof-Run Harness
Outlast Solutions LLC © 2026

Executes the same minimal task scenario 10 consecutive times through the full
Chief of Staff pipeline (receive → scope → decompose → plan → build → verify)
and validates that every run reaches a clean terminal state (all tasks done).

Fails immediately on the first non-clean outcome.
Produces a machine-readable JSON evidence artifact and human-readable summary.

Usage:
  python3 proof_run.py               Run 10 consecutive proof runs
  python3 proof_run.py --dry-run     Validate harness config without executing
"""

from __future__ import annotations
import sys
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from db.connection import transaction
from core.pm  import receive_request, scope_request
from core.apm import decompose_request, get_request_status
from core.director import run_domain
from config.settings import VALID_DOMAINS

# ── Configuration ─────────────────────────────────────────────────────────

PROOF_RUNS = 10

# A single minimal research task that exercises the full pipeline with
# low failure risk: web_search + file_edit, no code execution or tests.
SCENARIO = {
    "title":       "Proof run: summarise Python os.path in markdown",
    "description": (
        "Use web_search to find documentation for Python's os.path module. "
        "Write a concise 100-150 word summary of the 5 most useful functions "
        "to output/os_path_summary.md. Plain markdown only — "
        "no code execution, no test files needed."
    ),
    "category":      "research",
    "business_unit": "one_last",
    "priority":      "low",
    "requester":     "proof_harness",
    "source":        "cli",
}

SCENARIO_DOMAIN = SCENARIO["category"]   # director that owns this task type

# A task is clean when it reaches one of these statuses
CLEAN_STATUSES = {"done"}
# A task is terminally failed when it reaches one of these statuses
FAIL_STATUSES  = {"blocked", "cancelled"}

MAX_DIRECTOR_PASSES = 3   # retry director up to 3× per run before declaring incomplete


# ── Per-run execution ──────────────────────────────────────────────────────

def run_one(run_idx: int) -> dict:
    """
    Execute one proof run. Returns an evidence dict with terminal_status
    set to 'PASS', 'FAIL', 'INCOMPLETE', or 'ERROR'.
    """
    ikey = f"proof-run-{run_idx:02d}-{uuid.uuid4().hex[:8]}"
    t0   = time.time()

    evidence: dict = {
        "run":             run_idx,
        "idempotency_key": ikey,
        "request_id":      None,
        "task_ids":        [],
        "task_statuses":   {},
        "director_passes": 0,
        "terminal_status": "UNKNOWN",
        "failure_reason":  None,
        "elapsed_s":       None,
    }

    try:
        # Step 1: PM receive
        with transaction() as conn:
            req = receive_request(conn, {**SCENARIO, "idempotency_key": ikey})
        evidence["request_id"] = req["request_id"]

        # Step 2: PM scope (1 LLM call)
        with transaction() as conn:
            scope_request(conn, req["request_id"])

        # Step 3: APM decompose (1 LLM call)
        with transaction() as conn:
            tasks = decompose_request(conn, req["request_id"])
        evidence["task_ids"] = [t["task_id"] for t in tasks]

        if not tasks:
            evidence["terminal_status"] = "FAIL"
            evidence["failure_reason"]  = "APM returned 0 tasks"
            return evidence

        # Step 4: Director pipeline — up to MAX_DIRECTOR_PASSES passes
        # Each pass drives all domains; the scenario task lives in SCENARIO_DOMAIN
        # but we run all domains so dependencies across domains are satisfied.
        for pass_num in range(1, MAX_DIRECTOR_PASSES + 1):
            evidence["director_passes"] = pass_num

            for domain in VALID_DOMAINS:
                with transaction() as conn:
                    run_domain(conn, domain,
                               request_id=req["request_id"], max_tasks=10)

            # Check if all tasks have reached a terminal state
            with transaction() as conn:
                status = get_request_status(conn, req["request_id"])

            all_terminal = all(
                t["status"] in CLEAN_STATUSES | FAIL_STATUSES
                for t in status["tasks"]
            )
            if all_terminal:
                break

        # Step 5: Evaluate terminal state
        with transaction() as conn:
            status = get_request_status(conn, req["request_id"])

        for t in status["tasks"]:
            evidence["task_statuses"][t["task_id"]] = t["status"]

        failed_tasks    = [t for t in status["tasks"] if t["status"] in FAIL_STATUSES]
        unfinished_tasks = [t for t in status["tasks"]
                            if t["status"] not in CLEAN_STATUSES | FAIL_STATUSES]

        if failed_tasks:
            evidence["terminal_status"] = "FAIL"
            evidence["failure_reason"]  = (
                f"{len(failed_tasks)} task(s) in terminal failure state: "
                + ", ".join(f"{t['task_id']}={t['status']}" for t in failed_tasks)
            )
        elif unfinished_tasks:
            evidence["terminal_status"] = "INCOMPLETE"
            evidence["failure_reason"]  = (
                f"{len(unfinished_tasks)} task(s) did not reach 'done' after "
                f"{MAX_DIRECTOR_PASSES} director pass(es): "
                + ", ".join(f"{t['task_id']}={t['status']}" for t in unfinished_tasks)
            )
        else:
            evidence["terminal_status"] = "PASS"

    except Exception as e:
        import traceback
        evidence["terminal_status"] = "ERROR"
        evidence["failure_reason"]  = str(e)
        traceback.print_exc()

    finally:
        evidence["elapsed_s"] = round(time.time() - t0, 1)

    return evidence


# ── Main ──────────────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"


def main() -> int:
    dry_run = "--dry-run" in sys.argv

    print(f"\n{BOLD}{'─' * 64}{RESET}")
    print(f"{BOLD}  Stage 1 Proof-Run Harness — {PROOF_RUNS} consecutive runs{RESET}")
    print(f"{BOLD}{'─' * 64}{RESET}")
    print(f"  Scenario : {SCENARIO['title']}")
    print(f"  Domain   : {SCENARIO_DOMAIN}")
    print(f"  Runs     : {PROOF_RUNS}")
    print(f"  Gate     : all {PROOF_RUNS} runs must reach PASS (partial = FAIL)")

    if dry_run:
        print(f"\n{YELLOW}  DRY RUN — config validated, no DB writes, no LLM calls.{RESET}")
        print(f"  PROOF_RUNS        = {PROOF_RUNS}")
        print(f"  SCENARIO_DOMAIN   = {SCENARIO_DOMAIN}")
        print(f"  MAX_DIR_PASSES    = {MAX_DIRECTOR_PASSES}")
        print(f"  CLEAN_STATUSES    = {CLEAN_STATUSES}")
        print(f"  FAIL_STATUSES     = {FAIL_STATUSES}")
        print(f"  Artifact template : proof_run_YYYYMMDD_HHMMSS.json\n")
        return 0

    run_ts        = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    artifact_path = f"proof_run_{run_ts}.json"
    all_evidence: list[dict] = []
    passed = 0
    failed = 0

    for i in range(1, PROOF_RUNS + 1):
        print(f"\n{CYAN}▶ Run {i:2d}/{PROOF_RUNS}{RESET}  ", end="", flush=True)
        ev = run_one(i)
        all_evidence.append(ev)

        if ev["terminal_status"] == "PASS":
            color = GREEN
            passed += 1
        else:
            color = RED
            failed += 1

        statuses_str = " ".join(
            f"{tid[-8:]}={st}" for tid, st in ev["task_statuses"].items()
        ) or "no tasks"

        print(
            f"{color}{ev['terminal_status']}{RESET}  "
            f"req={ev['request_id'] or 'N/A'}  "
            f"tasks={len(ev['task_ids'])}  "
            f"passes={ev['director_passes']}  "
            f"elapsed={ev['elapsed_s']}s"
        )
        print(f"  {DIM}tasks: {statuses_str}{RESET}")

        if ev.get("failure_reason"):
            print(f"  {RED}✗ {ev['failure_reason'][:100]}{RESET}")

        if ev["terminal_status"] != "PASS":
            print(
                f"\n{RED}  Aborting — proof gate requires all "
                f"{PROOF_RUNS} runs to be clean.{RESET}"
            )
            break

    # Write evidence artifact
    gate_result = "PASS" if passed == PROOF_RUNS else "FAIL"
    artifact_data = {
        "proof_run_ts":   run_ts,
        "gate_result":    gate_result,
        "runs_attempted": len(all_evidence),
        "runs_passed":    passed,
        "runs_failed":    failed,
        "scenario":       SCENARIO,
        "config": {
            "PROOF_RUNS":          PROOF_RUNS,
            "SCENARIO_DOMAIN":     SCENARIO_DOMAIN,
            "MAX_DIRECTOR_PASSES": MAX_DIRECTOR_PASSES,
            "CLEAN_STATUSES":      sorted(CLEAN_STATUSES),
            "FAIL_STATUSES":       sorted(FAIL_STATUSES),
        },
        "evidence": all_evidence,
    }

    with open(artifact_path, "w") as f:
        json.dump(artifact_data, f, indent=2, default=str)

    # Summary
    print(f"\n{BOLD}{'─' * 64}{RESET}")
    if gate_result == "PASS":
        print(f"{GREEN}  GATE: PASS — {passed}/{PROOF_RUNS} consecutive clean runs{RESET}")
    else:
        print(f"{RED}  GATE: FAIL — {passed}/{PROOF_RUNS} clean runs "
              f"({failed} failure(s)){RESET}")
    print(f"  Evidence artifact: {artifact_path}")
    print(f"{BOLD}{'─' * 64}{RESET}\n")

    return 0 if gate_result == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
