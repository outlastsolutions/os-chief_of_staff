"""
APM CLI — interact with the Assistant Project Manager agent
Usage:
  python apm_cli.py decompose <request_id>   Decompose a scoped request into tasks
  python apm_cli.py status <request_id>      Show task progress for a request
  python apm_cli.py ready <request_id>       List tasks ready to execute
  python apm_cli.py escalate <request_id>    Check for and report escalations
"""
import sys
import json
from db.connection import transaction
from core.apm import (
    decompose_request, get_request_status,
    get_next_ready_tasks, check_escalations,
)

RESET = "\033[0m"
BOLD  = "\033[1m"
STATE_COLORS = {
    "done":      "\033[92m",
    "executing": "\033[94m",
    "verifying": "\033[96m",
    "planned":   "\033[37m",
    "blocked":   "\033[91m",
    "cancelled": "\033[90m",
}


def cmd_decompose(request_id: str):
    from config.settings import APM_MODEL
    print(f"Decomposing {BOLD}{request_id}{RESET} with APM ({APM_MODEL})...")
    with transaction() as conn:
        tasks = decompose_request(conn, request_id)

    print(f"\n✓ {len(tasks)} tasks created:\n")
    print(f"{BOLD}{'TASK ID':<20} {'COMPLEXITY':<10} {'DIRECTOR':<14} TITLE{RESET}")
    print("─" * 80)
    for t in tasks:
        print(f"{t['task_id']:<20} {t.get('complexity','?'):<10} {t.get('assigned_director','?'):<14} {t['title'][:40]}")

    print(f"\nRun:  python apm_cli.py status {request_id}")


def cmd_status(request_id: str):
    with transaction() as conn:
        s = get_request_status(conn, request_id)

    bar_done = "█" * (s["progress_pct"] // 5)
    bar_rem  = "░" * (20 - len(bar_done))

    print(f"\n{BOLD}{s['request_title']}{RESET}  [{request_id}]")
    print(f"Request: {s['request_status']}  |  Progress: [{bar_done}{bar_rem}] {s['progress_pct']}%")
    print(f"Tasks: {s['done']} done / {s['in_flight']} running / {s['pending']} pending / {s['blocked']} blocked\n")

    print(f"{BOLD}{'TASK ID':<20} {'STATUS':<12} TITLE{RESET}")
    print("─" * 72)
    for t in s["tasks"]:
        color = STATE_COLORS.get(t["status"], "")
        print(f"{t['task_id']:<20} {color}{t['status']:<12}{RESET} {t['title'][:40]}")

    if s["blocked_tasks"]:
        print(f"\n{BOLD}Blocked tasks:{RESET}")
        for b in s["blocked_tasks"]:
            print(f"  {b['task_id']} — {b['title']}")
            print(f"  Reason: {b['reason'] or 'unknown'}")


def cmd_ready(request_id: str):
    with transaction() as conn:
        tasks = get_next_ready_tasks(conn, request_id)

    if not tasks:
        print("No tasks ready to execute right now.")
        return

    print(f"\n{BOLD}Ready to execute:{RESET}\n")
    for t in tasks:
        print(f"  {BOLD}{t['task_id']}{RESET} — {t['title']}")
        print(f"  Director: {t['assigned_director']}  Complexity: {t['complexity']}")
        print(f"  Tools: {', '.join(json.loads(t['tools_allowed']) if isinstance(t['tools_allowed'], str) else t['tools_allowed'])}")
        print()


def cmd_escalate(request_id: str):
    with transaction() as conn:
        escalated = check_escalations(conn, request_id)

    if not escalated:
        print("No escalations needed.")
        return

    print(f"\n{BOLD}Escalated to PM:{RESET}")
    for t in escalated:
        print(f"  {t['task_id']} — {t['title']}")
        print(f"  Attempts: {t['attempt']}  Reason: {t.get('blocked_reason', 'unknown')}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "decompose" and len(args) >= 2:
        cmd_decompose(args[1])
    elif cmd == "status" and len(args) >= 2:
        cmd_status(args[1])
    elif cmd == "ready" and len(args) >= 2:
        cmd_ready(args[1])
    elif cmd == "escalate" and len(args) >= 2:
        cmd_escalate(args[1])
    else:
        print(__doc__)
