"""
PM CLI — interact with the Project Manager agent
Usage:
  python pm_cli.py submit    Submit a new work request
  python pm_cli.py backlog   View the request backlog
  python pm_cli.py scope <request_id>   Scope a pending request
  python pm_cli.py done <request_id>
  python pm_cli.py block <request_id> <reason>
  python pm_cli.py cancel <request_id>
"""
import sys
import json
from db.connection import transaction
from core.pm import (
    receive_request, scope_request, get_backlog,
    mark_done, mark_blocked, mark_cancelled,
)

PRIORITY_COLORS = {
    "critical": "\033[91m", "high": "\033[93m",
    "medium": "\033[94m",   "low":  "\033[37m",
}
RESET = "\033[0m"
BOLD  = "\033[1m"


def cmd_submit():
    print(f"{BOLD}Submit a work request{RESET}")
    requester    = input("Your name: ").strip()
    title        = input("Title: ").strip()
    description  = input("Description: ").strip()
    category     = input("Category (development/operations/research/marketing): ").strip() or "development"
    business_unit = input("Business unit (xout/cyberlight/low_volt_nyc/property_with_peter/one_last/outlast): ").strip() or None
    priority     = input("Priority [medium]: ").strip() or "medium"

    with transaction() as conn:
        req = receive_request(conn, {
            "requester": requester,
            "source": "cli",
            "title": title,
            "description": description,
            "category": category,
            "business_unit": business_unit,
            "priority": priority,
        })

    print(f"\n✓ Request created: {BOLD}{req['request_id']}{RESET}")
    print(f"  Status: {req['status']}")
    print(f"\nScope it now? Run:  python pm_cli.py scope {req['request_id']}")


def cmd_scope(request_id: str):
    print(f"Scoping {BOLD}{request_id}{RESET} with PM ({__import__('config.settings', fromlist=['PM_MODEL']).PM_MODEL})...")
    with transaction() as conn:
        updated = scope_request(conn, request_id)
    print(f"\n✓ Scoped.")
    print(f"  Priority: {updated['priority']}")
    print(f"  Category: {updated['category']}")
    print(f"  Status:   {updated['status']}")
    print(f"\nCheck agent_logs or director_reports for acceptance criteria.")


def cmd_backlog():
    with transaction() as conn:
        requests = get_backlog(conn)

    if not requests:
        print("Backlog is empty.")
        return

    print(f"\n{BOLD}{'ID':<20} {'PRIORITY':<10} {'STATUS':<14} {'TITLE'}{RESET}")
    print("─" * 80)
    for r in requests:
        color = PRIORITY_COLORS.get(r["priority"], "")
        print(f"{r['request_id']:<20} {color}{r['priority']:<10}{RESET} {r['status']:<14} {r['title'][:50]}")


def cmd_done(request_id: str):
    summary = input("Completion summary (optional): ").strip() or None
    with transaction() as conn:
        mark_done(conn, request_id, summary)
    print(f"✓ {request_id} marked done.")


def cmd_block(request_id: str, reason: str):
    with transaction() as conn:
        mark_blocked(conn, request_id, reason)
    print(f"✓ {request_id} marked blocked: {reason}")


def cmd_cancel(request_id: str):
    with transaction() as conn:
        mark_cancelled(conn, request_id)
    print(f"✓ {request_id} cancelled.")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "submit":
        cmd_submit()
    elif cmd == "backlog":
        cmd_backlog()
    elif cmd == "scope" and len(args) >= 2:
        cmd_scope(args[1])
    elif cmd == "done" and len(args) >= 2:
        cmd_done(args[1])
    elif cmd == "block" and len(args) >= 3:
        cmd_block(args[1], " ".join(args[2:]))
    elif cmd == "cancel" and len(args) >= 2:
        cmd_cancel(args[1])
    else:
        print(__doc__)
