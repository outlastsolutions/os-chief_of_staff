"""
Planner CLI — produce implementation plans for tasks
Usage:
  python planner_cli.py plan <task_id>          Plan a specific task
  python planner_cli.py plan-all <request_id>   Plan all unplanned tasks in a request
  python planner_cli.py show <task_id>          Show an existing plan
  python planner_cli.py unplanned [request_id]  List tasks without a plan
"""
import sys
import json
from db.connection import transaction
from core.planner import plan_task, get_plan, list_unplanned_tasks

RESET = "\033[0m"
BOLD  = "\033[1m"
DIM   = "\033[2m"
CYAN  = "\033[96m"
YELLOW = "\033[93m"
RED   = "\033[91m"


def cmd_plan(task_id: str):
    from config.settings import PLANNER_MODEL
    print(f"Planning {BOLD}{task_id}{RESET} with Planner ({PLANNER_MODEL})...")
    with transaction() as conn:
        plan = plan_task(conn, task_id)
    _print_plan(plan)


def cmd_plan_all(request_id: str):
    from config.settings import PLANNER_MODEL
    with transaction() as conn:
        tasks = list_unplanned_tasks(conn, request_id=request_id)

    if not tasks:
        print("No unplanned tasks found.")
        return

    print(f"Planning {len(tasks)} tasks with Planner ({PLANNER_MODEL})...\n")
    for t in tasks:
        print(f"  → {t['task_id']} — {t['title'][:50]}")
        try:
            with transaction() as conn:
                plan = plan_task(conn, t["task_id"])
            steps = json.loads(plan["steps"]) if isinstance(plan["steps"], str) else plan["steps"]
            print(f"    ✓ {len(steps)} steps  |  est. {plan.get('estimated_tool_calls', '?')} tool calls\n")
        except Exception as e:
            print(f"    ✗ Failed: {e}\n")


def cmd_show(task_id: str):
    with transaction() as conn:
        plan = get_plan(conn, task_id)
        if not plan:
            print(f"No plan found for {task_id}.")
            return
        # Also fetch task title
        with conn.cursor() as cur:
            cur.execute("SELECT title FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            title = row["title"] if row else task_id
    print(f"\n{BOLD}{title}{RESET}  [{task_id}]")
    _print_plan(plan)


def cmd_unplanned(request_id: str = None):
    with transaction() as conn:
        tasks = list_unplanned_tasks(conn, request_id=request_id)

    if not tasks:
        print("All tasks are planned.")
        return

    print(f"\n{BOLD}{'TASK ID':<20} {'COMPLEXITY':<10} {'DIRECTOR':<14} TITLE{RESET}")
    print("─" * 72)
    for t in tasks:
        print(f"{t['task_id']:<20} {t.get('complexity','?'):<10} {t.get('assigned_director','?'):<14} {t['title'][:36]}")


def _print_plan(plan: dict):
    steps = plan["steps"]
    if isinstance(steps, str):
        steps = json.loads(steps)
    risks = plan.get("risks", [])
    if isinstance(risks, str):
        risks = json.loads(risks)

    print(f"\n{BOLD}Plan {plan['plan_id']}{RESET}  |  {len(steps)} steps  |  "
          f"est. {plan.get('estimated_tool_calls', '?')} tool calls\n")

    for step in steps:
        risk_color = RED if step.get("risk", "low") not in ("low", "Low", "minimal", "Minimal") else DIM
        print(f"  {CYAN}{step['order']:>2}.{RESET} {BOLD}{step['title']}{RESET}")
        print(f"      {step['description']}")
        print(f"      {DIM}Tool: {step['tool']}  |  Resource: {step.get('resource','n/a')}{RESET}")
        print(f"      {DIM}Expected: {step['expected_output']}{RESET}")
        print(f"      {risk_color}Risk: {step.get('risk','?')}{RESET}\n")

    if plan.get("test_strategy"):
        print(f"{BOLD}Test strategy:{RESET}\n  {plan['test_strategy']}\n")

    if risks:
        print(f"{BOLD}Risks:{RESET}")
        for r in risks:
            print(f"  • {r}")

    if plan.get("notes"):
        print(f"\n{YELLOW}Notes:{RESET} {plan['notes']}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "plan" and len(args) >= 2:
        cmd_plan(args[1])
    elif cmd == "plan-all" and len(args) >= 2:
        cmd_plan_all(args[1])
    elif cmd == "show" and len(args) >= 2:
        cmd_show(args[1])
    elif cmd == "unplanned":
        cmd_unplanned(args[1] if len(args) >= 2 else None)
    else:
        print(__doc__)
