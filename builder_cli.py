"""
Builder CLI — execute planned tasks
Usage:
  python builder_cli.py run <task_id>           Execute a specific task
  python builder_cli.py run-next [director]     Claim and execute next available task
  python builder_cli.py report <task_id>        Show execution report
"""
import sys
import json
from db.connection import transaction
from core.builder import execute_task, get_report

RESET = "\033[0m"
BOLD  = "\033[1m"
GREEN = "\033[92m"
RED   = "\033[91m"
DIM   = "\033[2m"


def cmd_run(task_id: str):
    from config.settings import BUILDER_MODEL
    print(f"\nBuilder ({BUILDER_MODEL}) executing {BOLD}{task_id}{RESET}...\n")
    with transaction() as conn:
        report = execute_task(conn, agent_id="builder-01", task_id=task_id)

    if report:
        _print_report(report)
    else:
        print(f"{RED}Execution failed or task unavailable.{RESET}")


def cmd_run_next(director: str = None):
    from config.settings import BUILDER_MODEL
    label = f"director={director}" if director else "any director"
    print(f"\nBuilder ({BUILDER_MODEL}) claiming next task ({label})...\n")
    with transaction() as conn:
        report = execute_task(conn, agent_id="builder-01", director=director)

    if report:
        _print_report(report)
    else:
        print("No tasks available to execute right now.")


def cmd_report(task_id: str):
    with transaction() as conn:
        report = get_report(conn, task_id)
    if not report:
        print(f"No execution report found for {task_id}.")
        return
    _print_report(report)


def _print_report(report: dict):
    status_color = GREEN if report["status"] == "completed" else RED
    artifacts = report["artifacts"]
    logs      = report["logs"]
    if isinstance(artifacts, str):
        artifacts = json.loads(artifacts)
    if isinstance(logs, str):
        logs = json.loads(logs)

    print(f"\n{BOLD}Execution Report{RESET} — {report['report_id']}")
    print(f"Task:    {report['task_id']}")
    print(f"Status:  {status_color}{report['status']}{RESET}")
    print(f"Executor: {report['executor']}\n")

    if logs:
        print(f"{BOLD}Execution log:{RESET}")
        for line in logs:
            icon = f"{GREEN}✓{RESET}" if line.startswith("✓") else f"{RED}✗{RESET}"
            print(f"  {icon} {line[2:] if line.startswith(('✓','✗')) else line}")

    if artifacts:
        print(f"\n{BOLD}Artifacts:{RESET}")
        for a in artifacts:
            atype = a.get("type", "?")
            if atype == "file":
                print(f"  📄 {a.get('path')}")
            elif atype in ("code_output", "shell_output"):
                print(f"  ⚙️  {a.get('path') or a.get('command','')}")
                if a.get("output"):
                    print(f"     {DIM}{a['output'][:120]}{RESET}")
            elif atype == "research":
                print(f"  🔍 query: {a.get('query','')[:60]}")
                if a.get("snippet"):
                    print(f"     {DIM}{a['snippet'][:120]}{RESET}")
            else:
                print(f"  • {a}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "run" and len(args) >= 2:
        cmd_run(args[1])
    elif cmd == "run-next":
        cmd_run_next(args[1] if len(args) >= 2 else None)
    elif cmd == "report" and len(args) >= 2:
        cmd_report(args[1])
    else:
        print(__doc__)
