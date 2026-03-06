"""
Auditor CLI — verify completed tasks
Usage:
  python auditor_cli.py verify <task_id>      Verify a specific task
  python auditor_cli.py verify-next [dir]     Claim and verify next verifying task
  python auditor_cli.py report <task_id>      Show verification report
"""
import sys
import json
from db.connection import transaction
from core.auditor import verify_task, get_verification_report

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
DIM    = "\033[2m"


def cmd_verify(task_id: str):
    from config.settings import AUDITOR_MODEL
    print(f"\nAuditor ({AUDITOR_MODEL}) verifying {BOLD}{task_id}{RESET}...\n")
    with transaction() as conn:
        report = verify_task(conn, agent_id="auditor-01", task_id=task_id)
    if report:
        _print_report(report)
    else:
        print(f"{RED}Task unavailable or not in verifying state.{RESET}")


def cmd_verify_next(director: str = None):
    from config.settings import AUDITOR_MODEL
    label = f"director={director}" if director else "any director"
    print(f"\nAuditor ({AUDITOR_MODEL}) claiming next verifying task ({label})...\n")
    with transaction() as conn:
        report = verify_task(conn, agent_id="auditor-01", director=director)
    if report:
        _print_report(report)
    else:
        print("No tasks in verifying state right now.")


def cmd_report(task_id: str):
    with transaction() as conn:
        report = get_verification_report(conn, task_id)
    if not report:
        print(f"No verification report found for {task_id}.")
        return
    _print_report(report)


def _print_report(report: dict):
    verdict       = report["result"]
    verdict_color = GREEN if verdict == "pass" else RED
    checks        = report.get("checks", {})
    issues        = report["issues"]
    if isinstance(checks, str):
        checks = json.loads(checks)
    if isinstance(issues, str):
        issues = json.loads(issues)
    criteria = checks.get("criteria", []) if isinstance(checks, dict) else []
    summary  = checks.get("summary", "") if isinstance(checks, dict) else ""

    print(f"\n{BOLD}Verification Report{RESET} — {report['report_id']}")
    print(f"Task:    {report['task_id']}")
    print(f"Verdict: {verdict_color}{verdict.upper()}{RESET}")
    print(f"Auditor: {report['verifier']}")
    print(f"Summary: {summary}\n")

    if criteria:
        print(f"{BOLD}Criteria:{RESET}")
        for c in criteria:
            icon = f"{GREEN}✓{RESET}" if c.get("result") == "pass" else f"{RED}✗{RESET}"
            print(f"  {icon} {c.get('criterion', '')[:80]}")
            if c.get("reason"):
                print(f"     {DIM}{c['reason'][:100]}{RESET}")

    if issues:
        print(f"\n{BOLD}{RED}Issues:{RESET}")
        for issue in issues:
            print(f"  • {issue}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        sys.exit(0)

    cmd = args[0]
    if cmd == "verify" and len(args) >= 2:
        cmd_verify(args[1])
    elif cmd == "verify-next":
        cmd_verify_next(args[1] if len(args) >= 2 else None)
    elif cmd == "report" and len(args) >= 2:
        cmd_report(args[1])
    else:
        print(__doc__)
