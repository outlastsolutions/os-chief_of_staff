"""
Director CLI — drive a domain's task pipeline
Domains derive from config.settings.VALID_DOMAINS at runtime.
"""
import sys
import json
from db.connection import transaction
from core.director import run_domain, get_domain_status, generate_director_report, DOMAINS

# Usage string built at import time from the canonical DOMAINS source so it
# never drifts from the actual set of valid domains.
_USAGE = (
    "Director CLI — drive a domain's task pipeline\n"
    "Usage:\n"
    "  python director_cli.py run <domain> [request_id]     Drive tasks for a domain\n"
    "  python director_cli.py status <domain> [request_id]  Show domain task status\n"
    "  python director_cli.py report <domain> <request_id>  Generate director report\n"
    "  python director_cli.py run-all [request_id]          Drive all domains\n"
    "\n"
    f"Domains: {' | '.join(DOMAINS)}\n"
)

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
DIM    = "\033[2m"
CYAN   = "\033[96m"


def cmd_run(domain: str, request_id: str = None):
    print(f"\n{BOLD}Director [{domain}]{RESET} running pipeline"
          + (f" for {request_id}" if request_id else "") + "...\n")
    with transaction() as conn:
        results = run_domain(conn, domain, request_id=request_id)
    _print_results(domain, results)
    if results.get("gating_status", "ENABLED") != "ENABLED":
        sys.exit(1)


def cmd_run_all(request_id: str = None):
    print(f"\n{BOLD}Running all directors{RESET}"
          + (f" for {request_id}" if request_id else "") + "...\n")
    any_gated = False
    for domain in DOMAINS:
        with transaction() as conn:
            results = run_domain(conn, domain, request_id=request_id, max_tasks=3)
        _print_results(domain, results)
        if results.get("gating_status", "ENABLED") != "ENABLED":
            any_gated = True
    if any_gated:
        sys.exit(1)


def cmd_status(domain: str, request_id: str = None):
    with transaction() as conn:
        status = get_domain_status(conn, domain, request_id=request_id)
    _print_status(status)


def cmd_report(domain: str, request_id: str):
    print(f"\nGenerating director report for {domain} / {request_id}...")
    with transaction() as conn:
        report = generate_director_report(conn, domain, request_id)
    print(f"\n{BOLD}Director Report{RESET} — {report['report_id']}")
    print(f"Domain:   {report['director']}")
    print(f"Request:  {report['request_id']}")
    status_color = GREEN if report["overall_status"] == "complete" else (
                   RED if report["overall_status"] == "blocked" else YELLOW)
    print(f"Status:   {status_color}{report['overall_status'].upper()}{RESET}")
    print(f"Done:     {report['tasks_completed']} | Failed: {report['tasks_failed']} "
          f"| Remaining: {report['tasks_remaining']}")
    print(f"Summary:  {report['summary']}")


def _print_results(domain: str, results: dict):
    gating = results.get("gating_status", "ENABLED")
    if gating != "ENABLED":
        reason = results.get("gating_reason", "")
        print(f"  {YELLOW}[{domain}] GATED [{gating}]{RESET}"
              + (f": {reason}" if reason else ""))
        return
    color = GREEN if results["failed"] == 0 and results["blocked"] == 0 else YELLOW
    print(f"  {color}[{domain}]{RESET}  "
          f"planned={results['planned']}  "
          f"built={results['built']}  "
          f"verified={results['verified']}  "
          f"{RED}failed={results['failed']}{RESET}  "
          f"{YELLOW}blocked={results['blocked']}{RESET}")


def _print_status(status: dict):
    bar_filled = status["progress"] // 5
    bar = "█" * bar_filled + "░" * (20 - bar_filled)
    color = GREEN if status["progress"] == 100 else (
            RED if status["blocked"] > 0 else CYAN)
    print(f"\n{BOLD}Domain: {status['domain']}{RESET}")
    print(f"  [{bar}] {color}{status['progress']}%{RESET}  "
          f"{status['done']}/{status['total']} done")
    for state, n in status["counts"].items():
        if n > 0:
            c = GREEN if state == "done" else (RED if state == "blocked" else DIM)
            print(f"  {c}{state:12}{RESET} {n}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print(_USAGE)
        sys.exit(0)

    cmd = args[0]
    if cmd == "run" and len(args) >= 2:
        cmd_run(args[1], args[2] if len(args) >= 3 else None)
    elif cmd == "run-all":
        cmd_run_all(args[1] if len(args) >= 2 else None)
    elif cmd == "status" and len(args) >= 2:
        cmd_status(args[1], args[2] if len(args) >= 3 else None)
    elif cmd == "report" and len(args) >= 3:
        cmd_report(args[1], args[2])
    else:
        print(_USAGE)
