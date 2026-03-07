"""
OSAIO Chief of Staff Daemon
Outlast Solutions LLC © 2026

Runs the complete CoS loop in one process:
  - Slack intake:  polls #tasks for new requests (every 60s)
  - Pipeline:      scopes, decomposes, and drives tasks through Director (every 30s)
  - Outbox:        drains and dispatches Slack/email notifications (every 5s)

The daemon is self-driving from Slack message -> done -> Slack thread reply.

Usage:
  python cos_daemon.py                    # run continuously
  python cos_daemon.py --once             # one full cycle and exit
  python cos_daemon.py --no-intake        # skip Slack intake (pipeline + outbox only)
  python cos_daemon.py --no-pipeline      # skip pipeline (intake + outbox only)
"""

from __future__ import annotations
import sys
import time
import signal
import argparse
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from db.connection import transaction
from core.pm import scope_request
from core.apm import decompose_request
from core.director import run_domain, DOMAINS
from workers.outbox_worker import drain_once
from workers.slack_intake_worker import poll_once as intake_once

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
DIM    = "\033[2m"

# ── Configurable intervals ─────────────────────────────────────────────────
INTAKE_INTERVAL   = 60   # seconds between Slack intake polls
PIPELINE_INTERVAL = 30   # seconds between pipeline passes
OUTBOX_INTERVAL   = 5    # seconds between outbox drain passes

_running = True


def _sigterm_handler(sig, frame):
    global _running
    print(f"\n[cos_daemon] SIGTERM received — shutting down gracefully...")
    _running = False


signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT,  _sigterm_handler)


# ── Pipeline pass ──────────────────────────────────────────────────────────

def _get_pending_requests(conn, status: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT request_id, title FROM requests WHERE status = %s ORDER BY created_at",
            (status,)
        )
        return [dict(r) for r in cur.fetchall()]


def run_pipeline_pass() -> dict:
    """
    One full pipeline pass:
    1. Scope all 'received' requests
    2. Decompose all 'scoped' requests
    3. Run Director for all 'in_progress' requests
    Returns summary counts.
    """
    summary = {"scoped": 0, "decomposed": 0, "planned": 0,
               "built": 0, "verified": 0, "failed": 0, "blocked": 0}

    # 1. Scope received requests
    with transaction() as conn:
        received = _get_pending_requests(conn, "received")

    for req in received:
        try:
            with transaction() as conn:
                scope_request(conn, req["request_id"])
            summary["scoped"] += 1
            print(f"  [pipeline] scoped {req['request_id']} — {req['title'][:50]}")
        except Exception as e:
            print(f"  [pipeline] scope failed {req['request_id']}: {e}")

    # 2. Decompose scoped requests
    with transaction() as conn:
        scoped = _get_pending_requests(conn, "scoped")

    for req in scoped:
        try:
            with transaction() as conn:
                tasks = decompose_request(conn, req["request_id"])
            summary["decomposed"] += 1
            print(f"  [pipeline] decomposed {req['request_id']} — {len(tasks)} task(s)")
        except Exception as e:
            print(f"  [pipeline] decompose failed {req['request_id']}: {e}")

    # 3. Director pipeline for in_progress requests
    with transaction() as conn:
        in_progress = _get_pending_requests(conn, "in_progress")

    for req in in_progress:
        request_id = req["request_id"]
        for domain in DOMAINS:
            try:
                with transaction() as conn:
                    results = run_domain(conn, domain, request_id=request_id, max_tasks=10)
                summary["planned"]  += results["planned"]
                summary["built"]    += results["built"]
                summary["verified"] += results["verified"]
                summary["failed"]   += results["failed"]
                summary["blocked"]  += results["blocked"]
                if results["planned"] + results["built"] + results["verified"] > 0:
                    color = RED if (results["failed"] or results["blocked"]) else GREEN
                    print(f"  [pipeline:{domain}] {request_id}  "
                          f"planned={results['planned']} built={results['built']} "
                          f"verified={results['verified']} failed={results['failed']}")
            except Exception as e:
                print(f"  [pipeline:{domain}] error on {request_id}: {e}")

    return summary


# ── Main loop ──────────────────────────────────────────────────────────────

def run_once(intake: bool = True, pipeline: bool = True) -> None:
    """Run exactly one cycle of intake + pipeline + outbox."""
    if intake:
        try:
            n = intake_once()
            if n:
                print(f"[cos_daemon] intake: {n} new request(s)")
        except Exception as e:
            print(f"[cos_daemon] intake error: {e}")

    if pipeline:
        try:
            s = run_pipeline_pass()
            active = s["scoped"] + s["decomposed"] + s["planned"] + s["built"] + s["verified"]
            if active:
                print(f"[cos_daemon] pipeline: scoped={s['scoped']} decomposed={s['decomposed']} "
                      f"planned={s['planned']} built={s['built']} verified={s['verified']} "
                      f"failed={s['failed']} blocked={s['blocked']}")
        except Exception as e:
            print(f"[cos_daemon] pipeline error: {e}")

    try:
        n = drain_once()
        if n:
            print(f"[cos_daemon] outbox: {n} item(s) dispatched")
    except Exception as e:
        print(f"[cos_daemon] outbox error: {e}")


def run(intake: bool = True, pipeline: bool = True,
        intake_interval: int = INTAKE_INTERVAL,
        pipeline_interval: int = PIPELINE_INTERVAL,
        outbox_interval: int = OUTBOX_INTERVAL) -> None:
    """Run continuously until SIGTERM/SIGINT."""
    global _running

    print(f"[cos_daemon] starting")
    print(f"  intake={'on' if intake else 'off'} ({intake_interval}s)  "
          f"pipeline={'on' if pipeline else 'off'} ({pipeline_interval}s)  "
          f"outbox=on ({outbox_interval}s)")
    print(f"  Ctrl-C or SIGTERM to stop\n")

    last_intake   = 0.0
    last_pipeline = 0.0
    last_outbox   = 0.0

    while _running:
        now = time.time()

        if intake and (now - last_intake) >= intake_interval:
            try:
                n = intake_once()
                if n:
                    print(f"[cos_daemon] intake: {n} new request(s)")
            except Exception as e:
                print(f"[cos_daemon] intake error: {e}")
            last_intake = time.time()

        if pipeline and (now - last_pipeline) >= pipeline_interval:
            try:
                s = run_pipeline_pass()
                active = s["scoped"] + s["decomposed"] + s["planned"] + s["built"] + s["verified"]
                if active:
                    print(f"[cos_daemon] pipeline: scoped={s['scoped']} decomposed={s['decomposed']} "
                          f"planned={s['planned']} built={s['built']} verified={s['verified']} "
                          f"failed={s['failed']} blocked={s['blocked']}")
            except Exception as e:
                print(f"[cos_daemon] pipeline error: {e}")
            last_pipeline = time.time()

        if (now - last_outbox) >= outbox_interval:
            try:
                n = drain_once()
                if n:
                    print(f"[cos_daemon] outbox: {n} item(s) dispatched")
            except Exception as e:
                print(f"[cos_daemon] outbox error: {e}")
            last_outbox = time.time()

        time.sleep(1)

    print("[cos_daemon] stopped.")


# ── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSAIO Chief of Staff Daemon")
    parser.add_argument("--once",        action="store_true",
                        help="Run one full cycle and exit")
    parser.add_argument("--no-intake",   action="store_true",
                        help="Disable Slack intake polling")
    parser.add_argument("--no-pipeline", action="store_true",
                        help="Disable the task pipeline")
    parser.add_argument("--intake-interval",   type=int, default=INTAKE_INTERVAL)
    parser.add_argument("--pipeline-interval", type=int, default=PIPELINE_INTERVAL)
    parser.add_argument("--outbox-interval",   type=int, default=OUTBOX_INTERVAL)
    args = parser.parse_args()

    do_intake   = not args.no_intake
    do_pipeline = not args.no_pipeline

    if args.once:
        run_once(intake=do_intake, pipeline=do_pipeline)
    else:
        run(
            intake=do_intake,
            pipeline=do_pipeline,
            intake_interval=args.intake_interval,
            pipeline_interval=args.pipeline_interval,
            outbox_interval=args.outbox_interval,
        )
