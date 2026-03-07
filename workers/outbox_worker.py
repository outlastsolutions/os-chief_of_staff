"""
OSAIO Outbox Worker
Outlast Solutions LLC © 2026

Drains the outbox table and dispatches side effects to Secretary.
Runs as a standalone process alongside the CoS pipeline.

Supported outbox types:
  - slack_post  → Secretary slack_post_message tool
  - email       → Secretary send_email tool
  - webhook     → raw HTTP POST to payload["url"]

Usage:
  python -m workers.outbox_worker
  python -m workers.outbox_worker --once      # drain once and exit
  python -m workers.outbox_worker --interval 10
"""

from __future__ import annotations
import sys
import time
import json
import urllib.request
import argparse
from pathlib import Path

# Allow running as a module from the chief_of_staff root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from db.connection import transaction
from core.idempotency import (
    claim_pending_outbox, mark_outbox_sent, mark_outbox_failed, reclaim_stale_outbox
)
from core.secretary_client import post_slack, send_email as sec_send_email


DEFAULT_POLL_INTERVAL = 5  # seconds
BATCH_SIZE = 20


# ── Dispatcher ────────────────────────────────────────────────────────────

def dispatch(item: dict) -> str:
    """
    Dispatch one outbox item. Returns a short status string.
    Raises on failure so the caller can mark it failed.
    """
    type_   = item["type"]
    payload = item["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    if type_ == "slack_post":
        post_slack(
            channel    = payload["channel"],
            text       = payload["text"],
            username   = payload.get("username"),
            icon_emoji = payload.get("icon_emoji"),
            thread_ts  = payload.get("thread_ts"),
        )
        return f"slack→{payload['channel']}"

    elif type_ == "email":
        sec_send_email(
            to      = payload["to"],
            subject = payload["subject"],
            body    = payload["body"],
            unit    = payload.get("unit", "outlast"),
        )
        return f"email→{payload['to']}"

    elif type_ == "webhook":
        url  = payload["url"]
        body = json.dumps(payload.get("body", {})).encode()
        headers = payload.get("headers", {"Content-Type": "application/json"})
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return f"webhook→{url}"

    else:
        raise ValueError(f"Unknown outbox type: {type_!r}")


# ── Poll loop ─────────────────────────────────────────────────────────────

def drain_once() -> tuple[int, int, int]:
    """
    Claim and dispatch one batch of pending outbox items.
    Reclaims any stale 'sending' rows first (recovery from prior worker crash).

    Delivery semantics: at-least-once. If the worker crashes between dispatch()
    and mark_outbox_sent(), reclaim_stale_outbox() will reset the row to 'pending'
    and it will be re-sent. Consumers of outbox side-effects (Slack, email) should
    tolerate occasional duplicate delivery.

    Returns (attempted, sent, failed).
    """
    with transaction() as conn:
        reclaimed = reclaim_stale_outbox(conn)
        if reclaimed:
            print(f"  [outbox] reclaimed {reclaimed} stale sending row(s)")
        items = claim_pending_outbox(conn, limit=BATCH_SIZE)

    attempted = sent = failed = 0
    for item in items:
        oid = item["outbox_id"]
        attempted += 1
        try:
            result = dispatch(item)
            with transaction() as conn:
                mark_outbox_sent(conn, oid)
            print(f"  [outbox] sent  #{oid} ({item['type']}) → {result}")
            sent += 1
        except Exception as e:
            import traceback
            with transaction() as conn:
                mark_outbox_failed(conn, oid, error=str(e))
            print(f"  [outbox] FAIL  #{oid} ({item['type']}): {e}")
            traceback.print_exc()
            failed += 1

    return attempted, sent, failed


def run(poll_interval: int = DEFAULT_POLL_INTERVAL, once: bool = False) -> None:
    if poll_interval <= 0:
        raise ValueError(f"poll_interval must be > 0, got {poll_interval}")

    if once:
        attempted, sent, failed = drain_once()
        print(f"[outbox_worker] drained — attempted={attempted} sent={sent} failed={failed}")
        return

    print(f"[outbox_worker] starting — polling every {poll_interval}s")
    while True:
        try:
            attempted, sent, failed = drain_once()
            if attempted:
                print(f"  [outbox] cycle — attempted={attempted} sent={sent} failed={failed}")
        except Exception as e:
            import traceback
            print(f"  [outbox] poll error: {e}")
            traceback.print_exc()
        time.sleep(poll_interval)


# ── CLI ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSAIO Outbox Worker")
    parser.add_argument("--once",     action="store_true",
                        help="Drain pending items once and exit")
    parser.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})")
    args = parser.parse_args()
    run(poll_interval=args.interval, once=args.once)
