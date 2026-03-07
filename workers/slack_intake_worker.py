"""
OSAIO Slack Intake Worker
Outlast Solutions LLC © 2026

Polls #tasks for new human messages and creates Chief of Staff work requests.
Wraps core/slack_intake.ingest() in a polling loop.

Usage:
  python -m workers.slack_intake_worker
  python -m workers.slack_intake_worker --once          # single poll and exit
  python -m workers.slack_intake_worker --interval 30   # poll every 30s
"""

from __future__ import annotations
import sys
import time
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from db.connection import transaction
from core.slack_intake import ingest


DEFAULT_POLL_INTERVAL = 60  # seconds


def poll_once(lookback_hours: int = 24) -> int:
    """Run one ingestion cycle. Returns count of new requests created."""
    with transaction() as conn:
        return ingest(conn, lookback_hours=lookback_hours)


def run(poll_interval: int = DEFAULT_POLL_INTERVAL,
        once: bool = False,
        lookback_hours: int = 24) -> None:
    if once:
        n = poll_once(lookback_hours=lookback_hours)
        print(f"[slack_intake_worker] ingested {n} new request(s).")
        return

    print(f"[slack_intake_worker] starting — polling every {poll_interval}s "
          f"(lookback {lookback_hours}h)")
    while True:
        try:
            n = poll_once(lookback_hours=lookback_hours)
            if n:
                print(f"  [slack_intake] {n} new request(s) ingested")
        except Exception as e:
            print(f"  [slack_intake] poll error: {e}")
        time.sleep(poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OSAIO Slack Intake Worker")
    parser.add_argument("--once",     action="store_true",
                        help="Poll once and exit")
    parser.add_argument("--interval", type=int, default=DEFAULT_POLL_INTERVAL,
                        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})")
    parser.add_argument("--lookback", type=int, default=24,
                        help="Hours of Slack history to scan per poll (default: 24)")
    args = parser.parse_args()
    run(poll_interval=args.interval, once=args.once, lookback_hours=args.lookback)
