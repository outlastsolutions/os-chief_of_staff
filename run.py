"""
Chief of Staff — autonomous daemon loop
Outlast Solutions LLC © 2026

Runs continuously. Each cycle:
  1. PM:       scope any RECEIVED requests
  2. APM:      decompose any SCOPED requests into tasks
  3. Directors: drive PLANNED/EXECUTING tasks through planner→builder→auditor

Cycle interval: COS_POLL_INTERVAL seconds (default 15)
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import datetime

import psycopg2

from db.connection import get_conn
from core.pm import get_backlog, scope_request
from core.apm import decompose_request
from core.director import run_domain, DOMAINS
from core.slack_intake import ingest as slack_ingest

POLL_INTERVAL       = int(os.getenv("COS_POLL_INTERVAL", "15"))
MAX_TASKS_PER_DOMAIN = int(os.getenv("COS_MAX_TASKS_PER_DOMAIN", "3"))


# ── Logging ───────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log(msg: str):
    print(f"[{_ts()}] {msg}", flush=True)

def err(msg: str):
    print(f"[{_ts()}] ERROR {msg}", flush=True)


# ── One cycle ─────────────────────────────────────────────────────────────────

def _recover_stale_leases(conn) -> int:
    """Reset tasks stuck in executing/verifying with expired leases back to planned."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET status          = 'planned',
                leased_by       = NULL,
                leased_until    = NULL,
                plan_id         = NULL,
                tool_calls_used = 0,
                updated_at      = NOW()
            WHERE status IN ('executing', 'verifying')
              AND leased_until < NOW()
            RETURNING task_id, title, status
            """
        )
        recovered = cur.fetchall()
    if recovered:
        conn.commit()
        for r in recovered:
            log(f"[recovery] reset stale lease: {r['task_id']} — {r['title'][:60]}")
    return len(recovered)


def _run_cycle(conn) -> dict:
    summary = {"ingested": 0, "scoped": 0, "decomposed": 0, "tasks_run": 0}

    # ── 0a. Recover stale leases ──────────────────────────────────────────────
    _recover_stale_leases(conn)

    # ── 0b. Slack intake: pull new messages from #tasks ───────────────────────
    try:
        ingested = slack_ingest(conn)
        summary["ingested"] = ingested
    except Exception as e:
        err(f"[slack_intake] error: {e}")

    # ── 1. PM: scope RECEIVED requests ───────────────────────────────────────
    received = get_backlog(conn, status="received")
    for req in received:
        rid = req["request_id"]
        try:
            log(f"[PM] scoping {rid} — {req['title'][:60]}")
            scope_request(conn, rid)
            conn.commit()
            summary["scoped"] += 1
        except Exception as e:
            conn.rollback()
            err(f"[PM] scope failed {rid}: {e}")

    # ── 2. APM: decompose SCOPED requests ────────────────────────────────────
    scoped = get_backlog(conn, status="scoped")
    for req in scoped:
        rid = req["request_id"]
        try:
            log(f"[APM] decomposing {rid} — {req['title'][:60]}")
            tasks = decompose_request(conn, rid)
            conn.commit()
            summary["decomposed"] += 1
            log(f"[APM] {rid} → {len(tasks)} task(s)")
        except Exception as e:
            conn.rollback()
            err(f"[APM] decompose failed {rid}: {e}")

    # ── 3. Directors: drive planned/executing tasks ───────────────────────────
    for domain in DOMAINS:
        try:
            result = run_domain(conn, domain, max_tasks=MAX_TASKS_PER_DOMAIN)
            conn.commit()
            ran = result.get("built", 0) + result.get("verified", 0)
            if ran:
                log(f"[Director:{domain}] planned={result['planned']} built={result['built']} "
                    f"verified={result['verified']} failed={result['failed']}")
            summary["tasks_run"] += ran
        except Exception as e:
            conn.rollback()
            err(f"[Director:{domain}] cycle error: {e}")

    return summary


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log(f"Chief of Staff starting — poll={POLL_INTERVAL}s max_tasks/domain={MAX_TASKS_PER_DOMAIN}")

    # Announce startup to Slack
    try:
        from core.secretary_client import notify
        from config.settings import SLACK_TASKS_CHANNEL
        notify(SLACK_TASKS_CHANNEL,
               ":briefcase: *Chief of Staff online* — autonomous loop started",
               agent="pm")
    except Exception as e:
        log(f"Slack startup notify skipped: {e}")

    cycle = 0
    db_down = False
    while True:
        cycle += 1
        conn = None
        try:
            conn = get_conn()
            if db_down:
                log("DB reconnected")
                db_down = False
            summary = _run_cycle(conn)
            active = summary["ingested"] + summary["scoped"] + summary["decomposed"] + summary["tasks_run"]
            if active:
                log(f"Cycle {cycle} — ingested={summary['ingested']} scoped={summary['scoped']} "
                    f"decomposed={summary['decomposed']} tasks={summary['tasks_run']}")
        except psycopg2.OperationalError as e:
            if not db_down:
                err(f"DB unreachable: {e}")
                db_down = True
            time.sleep(30)
            continue
        except Exception as e:
            err(f"Cycle {cycle} fatal: {e}")
            traceback.print_exc()
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
