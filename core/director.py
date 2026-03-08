"""
OSAIO Director Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Own a domain queue (development | operations | research | marketing)
  - Drive tasks through the Planner → Builder → Auditor pipeline
  - Detect and surface blockers to APM
  - Generate a Director Report summarising request progress for APM
  - Post milestone notifications to Slack via Secretary

Design:
  Directors do not plan or build — they coordinate.
  One Director per domain; each run processes one request's tasks.
"""

from __future__ import annotations
import json
import time
import uuid
import traceback
from datetime import datetime, timezone
from typing import Optional

from config.settings import (SLACK_TASKS_CHANNEL, DIRECTOR_MODEL,
                             DIRECTOR_APPROVAL_ENABLED, VALID_DOMAINS,
                             DOMAIN_REGISTRY)
from core.lease import fail_task
from core.idempotency import enqueue_outbox
from core.secretary_client import AGENT_IDENTITY
from core.llm import chat_json
from core import escalation as esc
from core import planner as planner_agent
from core import builder as builder_agent
from core import auditor as auditor_agent


DOMAINS = VALID_DOMAINS  # canonical source is config.settings.VALID_DOMAINS


# ── Public interface ───────────────────────────────────────────────────────

def run_domain(conn, domain: str, request_id: Optional[str] = None,
               max_tasks: int = 5) -> dict:
    """
    Drive up to max_tasks tasks for a given domain through the full pipeline.
    Optionally scoped to a single request_id.
    Returns a summary dict.
    """
    if domain not in DOMAINS:
        raise ValueError(f"Unknown domain '{domain}'. Must be one of: {DOMAINS}")

    t0 = time.time()

    # Enforce domain rollout gating before touching the DB
    is_gated, gating_status, gating_reason = _check_domain_gating(domain)
    if is_gated:
        print(f"  [director:{domain}] gated [{gating_status}]: {gating_reason}")
        gated = {
            "domain": domain, "planned": 0, "built": 0, "verified": 0,
            "failed": 0, "blocked": 0, "skipped": 0, "typed_failures": {},
            "elapsed_s": round(time.time() - t0, 3),
            "run_ts":    datetime.now(timezone.utc).isoformat(),
            "gating_status": gating_status,
            "gating_reason": gating_reason,
        }
        print(f"[telemetry] {json.dumps(gated)}")
        return gated

    results = {"domain": domain, "planned": 0, "built": 0, "verified": 0,
               "failed": 0, "blocked": 0, "skipped": 0, "typed_failures": {},
               "gating_status": "ENABLED", "gating_reason": ""}

    for _ in range(max_tasks):
        # 1. Plan the next unplanned task
        task = _next_unplanned(conn, domain, request_id)
        if task:
            try:
                planner_agent.plan_task(conn, task["task_id"])
                results["planned"] += 1
                print(f"  [director:{domain}] planned {task['task_id']} — {task['title']}")
            except ValueError as e:
                msg = str(e)
                if "escalated" in msg.lower():
                    print(f"  [director:{domain}] escalated {task['task_id']}: {msg}")
                    results["blocked"] += 1
                else:
                    print(f"  [director:{domain}] plan failed {task['task_id']}: {msg}")
                    results["failed"] += 1
                continue
            except Exception as e:
                print(f"  [director:{domain}] plan failed {task['task_id']}: {e}")
                results["failed"] += 1
                continue

            # 1.5. Director approval checkpoint (optional, gated by config)
            if DIRECTOR_APPROVAL_ENABLED:
                verdict, feedback = _review_plan(conn, task["task_id"], domain)
                if verdict == "escalate":
                    esc.escalate_task(conn, task["task_id"], feedback,
                                      agent_id=f"director:{domain}")
                    results["blocked"] += 1
                    continue
                elif verdict == "revise":
                    _request_plan_revision(conn, task["task_id"], feedback)
                    results["planned"] -= 1  # un-count; will re-plan next iteration
                    print(f"  [director:{domain}] plan revision requested: {feedback[:60]}")
                    continue
                # "approve" falls through to build

        # 2. Build the next planned task (must have a plan attached)
        report = builder_agent.execute_task(
            conn, agent_id=f"builder:{domain}",
            director=domain, task_id=None, request_id=request_id
        )
        if report:
            results["built"] += 1
        else:
            # Check if there are still planned tasks remaining before giving up
            remaining = _count_active(conn, domain, request_id)
            if remaining == 0:
                break
            # Builder found nothing claimable right now (all leased or no plans yet)
            if not task:
                break

        # 3. Verify the next verifying task
        vrep = auditor_agent.verify_task(
            conn, agent_id=f"auditor:{domain}",
            director=domain, request_id=request_id
        )
        if vrep:
            if vrep["result"] == "pass":
                results["verified"] += 1
            else:
                results["failed"] += 1

    # Surface any blocked tasks
    blocked = _get_blocked(conn, domain, request_id)
    results["blocked"] = len(blocked)
    if blocked:
        _notify_blocked(conn, domain, blocked)

    # Collect typed failure codes for tasks that failed during this cycle
    results["typed_failures"] = _collect_typed_failures(conn, domain, request_id, t0)

    # Structured telemetry — one JSON line per director cycle
    results["elapsed_s"] = round(time.time() - t0, 2)
    results["run_ts"]    = datetime.now(timezone.utc).isoformat()
    print(f"[telemetry] {json.dumps(results)}")

    return results


def get_domain_status(conn, domain: str,
                      request_id: Optional[str] = None) -> dict:
    """
    Return a status summary for all tasks in a domain.
    """
    filters = ["assigned_director = %s"]
    params: list = [domain]
    if request_id:
        filters.append("request_id = %s")
        params.append(request_id)

    where = " AND ".join(filters)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT status, COUNT(*) as n
            FROM tasks WHERE {where}
            GROUP BY status
            """,
            params
        )
        counts = {r["status"]: r["n"] for r in cur.fetchall()}

    total = sum(counts.values())
    done  = counts.get("done", 0)
    return {
        "domain":    domain,
        "total":     total,
        "done":      done,
        "executing": counts.get("executing", 0),
        "verifying": counts.get("verifying", 0),
        "planned":   counts.get("planned", 0),
        "blocked":   counts.get("blocked", 0),
        "progress":  round(done / total * 100) if total else 0,
        "counts":    counts,
    }


def generate_director_report(conn, domain: str, request_id: str) -> dict:
    """
    Create a director_report for a request, aggregating task outcomes.
    Called when all tasks in a request's domain are terminal (done/blocked).
    """
    status = get_domain_status(conn, domain, request_id)

    overall = (
        "complete" if status["blocked"] == 0 and status["planned"] == 0
                      and status["executing"] == 0 and status["verifying"] == 0
        else "blocked" if status["blocked"] > 0
        else "in_progress"
    )

    report_id = f"DREP-{uuid.uuid4().hex[:8].upper()}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO director_reports
                (report_id, request_id, director,
                 tasks_completed, tasks_failed, tasks_remaining, overall_status, summary)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                report_id, request_id, domain,
                status["done"],
                status["blocked"],
                status["planned"] + status["executing"] + status["verifying"],
                overall,
                f"{domain} domain: {status['done']}/{status['total']} tasks done. "
                f"Status: {overall}.",
            )
        )
        row = cur.fetchone()

    _notify_report(conn, domain, request_id, status, overall)
    return dict(row)


# ── Helpers ────────────────────────────────────────────────────────────────

def _check_domain_gating(domain: str) -> tuple[bool, str, str]:
    """
    Check the DOMAIN_REGISTRY rollout policy for a domain.
    Returns (is_gated, gating_status, gating_reason).
    is_gated=True means the domain must not execute.
    """
    entry  = DOMAIN_REGISTRY.get(domain, {"status": "disabled", "reason": "not in registry"})
    status = entry.get("status", "disabled")
    reason = entry.get("reason", "")
    if status == "enabled":
        return False, "ENABLED", ""
    return True, status.upper(), reason or f"domain is {status}"


def _collect_typed_failures(conn, domain: str, request_id: Optional[str],
                            since_epoch: float) -> dict:
    """
    Count tasks by failure_code that were updated during this cycle window.
    Returns e.g. {"TOOL_FAILURE": 2, "INTERNAL_ERROR": 1}.
    Surfaces build failures that the builder returns None for (silent drop paths).
    """
    since_ts = datetime.fromtimestamp(since_epoch, tz=timezone.utc)
    filters  = ["assigned_director = %s", "failure_code IS NOT NULL", "updated_at >= %s"]
    params: list = [domain, since_ts]
    if request_id:
        filters.append("request_id = %s")
        params.append(request_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT failure_code, COUNT(*) AS n FROM tasks "
            f"WHERE {' AND '.join(filters)} GROUP BY failure_code",
            params,
        )
        return {r["failure_code"]: r["n"] for r in cur.fetchall()}


def _validate_report_consistency(report_row: dict, domain_status: dict) -> list[str]:
    """
    Compare a persisted director_report row against the live domain status for
    the same domain/request. Returns a list of inconsistency descriptions;
    empty list means consistent.
    """
    issues = []
    if report_row["tasks_completed"] != domain_status["done"]:
        issues.append(
            f"tasks_completed mismatch: report={report_row['tasks_completed']} "
            f"db_done={domain_status['done']}"
        )
    expected_remaining = (
        domain_status.get("planned", 0)
        + domain_status.get("executing", 0)
        + domain_status.get("verifying", 0)
    )
    if report_row["tasks_remaining"] != expected_remaining:
        issues.append(
            f"tasks_remaining mismatch: report={report_row['tasks_remaining']} "
            f"db_remaining={expected_remaining}"
        )
    if report_row["tasks_failed"] != domain_status["blocked"]:
        issues.append(
            f"tasks_failed mismatch: report={report_row['tasks_failed']} "
            f"db_blocked={domain_status['blocked']}"
        )
    return issues


def _next_unplanned(conn, domain: str,
                    request_id: Optional[str]) -> Optional[dict]:
    """Return the oldest planned task without a plan in this domain."""
    filters = ["status = 'planned'", "plan_id IS NULL",
               "assigned_director = %s",
               "(leased_until IS NULL OR leased_until < NOW())"]
    params: list = [domain]
    if request_id:
        filters.append("request_id = %s")
        params.append(request_id)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM tasks WHERE {' AND '.join(filters)} ORDER BY created_at LIMIT 1",
            params
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _count_active(conn, domain: str, request_id: Optional[str]) -> int:
    """Count tasks still in an active (non-terminal) state for this domain."""
    filters = [
        "assigned_director = %s",
        "status IN ('planned', 'executing', 'verifying')",
    ]
    params: list = [domain]
    if request_id:
        filters.append("request_id = %s")
        params.append(request_id)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) AS n FROM tasks WHERE {' AND '.join(filters)}",
            params
        )
        return cur.fetchone()["n"]


def _get_blocked(conn, domain: str,
                 request_id: Optional[str]) -> list[dict]:
    filters = ["status = 'blocked'", "assigned_director = %s"]
    params: list = [domain]
    if request_id:
        filters.append("request_id = %s")
        params.append(request_id)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT task_id, title, blocked_reason FROM tasks WHERE {' AND '.join(filters)}",
            params
        )
        return [dict(r) for r in cur.fetchall()]


def _notify_blocked(conn, domain: str, blocked: list[dict]) -> None:
    lines = "\n".join(
        f"  • `{t['task_id']}` {t['title']}"
        + (f" — {t['blocked_reason'][:80]}" if t.get("blocked_reason") else "")
        for t in blocked
    )
    text = (
        f":warning: *[Director: {domain}] Blocked tasks*\n"
        f"{len(blocked)} task(s) need attention:\n{lines}"
    )
    dedupe_key = f"director:blocked:{domain}:" + ":".join(t["task_id"] for t in blocked)
    _enqueue_slack(conn, dedupe_key, text, agent="apm")


def _notify_report(conn, domain: str, request_id: str,
                   status: dict, overall: str) -> None:
    icon = ":white_check_mark:" if overall == "complete" else (
           ":warning:" if overall == "blocked" else ":arrows_counterclockwise:")
    text = (
        f"{icon} *[Director: {domain}]* `{request_id}` — {overall.upper()}\n"
        f"{status['done']}/{status['total']} tasks done"
        + (f" | {status['blocked']} blocked" if status["blocked"] else "")
    )
    _enqueue_slack(conn, f"director:report:{request_id}:{overall}", text, agent="apm")


def _review_plan(conn, task_id: str, domain: str) -> tuple[str, str]:
    """
    Director LLM reviews a newly created plan.
    Returns (verdict, feedback): verdict is 'approve' | 'revise' | 'escalate'.
    Only called when DIRECTOR_APPROVAL_ENABLED is True.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM tasks WHERE task_id = %s", (task_id,))
        task = cur.fetchone()
        cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
        plan = cur.fetchone()

    if not task or not plan:
        return "approve", ""

    steps = plan["steps"]
    if isinstance(steps, str):
        try:
            steps = json.loads(steps)
        except Exception:
            steps = []

    step_summary = "\n".join(
        f"  {s.get('order', '?')}. [{s.get('tool', '?')}] {s.get('title', '?')}: "
        f"{s.get('description', '')[:100]}"
        for s in steps
    )

    prompt = (
        f"You are the {domain.title()} Director at Outlast Solutions LLC.\n"
        f"Review this implementation plan and return a verdict.\n\n"
        f"Task: {task['title']}\n"
        f"Description: {task['description']}\n"
        f"Complexity: {task['complexity']}\n\n"
        f"Plan steps:\n{step_summary}\n\n"
        f"Test strategy: {plan.get('test_strategy', 'not specified')}\n"
        f"Notes: {plan.get('notes', 'none')}\n\n"
        f"Return JSON:\n"
        f'{{"verdict": "approve|revise|escalate", "feedback": "one sentence reason if not approve"}}'
    )

    try:
        result = chat_json(
            model=DIRECTOR_MODEL,
            system=(
                f"You are the {domain.title()} Director. Review plans concisely. "
                f"Approve unless the plan is clearly wrong, unsafe, or incomplete."
            ),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=256,
        )
        verdict  = result.get("verdict", "approve")
        feedback = result.get("feedback", "")
        if verdict not in ("approve", "revise", "escalate"):
            verdict = "approve"
        print(f"  [director:{domain}] plan review → {verdict}: {feedback[:60]}")
        return verdict, feedback
    except Exception as e:
        print(f"  [director:{domain}] plan review failed, defaulting to approve: {e}")
        return "approve", ""


def _request_plan_revision(conn, task_id: str, feedback: str) -> None:
    """Clear the plan and store Director feedback so Planner can re-plan."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET plan_id        = NULL,
                blocked_reason = %s,
                updated_at     = NOW()
            WHERE task_id = %s
            """,
            (f"[Director revision requested] {feedback[:400]}", task_id)
        )
        cur.execute("DELETE FROM plans WHERE task_id = %s", (task_id,))


def _enqueue_slack(conn, dedupe_key: str, text: str, agent: str = "apm") -> None:
    """Queue a Slack post via the outbox so it survives Secretary downtime."""
    identity = AGENT_IDENTITY.get(agent, {"username": agent.title(), "icon_emoji": ":robot_face:"})
    try:
        enqueue_outbox(conn, dedupe_key, "slack_post", {
            "channel":    SLACK_TASKS_CHANNEL,
            "text":       text,
            "username":   identity["username"],
            "icon_emoji": identity["icon_emoji"],
        })
    except Exception as e:
        print(f"  [director:{agent}] outbox enqueue failed: {e}")
