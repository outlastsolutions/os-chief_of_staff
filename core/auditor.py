"""
OSAIO Auditor Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Claim a task in 'verifying' state
  - Load the execution report + Definition of Done
  - Use LLM to verify all acceptance criteria and evidence requirements
  - PASS → transition task to 'done', store verification report
  - FAIL → transition task back to 'executing' with specific issues noted
    (Builder receives issues on next claim and can adapt)

Design:
  Single LLM call per verification. The LLM evaluates the execution report
  against the DoD criteria and returns a structured verdict with per-criterion
  results. No inner LLM loop — deterministic after the verdict is parsed.
"""

from __future__ import annotations
import json
import uuid
import traceback
from datetime import datetime, timezone
from typing import Optional

from config.settings import AUDITOR_MODEL
from core.llm import chat_json
from core.lease import claim_task, heartbeat


AUDITOR_SYSTEM = """You are the Auditor Agent for Outlast Solutions LLC.
Your job is to verify that a Builder's execution report satisfies the task's Definition of Done.

You will be given:
- The task description and goal
- The Definition of Done (acceptance criteria, evidence required, security checks)
- The execution report (artifacts, log)

Key constraints on what you can see:
- File artifacts show a path and up to 400 characters of content preview.
  If the log confirms the file was written and the preview shows relevant content,
  treat the file criterion as met — do NOT fail just because you cannot see the full file.
- code_run/shell artifacts show stdout (up to 500 chars). Truncation is normal.
- web_search artifacts show a query and snippet. Truncation is normal.

Evaluation rules:
- PASS if the log and artifact previews together provide reasonable evidence of completion.
- FAIL only if there is a clear gap — the file was NOT written, the code errored, the
  wrong content was produced, or a required criterion has zero evidence.
- Do not invent requirements beyond what the DoD states.

Respond with valid JSON only:
{
  "verdict": "pass" | "fail",
  "criteria_results": [
    {"criterion": "...", "result": "pass" | "fail", "reason": "one line"}
  ],
  "issues": ["specific issue if fail — be concrete"],
  "summary": "One sentence overall assessment"
}"""


# ── Public interface ───────────────────────────────────────────────────────

def verify_task(conn, agent_id: str,
                director: Optional[str] = None,
                task_id: Optional[str] = None) -> Optional[dict]:
    """
    Claim and verify one task in 'verifying' state.
    If task_id is given, targets that specific task.
    Otherwise claims the next available verifying task.
    Returns the verification report dict, or None if nothing to claim.
    """
    task = _claim_verifying(conn, agent_id, director, task_id)
    if not task:
        return None

    tid = task["task_id"]
    print(f"  [auditor:{agent_id}] claimed {tid} — {task['title']}")

    try:
        report  = _load_execution_report(conn, tid)
        dod     = _load_dod(conn, tid)
        plan    = _load_plan(conn, tid)

        verdict = _run_verification(task, report, dod, plan)

        # Sanity check: if LLM says fail but can't identify any actual issue
        # and all stated criteria passed, override to pass.
        criteria = verdict.get("criteria_results", [])
        issues   = verdict.get("issues", [])
        all_pass = criteria and all(c.get("result") == "pass" for c in criteria)
        if verdict["verdict"] == "fail" and not issues and all_pass:
            verdict["verdict"] = "pass"
            verdict["summary"] = (verdict.get("summary") or "") + " [auto-corrected: no issues found]"

        vrep = _store_verification_report(conn, tid, agent_id, verdict)

        if verdict["verdict"] == "pass":
            _transition_done(conn, tid, agent_id)
            print(f"  [auditor:{agent_id}] {tid} → PASS → done")
        else:
            issues_str = "; ".join(verdict.get("issues", []))
            _transition_back_to_builder(conn, tid, agent_id, issues_str)
            print(f"  [auditor:{agent_id}] {tid} → FAIL → planned (issues: {issues_str[:80]})")

        return vrep

    except Exception as e:
        tb = traceback.format_exc()
        print(f"  [auditor:{agent_id}] {tid} error: {e}")
        # Release lease without changing state — task stays in verifying for retry
        _release_lease(conn, tid)
        raise


def get_verification_report(conn, task_id: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM verification_reports WHERE task_id = %s ORDER BY created_at DESC LIMIT 1",
            (task_id,)
        )
        row = cur.fetchone()
    return dict(row) if row else None


# ── Claim ──────────────────────────────────────────────────────────────────

def _claim_verifying(conn, agent_id: str, director: Optional[str],
                     task_id: Optional[str]) -> Optional[dict]:
    if task_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET leased_by = %s,
                    leased_until = NOW() + INTERVAL '10 minutes',
                    updated_at = NOW()
                WHERE task_id = %s AND status = 'verifying'
                  AND (leased_until IS NULL OR leased_until < NOW())
                RETURNING *
                """,
                (agent_id, task_id)
            )
            row = cur.fetchone()
        return dict(row) if row else None
    else:
        director_clause = "AND assigned_director = %s" if director else ""
        params = ([director, agent_id] if director else [agent_id])
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidate AS (
                    SELECT task_id FROM tasks
                    WHERE status = 'verifying'
                      AND (leased_until IS NULL OR leased_until < NOW())
                      {director_clause}
                    ORDER BY updated_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE tasks t
                SET leased_by = %s,
                    leased_until = NOW() + INTERVAL '10 minutes',
                    updated_at = NOW()
                FROM candidate
                WHERE t.task_id = candidate.task_id
                RETURNING t.*
                """,
                params
            )
            row = cur.fetchone()
        return dict(row) if row else None


# ── Load context ───────────────────────────────────────────────────────────

def _load_execution_report(conn, task_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM execution_reports WHERE task_id = %s ORDER BY created_at DESC LIMIT 1",
            (task_id,)
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"No execution report found for {task_id}")
    return dict(row)


def _load_dod(conn, task_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM definitions_of_done WHERE task_id = %s", (task_id,))
        row = cur.fetchone()
    return dict(row) if row else {}


def _load_plan(conn, task_id: str) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
        row = cur.fetchone()
    return dict(row) if row else {}


# ── Verification ───────────────────────────────────────────────────────────

def _format_artifact(a: dict) -> str:
    """Format a single artifact for the auditor's verification prompt."""
    atype   = a.get("type", "?")
    path    = a.get("path") or a.get("command") or a.get("query") or "n/a"
    # File artifacts store content in 'preview'; others use 'output'
    content = a.get("preview") or a.get("output") or a.get("snippet") or ""
    header  = f"    - [{atype}] {path}"
    if content:
        # Include up to 800 chars so the LLM can actually see what was written
        snippet = content[:800].replace("\n", "\n      ")
        return f"{header}\n      ```\n      {snippet}\n      ```"
    return header


def _run_verification(task: dict, report: dict, dod: dict, plan: dict) -> dict:
    artifacts = report.get("artifacts", [])
    logs      = report.get("logs", [])
    if isinstance(artifacts, str):
        artifacts = json.loads(artifacts)
    if isinstance(logs, str):
        logs = json.loads(logs)

    def _parse(val):
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return [val] if val else []
        return []

    criteria = _parse(dod.get("acceptance_criteria", []))
    evidence = _parse(dod.get("evidence_required", []))
    security = _parse(dod.get("security_checks", []))

    prompt = f"""Verify this task execution.

Task: {task['title']}
Description: {task['description']}

Definition of Done:
  Goal: {dod.get('goal', task['description'])}
  Acceptance criteria:
{chr(10).join(f'    - {c}' for c in criteria) or '    (none)'}
  Evidence required:
{chr(10).join(f'    - {e}' for e in evidence) or '    (none)'}
  Security checks:
{chr(10).join(f'    - {s}' for s in security) or '    (none)'}

Execution Report (status: {report['status']}):
  Artifacts:
{chr(10).join(_format_artifact(a) for a in artifacts) or '    (none)'}
  Log:
{chr(10).join(f'    {line}' for line in logs) or '    (none)'}

Test strategy: {plan.get('test_strategy', 'n/a')}

Evaluate whether the execution satisfies every criterion. Be strict — require evidence."""

    return chat_json(
        model=AUDITOR_MODEL,
        system=AUDITOR_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2048,
    )


# ── Transitions ────────────────────────────────────────────────────────────

def _transition_done(conn, task_id: str, agent_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET status = 'done', leased_by = NULL, leased_until = NULL,
                updated_at = NOW()
            WHERE task_id = %s AND status = 'verifying' AND leased_by = %s
            """,
            (task_id, agent_id)
        )
        if cur.rowcount == 0:
            raise RuntimeError(
                f"_transition_done: 0 rows updated for {task_id} — "
                "lease may have been stolen or task already transitioned."
            )
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, task_id, log_data)
            VALUES (%s, 'auditor', 'audit:pass', %s, %s)
            """,
            (agent_id, task_id, json.dumps({"verdict": "pass"}))
        )


def _transition_back_to_builder(conn, task_id: str, agent_id: str, issues: str) -> None:
    """Return task to planned so any builder can retry with audit issues noted."""
    with conn.cursor() as cur:
        # NULL plan_id on task FIRST (FK constraint), then delete the plan
        cur.execute(
            """
            UPDATE tasks
            SET status          = 'planned',
                leased_by       = NULL,
                leased_until    = NULL,
                blocked_reason  = %s,
                plan_id         = NULL,
                tool_calls_used = 0,
                updated_at      = NOW()
            WHERE task_id = %s AND status = 'verifying' AND leased_by = %s
            """,
            (f"[Auditor issues]: {issues}", task_id, agent_id)
        )
        if cur.rowcount == 0:
            raise RuntimeError(
                f"_transition_back_to_builder: 0 rows updated for {task_id} — "
                "lease may have been stolen or task already transitioned."
            )
        cur.execute("DELETE FROM plans WHERE task_id = %s", (task_id,))
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, task_id, log_data)
            VALUES (%s, 'auditor', 'audit:fail', %s, %s)
            """,
            (agent_id, task_id, json.dumps({"verdict": "fail", "issues": issues}))
        )


def _release_lease(conn, task_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE tasks SET leased_by = NULL, leased_until = NULL WHERE task_id = %s",
            (task_id,)
        )


# ── Store verification report ──────────────────────────────────────────────

def _store_verification_report(conn, task_id: str, auditor: str, verdict: dict) -> dict:
    report_id = f"VREP-{uuid.uuid4().hex[:8].upper()}"
    issues    = verdict.get("issues", [])
    criteria  = verdict.get("criteria_results", [])
    summary   = verdict.get("summary", "")

    # Map to DB schema: verifier, result, checks, evidence
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO verification_reports
                (report_id, task_id, verifier, result, checks, issues, evidence)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                report_id, task_id, auditor,
                verdict["verdict"],
                json.dumps({"criteria": criteria, "summary": summary}),
                json.dumps(issues),
                json.dumps([]),
            )
        )
        row = cur.fetchone()
    return dict(row)
