"""
OSAIO Escalation Rules — Stage 2
Outlast Solutions LLC © 2026

Human-in-the-loop safety gates, enforced in code before planning or execution.
Any rule that fires blocks the task with HUMAN_DECISION_REQUIRED
and enqueues a Slack notification via the outbox.

Rules:
  1. File count         — plan touches > MAX_FILES_PER_TASK files
  2. Schema + app code  — migration and application code in same task
  3. Sensitive content  — task touches secrets, billing, auth primitives, infra
  4. Repeated failure   — same failure code has fired twice on this task
  5. Missing criteria   — no acceptance criteria defined (can't verify completion)
"""

from __future__ import annotations
import json
import re
from typing import Optional

from core.lease import FailureCode
from core.idempotency import enqueue_outbox
from config.settings import SLACK_TASKS_CHANNEL
from core.secretary_client import AGENT_IDENTITY


# ── Thresholds ────────────────────────────────────────────────────────────

MAX_FILES_PER_TASK = 8

# High-risk terms that warrant human review regardless of context.
# Kept narrow on purpose — false positives are worse than false negatives.
HIGH_RISK_PATTERNS = [
    r"\bpassword[s]?\b",
    r"\bcredential[s]?\b",
    r"\bprivate[_\s]?key[s]?\b",
    r"\bapi[_\s]?key[s]?\b",
    r"\bbilling\b",
    r"\bpayment[s]?\b",
    r"\bstripe[_\s]?secret\b",
    r"\bwebhook[_\s]?secret\b",
    r"\bdrop[_\s]?table\b",
    r"\btruncate[_\s]?table\b",
    r"\biam[_\s]role[s]?\b",
    r"\broot[_\s]access\b",
    r"\.env\s*file",
]

SCHEMA_PATTERNS = [
    r"\bmigrat(?:e|ion)[s]?\b",
    r"\balter\s+table\b",
    r"\bcreate\s+table\b",
    r"\bdrop\s+table\b",
    r"\.sql\b",
    r"\bschema\s+change\b",
]

APP_CODE_EXTENSIONS = [
    r"\.py\b", r"\.js\b", r"\.ts\b", r"\.go\b", r"\.java\b", r"\.rb\b",
]


def _lower(*parts) -> str:
    return " ".join(str(p) for p in parts if p).lower()


# ── Rule 1: File count ────────────────────────────────────────────────────

def check_file_count(plan: dict) -> Optional[str]:
    steps = plan.get("steps", [])
    if isinstance(steps, str):
        try:
            steps = json.loads(steps)
        except Exception:
            return None
    file_steps = [s for s in steps if s.get("tool") == "file_edit"]
    if len(file_steps) > MAX_FILES_PER_TASK:
        return (
            f"Plan writes to {len(file_steps)} files in one task "
            f"(limit {MAX_FILES_PER_TASK}). Split into smaller tasks."
        )
    return None


# ── Rule 2: Schema migration + app code ──────────────────────────────────

def check_schema_with_app_code(plan: dict) -> Optional[str]:
    steps = plan.get("steps", [])
    if isinstance(steps, str):
        try:
            steps = json.loads(steps)
        except Exception:
            return None
    step_text = _lower(*[
        s.get("description", "") + " " + s.get("resource", "")
        for s in steps
    ])
    has_schema = any(re.search(p, step_text) for p in SCHEMA_PATTERNS)
    if not has_schema:
        return None
    has_app = any(re.search(p, step_text) for p in APP_CODE_EXTENSIONS)
    if has_app:
        return (
            "Task combines a schema migration with application code changes. "
            "Separate these into distinct tasks with explicit ordering."
        )
    return None


# ── Rule 3: Sensitive content ─────────────────────────────────────────────

def check_sensitive_content(text: str) -> Optional[str]:
    t = text.lower()
    matched = [p for p in HIGH_RISK_PATTERNS if re.search(p, t)]
    if matched:
        examples = ", ".join(matched[:3])
        return (
            f"Task references high-risk content ({examples}). "
            f"Human review required before autonomous execution."
        )
    return None


# ── Rule 4: Repeated failure class ───────────────────────────────────────

def check_repeated_failure(task: dict) -> Optional[str]:
    code = task.get("failure_code")
    attempt = task.get("attempt", 0)
    if (attempt >= 3
            and code
            and code != FailureCode.HUMAN_DECISION_REQUIRED):
        return (
            f"Task has failed {attempt} time(s) with code '{code}'. "
            f"Autonomous retry is exhausted — human must diagnose."
        )
    return None


# ── Rule 5: Missing acceptance criteria ───────────────────────────────────

def check_missing_criteria(dod: dict) -> Optional[str]:
    criteria = dod.get("acceptance_criteria", [])
    if isinstance(criteria, str):
        try:
            criteria = json.loads(criteria)
        except Exception:
            criteria = []
    if not criteria:
        return (
            "Task has no acceptance criteria. "
            "Cannot verify completion without a Definition of Done."
        )
    return None


# ── Gate functions called by agents ──────────────────────────────────────

def run_planner_checks(task: dict, dod: dict) -> Optional[str]:
    """
    Checks run BEFORE planning (no plan available yet).
    Covers: repeated failure, missing criteria, sensitive content in description.
    """
    for check in (
        lambda: check_repeated_failure(task),
        lambda: check_missing_criteria(dod),
        lambda: check_sensitive_content(
            _lower(task.get("title"), task.get("description"))
        ),
    ):
        result = check()
        if result:
            return result
    return None


def run_builder_checks(task: dict, plan: dict, dod: dict) -> Optional[str]:
    """
    Checks run AFTER planning, BEFORE execution (plan is available).
    Covers: file count, schema+app, sensitive content in plan steps.
    """
    steps = plan.get("steps", [])
    if isinstance(steps, str):
        try:
            steps = json.loads(steps)
        except Exception:
            steps = []
    plan_text = _lower(
        plan.get("notes", ""),
        *[s.get("description", "") + " " + s.get("resource", "")
          for s in steps],
    )
    for check in (
        lambda: check_file_count(plan),
        lambda: check_schema_with_app_code(plan),
        lambda: check_sensitive_content(plan_text),
    ):
        result = check()
        if result:
            return result
    return None


# ── Escalation action ─────────────────────────────────────────────────────

def escalate_task(conn, task_id: str, reason: str,
                  agent_id: str = "escalation") -> None:
    """
    Block a task with HUMAN_DECISION_REQUIRED and notify via outbox.
    Safe to call from any agent — does not require the task to be
    in a specific state (unlike fail_task which expects 'executing').
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET status          = 'blocked',
                leased_by       = NULL,
                leased_until    = NULL,
                blocked_reason  = %s,
                failure_code    = %s,
                updated_at      = NOW()
            WHERE task_id = %s
            RETURNING request_id, title
            """,
            (reason[:500], FailureCode.HUMAN_DECISION_REQUIRED, task_id)
        )
        row = cur.fetchone()
        if not row:
            return

    req_id = row["request_id"]
    title  = row["title"]

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs
                (agent_name, role, action, task_id, request_id, log_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (agent_id, "escalation", "task:human_escalation",
             task_id, req_id, json.dumps({"reason": reason}))
        )

    identity = AGENT_IDENTITY.get("pm", {
        "username": "Chief of Staff", "icon_emoji": ":briefcase:"
    })
    enqueue_outbox(
        conn,
        f"escalation:{task_id}:{FailureCode.HUMAN_DECISION_REQUIRED}",
        "slack_post",
        {
            "channel":    SLACK_TASKS_CHANNEL,
            "text": (
                f":rotating_light: *[ESCALATION] Human review required*"
                f" — `{task_id}`\n"
                f"*{title}*\n"
                f"*Reason:* {reason}\n"
                f"Task is blocked until a human reviews and re-queues it."
            ),
            "username":   identity["username"],
            "icon_emoji": identity["icon_emoji"],
        },
    )
    print(f"  [escalation] {task_id} → HUMAN_DECISION_REQUIRED: {reason[:80]}")
