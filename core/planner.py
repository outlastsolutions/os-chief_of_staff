"""
OSAIO Planner Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Read a planned task + its Definition of Done
  - Use LLM to produce a concrete, step-by-step implementation plan
  - Store the plan in DB and link it to the task
  - The plan is the Builder's instruction set — it must be specific enough
    that the Builder doesn't need to make architectural decisions

Rules:
  - Planner produces plans, never executes
  - Cannot call write-to-repo tools
  - Plan steps must reference specific files, functions, APIs where known
  - Must include a test strategy and risk notes
  - Estimated tool calls must be within the task's max_tool_calls budget
"""

from __future__ import annotations
import json
import uuid
from typing import Optional

from config.settings import PLANNER_MODEL
from core.llm import chat_json


PLANNER_SYSTEM = """You are the Planner Agent for Outlast Solutions LLC.
Your job is to produce detailed, executable implementation plans for tasks.

A plan is a step-by-step instruction set that a Builder agent will follow exactly.
The Builder will not make architectural decisions — your plan must be complete enough
that it can be followed without ambiguity.

Rules:
- Each step must specify: what to do, which tool to use, what file/endpoint/resource is involved, and what the expected output is.
- Reference specific file paths, function names, API endpoints, and data structures where known.
- Include a test strategy: how will the Builder verify their own work before handing to the Auditor?
- List risks: what could go wrong, and how to handle it.
- Keep estimated_tool_calls realistic and within the task budget.
- Tools available: github_api, web_search, file_edit, code_run, slack_api, docs_api, shell

Respond with valid JSON only."""


def plan_task(conn, task_id: str) -> dict:
    """
    Main Planner entry point.
    Reads task + DoD, calls LLM for a plan, stores and links it.
    Returns the created plan dict.
    """
    task, dod = _load_task_with_dod(conn, task_id)

    # Check task is still in a plannable state
    if task["status"] not in ("planned",):
        raise ValueError(
            f"Task must be 'planned' to create a plan. Current: {task['status']}"
        )

    # Check if plan already exists
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
        existing = cur.fetchone()
    if existing:
        return dict(existing)

    prompt = _build_plan_prompt(task, dod, task.get("blocked_reason") or "")

    result = chat_json(
        model=PLANNER_MODEL,
        system=PLANNER_SYSTEM,
        messages=[{"role": "user", "content": prompt}]
    )

    plan = _store_plan(conn, task_id, result, task["max_tool_calls"])

    _log(conn, task_id, task.get("request_id"), {
        "step_count": len(result.get("steps", [])),
        "estimated_tool_calls": result.get("estimated_tool_calls"),
    })

    return plan


def get_plan(conn, task_id: str) -> Optional[dict]:
    """Return the plan for a task, or None if not yet planned."""
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def list_unplanned_tasks(conn, request_id: Optional[str] = None,
                         director: Optional[str] = None) -> list[dict]:
    """
    Return planned tasks that don't yet have a plan.
    Optionally filter by request or director.
    """
    filters = ["t.status = 'planned'", "t.plan_id IS NULL"]
    params = []

    if request_id:
        filters.append("t.request_id = %s")
        params.append(request_id)
    if director:
        filters.append("t.assigned_director = %s")
        params.append(director)

    where = " AND ".join(filters)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT t.* FROM tasks t WHERE {where} ORDER BY t.created_at",
            params
        )
        return [dict(r) for r in cur.fetchall()]


# ── Internal helpers ──────────────────────────────────────────────────────

def _load_task_with_dod(conn, task_id: str) -> tuple[dict, dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM tasks WHERE task_id = %s", (task_id,))
        task = cur.fetchone()
        if not task:
            raise ValueError(f"Task '{task_id}' not found.")

        cur.execute("SELECT * FROM definitions_of_done WHERE task_id = %s", (task_id,))
        dod_row = cur.fetchone()

    dod = {}
    if dod_row:
        dod = {
            "goal": dod_row["goal"],
            "acceptance_criteria": _parse_json(dod_row["acceptance_criteria"]),
            "constraints": _parse_json(dod_row["constraints"]),
            "evidence_required": _parse_json(dod_row["evidence_required"]),
            "security_checks": _parse_json(dod_row["security_checks"]),
            "rollback_plan": dod_row["rollback_plan"],
        }

    return dict(task), dod


def _build_plan_prompt(task: dict, dod: dict, prior_issues: str = "") -> str:
    tools = _parse_json(task.get("tools_allowed", "[]"))
    deps  = _parse_json(task.get("dependencies", "[]"))

    criteria = "\n".join(f"  - {c}" for c in dod.get("acceptance_criteria", []))
    constraints = "\n".join(f"  - {c}" for c in dod.get("constraints", []))
    evidence = "\n".join(f"  - {e}" for e in dod.get("evidence_required", []))
    security = "\n".join(f"  - {s}" for s in dod.get("security_checks", []))

    return f"""Create a detailed implementation plan for this task.

Task ID: {task['task_id']}
Title: {task['title']}
Description: {task['description']}
Director: {task['assigned_director']}
Complexity: {task['complexity']}
Tools allowed: {', '.join(tools)}
Tool budget: {task['max_tool_calls']} max tool calls
Dependencies completed: {deps or 'none'}

Definition of Done:
  Goal: {dod.get('goal', task['description'])}
  Acceptance criteria:
{criteria or '  (none)'}
  Constraints:
{constraints or '  (none)'}
  Evidence required:
{evidence or '  (none)'}
  Security checks:
{security or '  (none)'}

{f"Previous attempt failed. Auditor issues to resolve:{chr(10)}{prior_issues}{chr(10)}" if prior_issues else ""}Return JSON in exactly this format:
{{
  "steps": [
    {{
      "order": 1,
      "title": "Short step title",
      "description": "Exact description of what to do",
      "tool": "file_edit|code_run|github_api|web_search|shell|slack_api|docs_api|none",
      "resource": "specific file path, URL, API endpoint, or 'n/a'",
      "expected_output": "What success looks like for this step",
      "risk": "What could go wrong (or 'low' if minimal risk)"
    }}
  ],
  "test_strategy": "How the Builder should verify their work before handing to Auditor",
  "risks": ["overall risk 1", "overall risk 2"],
  "estimated_tool_calls": 12,
  "notes": "Any important context or caveats for the Builder"
}}"""


def _store_plan(conn, task_id: str, result: dict, max_tool_calls: int) -> dict:
    plan_id = f"PLAN-{uuid.uuid4().hex[:8].upper()}"

    estimated = result.get("estimated_tool_calls", max_tool_calls)
    if estimated > max_tool_calls:
        # Warn in notes but don't fail — Builder will enforce budget
        result["notes"] = (
            f"[BUDGET WARNING: plan estimates {estimated} tool calls "
            f"but budget is {max_tool_calls}] "
        ) + (result.get("notes") or "")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plans (
                plan_id, task_id, steps, test_strategy,
                risks, estimated_tool_calls, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (task_id) DO NOTHING
            RETURNING *
            """,
            (
                plan_id,
                task_id,
                json.dumps(result.get("steps", [])),
                result.get("test_strategy"),
                json.dumps(result.get("risks", [])),
                result.get("estimated_tool_calls"),
                result.get("notes"),
            )
        )
        plan_row = cur.fetchone()

        if plan_row:
            cur.execute(
                "UPDATE tasks SET plan_id = %s, updated_at = NOW() WHERE task_id = %s",
                (plan_id, task_id)
            )
            return dict(plan_row)
        else:
            # Race: plan was already inserted
            cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
            return dict(cur.fetchone())


def _parse_json(val) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def _log(conn, task_id: str, request_id: Optional[str], data: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, task_id, request_id, log_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("planner", "planner", "plan:created", task_id, request_id,
             json.dumps(data))
        )
