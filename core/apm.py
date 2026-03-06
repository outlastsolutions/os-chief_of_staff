"""
OSAIO Assistant Project Manager (APM) Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Decompose a scoped request into 3-10 tasks
  - Attach a Definition of Done to every task
  - Set tool budgets, retry caps, complexity, dependencies
  - Transition request from scoped → in_progress
  - Monitor task progress and escalate failures to PM
  - Report status back up the chain

The APM does NOT execute tasks (that's Planner/Builder/Auditor).
It owns the task graph for a request.
"""

from __future__ import annotations
import json
import uuid
from typing import Optional

from config.settings import APM_MODEL, SLACK_TASKS_CHANNEL
from core.llm import chat_json
from core.state_machine import Role, RequestState, transition_request
from core.idempotency import enqueue_outbox


# ── System prompt ─────────────────────────────────────────────────────────

APM_SYSTEM = """You are the Assistant Project Manager (APM) for Outlast Solutions LLC.
Your job is to decompose a scoped work request into a minimal, executable task graph.

Rules:
- Use the FEWEST tasks possible. Most requests need 1-3 tasks. Never exceed 5.
- A task is one coherent unit of work one agent can complete end-to-end. Do NOT split
  "research" and "write" or "search" and "save" into separate tasks — that is one task.
- Only create a separate task when it has a genuinely different owner, tool set, or
  can run in parallel with another task. Prep/clarification/validation sub-steps belong
  INSIDE a single task's plan, not as separate tasks.
- Bad decomposition: Search web | Evaluate results | Write summary | Save file (4 tasks)
- Good decomposition: Research and document findings (1 task)
- Every task needs a clear Definition of Done with testable acceptance criteria.
- Assign complexity: low (simple script/doc/search), medium (moderate code/research), high (complex architecture).
- Assign the correct director domain: development | operations | research | marketing
- List tools the executor will need: github_api | web_search | file_edit | code_run | slack_api | docs_api | shell
- Specify dependencies only when a task genuinely cannot start until another finishes.
- Evidence required must match the task type:
    code tasks → ["file_paths", "test_output"]
    research tasks → ["source_notes"]
    content tasks → ["draft"]

Respond with valid JSON only."""


# ── Core APM functions ────────────────────────────────────────────────────

def decompose_request(conn, request_id: str) -> list[dict]:
    """
    Main APM entry point. Takes a scoped request and:
    1. Reads request + acceptance criteria
    2. Uses LLM to produce a task graph
    3. Persists tasks + DoDs to DB
    4. Transitions request to in_progress
    Returns list of created task dicts.
    """
    request, scoping = _load_request_with_scoping(conn, request_id)

    prompt = _build_decomposition_prompt(request, scoping)

    result = chat_json(
        model=APM_MODEL,
        system=APM_SYSTEM,
        messages=[{"role": "user", "content": prompt}]
    )

    tasks_data = result.get("tasks", [])
    if not tasks_data:
        raise ValueError("APM returned no tasks. Raw result: " + json.dumps(result))

    created_tasks = []
    title_to_id: dict[str, str] = {}

    # First pass — create all tasks (no dependencies yet)
    for t in tasks_data:
        task_id = f"TASK-{uuid.uuid4().hex[:8].upper()}"
        title_to_id[t["title"]] = task_id

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (
                    task_id, request_id, created_by, assigned_director,
                    title, description, tools_allowed, complexity,
                    max_tool_calls, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (request_id, title) DO NOTHING
                RETURNING *
                """,
                (
                    task_id,
                    request_id,
                    "apm",
                    t.get("assigned_director", "development"),
                    t["title"],
                    t["description"],
                    json.dumps(t.get("tools_allowed", [])),
                    t.get("complexity", "medium"),
                    _max_tool_calls(t.get("complexity", "medium")),
                )
            )
            row = cur.fetchone()
            if row:
                created_tasks.append(dict(row))
                _create_dod(conn, task_id, t)
            else:
                # already exists — fetch it
                cur.execute("SELECT * FROM tasks WHERE request_id = %s AND title = %s",
                            (request_id, t["title"]))
                existing = cur.fetchone()
                if existing:
                    created_tasks.append(dict(existing))
                    title_to_id[t["title"]] = existing["task_id"]

    # Second pass — wire up dependencies
    for t in tasks_data:
        dep_ids = [title_to_id[dep] for dep in t.get("dependencies", [])
                   if dep in title_to_id]
        if dep_ids:
            tid = title_to_id[t["title"]]
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET dependencies = %s WHERE task_id = %s",
                    (json.dumps(dep_ids), tid)
                )

    # Transition request → in_progress
    transition_request(conn, request_id, Role.APM, RequestState.IN_PROGRESS)

    _log(conn, "apm", "request:decomposed", request_id=request_id,
         data={"task_count": len(created_tasks),
                "tasks": [t["title"] for t in created_tasks]})

    _notify_slack(conn, request_id, request["title"], created_tasks)

    return created_tasks


def get_request_status(conn, request_id: str) -> dict:
    """
    Aggregate task status for a request.
    Returns a summary dict the APM uses for escalation decisions.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM requests WHERE request_id = %s", (request_id,))
        request = dict(cur.fetchone())

        cur.execute(
            """
            SELECT status, COUNT(*) as count
            FROM tasks WHERE request_id = %s
            GROUP BY status
            """,
            (request_id,)
        )
        status_counts = {r["status"]: r["count"] for r in cur.fetchall()}

        cur.execute(
            "SELECT * FROM tasks WHERE request_id = %s ORDER BY created_at",
            (request_id,)
        )
        tasks = [dict(r) for r in cur.fetchall()]

    total = sum(status_counts.values())
    done  = status_counts.get("done", 0)
    blocked = status_counts.get("blocked", 0)
    failed_tasks = [t for t in tasks if t["status"] == "blocked"]

    return {
        "request_id": request_id,
        "request_title": request["title"],
        "request_status": request["status"],
        "total_tasks": total,
        "done": done,
        "blocked": blocked,
        "in_flight": status_counts.get("executing", 0),
        "pending": status_counts.get("planned", 0),
        "progress_pct": round(done / total * 100) if total else 0,
        "blocked_tasks": [{"task_id": t["task_id"], "title": t["title"],
                           "reason": t.get("blocked_reason")} for t in failed_tasks],
        "tasks": tasks,
    }


def check_escalations(conn, request_id: str) -> list[dict]:
    """
    Check for tasks that need escalation to PM:
    - blocked tasks → surface to PM
    - expired leases → reclaim or block
    Returns list of tasks that were escalated.
    """
    escalated = []

    with conn.cursor() as cur:
        # Tasks with expired leases still marked executing
        cur.execute(
            """
            UPDATE tasks
            SET status = 'planned',
                leased_by = NULL,
                leased_until = NULL,
                updated_at = NOW()
            WHERE request_id = %s
              AND status = 'executing'
              AND leased_until < NOW()
            RETURNING *
            """,
            (request_id,)
        )
        reclaimed = [dict(r) for r in cur.fetchall()]
        for t in reclaimed:
            _log(conn, "apm", "task:lease_reclaimed", request_id=request_id,
                 task_id=t["task_id"], data={"title": t["title"]})

        # Blocked tasks with too many attempts → escalate to PM
        cur.execute(
            """
            SELECT * FROM tasks
            WHERE request_id = %s AND status = 'blocked'
            """,
            (request_id,)
        )
        blocked = [dict(r) for r in cur.fetchall()]

    for t in blocked:
        escalated.append(t)
        _log(conn, "apm", "task:escalated_to_pm", request_id=request_id,
             task_id=t["task_id"],
             data={"reason": t.get("blocked_reason"), "attempts": t["attempt"]})

    return escalated


def get_next_ready_tasks(conn, request_id: str) -> list[dict]:
    """
    Return tasks whose dependencies are all done and that are still planned.
    These are ready to be claimed by Planner/Builder agents.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM tasks WHERE request_id = %s AND status = 'planned'",
            (request_id,)
        )
        planned = [dict(r) for r in cur.fetchall()]

        cur.execute(
            "SELECT task_id FROM tasks WHERE request_id = %s AND status = 'done'",
            (request_id,)
        )
        done_ids = {r["task_id"] for r in cur.fetchall()}

    ready = []
    for task in planned:
        deps = task.get("dependencies") or []
        if isinstance(deps, str):
            deps = json.loads(deps)
        if all(d in done_ids for d in deps):
            ready.append(task)

    return ready


# ── Internal helpers ──────────────────────────────────────────────────────

def _load_request_with_scoping(conn, request_id: str) -> tuple[dict, dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM requests WHERE request_id = %s", (request_id,))
        req = cur.fetchone()
        if not req:
            raise ValueError(f"Request '{request_id}' not found.")
        if req["status"] not in ("scoped", "in_progress"):
            raise ValueError(
                f"Request must be 'scoped' before decomposition. Current: {req['status']}"
            )

        cur.execute(
            "SELECT summary FROM director_reports WHERE request_id = %s ORDER BY created_at LIMIT 1",
            (request_id,)
        )
        report = cur.fetchone()

    scoping = {}
    if report and report["summary"]:
        try:
            scoping = json.loads(report["summary"])
        except (json.JSONDecodeError, TypeError):
            scoping = {}

    return dict(req), scoping


def _build_decomposition_prompt(request: dict, scoping: dict) -> str:
    criteria = "\n".join(f"  - {c}" for c in scoping.get("acceptance_criteria", []))
    ambiguities = "\n".join(f"  - {a}" for a in scoping.get("ambiguities", []))

    # Rough task count hint based on description length and complexity
    desc_len = len(request.get('description', ''))
    if desc_len < 200:
        task_hint = "This is a small request. Aim for 1 task, 2 at most."
    elif desc_len < 500:
        task_hint = "This is a medium request. Aim for 2-3 tasks."
    else:
        task_hint = "This is a larger request. Use up to 5 tasks, no more."

    return f"""Decompose this work request into a MINIMAL executable task graph.

Request ID: {request['request_id']}
Title: {request['title']}
Description: {request['description']}
Business unit: {request.get('business_unit') or 'not specified'}
Priority: {request['priority']}
Category: {request['category']}

Acceptance criteria:
{criteria or '  (none provided)'}

Known ambiguities to account for:
{ambiguities or '  (none)'}

Sizing guidance: {task_hint}
Remember: sub-steps (search, evaluate, write, save) belong INSIDE one task — not as separate tasks.

Return JSON in exactly this format:
{{
  "tasks": [
    {{
      "title": "Short task title (unique within this request)",
      "description": "Detailed description of exactly what this task does",
      "assigned_director": "development|operations|research|marketing",
      "complexity": "low|medium|high",
      "tools_allowed": ["github_api", "web_search", "file_edit", "code_run", "slack_api", "docs_api", "shell"],
      "dependencies": ["Title of task that must complete first"],
      "definition_of_done": {{
        "goal": "One sentence goal",
        "acceptance_criteria": ["criterion 1", "criterion 2"],
        "constraints": ["constraint 1"],
        "evidence_required": ["git_diff", "test_output"],
        "security_checks": ["no credentials exposed"]
      }}
    }}
  ]
}}"""


def _create_dod(conn, task_id: str, task_data: dict) -> None:
    dod = task_data.get("definition_of_done", {})
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO definitions_of_done (
                dod_id, task_id, goal, acceptance_criteria,
                constraints, evidence_required, security_checks, rollback_plan
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (task_id) DO NOTHING
            """,
            (
                f"DOD-{uuid.uuid4().hex[:8].upper()}",
                task_id,
                dod.get("goal", task_data.get("description", "")),
                json.dumps(dod.get("acceptance_criteria", [])),
                json.dumps(dod.get("constraints", [])),
                json.dumps(dod.get("evidence_required", [])),
                json.dumps(dod.get("security_checks", [])),
                dod.get("rollback_plan"),
            )
        )


def _max_tool_calls(complexity: str) -> int:
    return {"low": 10, "medium": 20, "high": 40}.get(complexity, 20)


def _notify_slack(conn, request_id: str, title: str, tasks: list[dict]) -> None:
    task_lines = "\n".join(
        f"  `{t['task_id']}` {t['title']} [{t.get('complexity','?')}]"
        for t in tasks
    )
    text = (
        f"*[APM] Request decomposed* — `{request_id}`\n"
        f"*{title}*\n"
        f"{len(tasks)} tasks created:\n{task_lines}"
    )
    enqueue_outbox(conn,
        dedupe_key=f"slack:apm:decomposed:{request_id}",
        type_="slack_post",
        payload={"channel": SLACK_TASKS_CHANNEL, "text": text}
    )


def _log(conn, role: str, action: str, request_id: Optional[str] = None,
         task_id: Optional[str] = None, data: Optional[dict] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, request_id, task_id, log_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("apm", role, action, request_id, task_id, json.dumps(data or {}))
        )
