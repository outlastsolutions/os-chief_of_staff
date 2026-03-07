"""
OSAIO Task State Machine
Outlast Solutions LLC © 2026

Every task lives in exactly one state. Only authorized roles can move it.
No agent invents transitions. No silent infinite retries.

States:
    planned    → APM created the task, not yet claimed by Planner
    executing  → Builder holds the lease and is working
    verifying  → Auditor is checking the Builder's output
    done       → PM marked complete (evidence + audit passed)
    blocked    → Any agent surfaced an unresolvable blocker
    cancelled  → PM cancelled

Requests have their own lighter state set:
    received → scoped → in_progress → done | blocked | cancelled
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

# ─────────────────────────────────────────────
# State definitions
# ─────────────────────────────────────────────

class TaskState:
    PLANNED    = "planned"
    EXECUTING  = "executing"
    VERIFYING  = "verifying"
    DONE       = "done"
    BLOCKED    = "blocked"
    CANCELLED  = "cancelled"


class RequestState:
    RECEIVED   = "received"
    SCOPED     = "scoped"
    IN_PROGRESS = "in_progress"
    DONE       = "done"
    BLOCKED    = "blocked"
    CANCELLED  = "cancelled"


class Role:
    PM        = "pm"
    APM       = "apm"
    DIRECTOR  = "director"
    PLANNER   = "planner"
    BUILDER   = "builder"
    AUDITOR   = "auditor"
    SECRETARY = "secretary"


# ─────────────────────────────────────────────
# Allowed task transitions
# role → {from_state: [allowed_to_states]}
# ─────────────────────────────────────────────

TASK_TRANSITIONS: dict[str, dict[str, list[str]]] = {
    Role.APM: {
        TaskState.PLANNED:   [TaskState.BLOCKED, TaskState.CANCELLED],
    },
    Role.PLANNER: {
        TaskState.PLANNED:   [TaskState.EXECUTING],  # planner claims + starts
    },
    Role.BUILDER: {
        TaskState.EXECUTING: [TaskState.VERIFYING, TaskState.BLOCKED],
    },
    Role.AUDITOR: {
        TaskState.VERIFYING: [TaskState.DONE, TaskState.PLANNED],
        # DONE = pass, PLANNED = fail (re-queue for any builder to retry with issues noted)
    },
    Role.PM: {
        # PM can move anything to done/blocked/cancelled
        TaskState.PLANNED:   [TaskState.DONE, TaskState.BLOCKED, TaskState.CANCELLED],
        TaskState.EXECUTING: [TaskState.DONE, TaskState.BLOCKED, TaskState.CANCELLED],
        TaskState.VERIFYING: [TaskState.DONE, TaskState.BLOCKED, TaskState.CANCELLED],
        TaskState.BLOCKED:   [TaskState.PLANNED, TaskState.CANCELLED],
    },
    Role.DIRECTOR: {
        # Directors can reject back to executing or block
        TaskState.VERIFYING: [TaskState.EXECUTING, TaskState.BLOCKED],
        TaskState.PLANNED:   [TaskState.BLOCKED],
    },
}

# ─────────────────────────────────────────────
# Request transitions
# ─────────────────────────────────────────────

REQUEST_TRANSITIONS: dict[str, dict[str, list[str]]] = {
    Role.PM: {
        RequestState.RECEIVED:    [RequestState.SCOPED, RequestState.BLOCKED, RequestState.CANCELLED],
        RequestState.SCOPED:      [RequestState.IN_PROGRESS, RequestState.BLOCKED, RequestState.CANCELLED],
        RequestState.IN_PROGRESS: [RequestState.DONE, RequestState.BLOCKED, RequestState.CANCELLED],
        RequestState.BLOCKED:     [RequestState.IN_PROGRESS, RequestState.CANCELLED],
    },
    Role.APM: {
        RequestState.SCOPED:      [RequestState.IN_PROGRESS],
    },
    Role.SECRETARY: {
        RequestState.RECEIVED:    [RequestState.SCOPED],  # secretary can log + forward
    },
}


# ─────────────────────────────────────────────
# Transition enforcement
# ─────────────────────────────────────────────

class TransitionError(Exception):
    pass


def validate_task_transition(role: str, from_state: str, to_state: str) -> None:
    """Raise TransitionError if the role cannot move a task from from_state to to_state."""
    allowed = TASK_TRANSITIONS.get(role, {}).get(from_state, [])
    if to_state not in allowed:
        raise TransitionError(
            f"Role '{role}' cannot move task from '{from_state}' to '{to_state}'. "
            f"Allowed: {allowed or 'none'}"
        )


def validate_request_transition(role: str, from_state: str, to_state: str) -> None:
    """Raise TransitionError if the role cannot move a request from from_state to to_state."""
    allowed = REQUEST_TRANSITIONS.get(role, {}).get(from_state, [])
    if to_state not in allowed:
        raise TransitionError(
            f"Role '{role}' cannot move request from '{from_state}' to '{to_state}'. "
            f"Allowed: {allowed or 'none'}"
        )


# ─────────────────────────────────────────────
# DB-backed transition functions
# ─────────────────────────────────────────────

def transition_task(conn, task_id: str, role: str, to_state: str,
                    blocked_reason: Optional[str] = None) -> dict:
    """
    Move a task to a new state.
    Validates role permission, then writes to DB atomically.
    Returns the updated task row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT task_id, status FROM tasks WHERE task_id = %s FOR UPDATE", (task_id,))
        task = cur.fetchone()
        if not task:
            raise ValueError(f"Task '{task_id}' not found.")

        from_state = task["status"]
        validate_task_transition(role, from_state, to_state)

        cur.execute(
            """
            UPDATE tasks
            SET status = %s,
                blocked_reason = %s,
                updated_at = NOW()
            WHERE task_id = %s
            RETURNING *
            """,
            (to_state, blocked_reason, task_id)
        )
        updated = cur.fetchone()

    _log_transition(conn, role=role, entity="task", entity_id=task_id,
                    from_state=from_state, to_state=to_state, reason=blocked_reason)
    return dict(updated)


def transition_request(conn, request_id: str, role: str, to_state: str,
                       blocked_reason: Optional[str] = None) -> dict:
    """
    Move a request to a new state.
    Validates role permission, then writes to DB atomically.
    Returns the updated request row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT request_id, status FROM requests WHERE request_id = %s FOR UPDATE", (request_id,))
        req = cur.fetchone()
        if not req:
            raise ValueError(f"Request '{request_id}' not found.")

        from_state = req["status"]
        validate_request_transition(role, from_state, to_state)

        cur.execute(
            """
            UPDATE requests
            SET status = %s,
                blocked_reason = %s,
                updated_at = NOW()
            WHERE request_id = %s
            RETURNING *
            """,
            (to_state, blocked_reason, request_id)
        )
        updated = cur.fetchone()

    _log_transition(conn, role=role, entity="request", entity_id=request_id,
                    from_state=from_state, to_state=to_state, reason=blocked_reason)
    return dict(updated)


def _log_transition(conn, role: str, entity: str, entity_id: str,
                    from_state: str, to_state: str, reason: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, task_id, request_id, log_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                role,
                role,
                f"transition:{entity}:{from_state}→{to_state}",
                entity_id if entity == "task" else None,
                entity_id if entity == "request" else None,
                __import__("json").dumps({"from": from_state, "to": to_state, "reason": reason})
            )
        )
