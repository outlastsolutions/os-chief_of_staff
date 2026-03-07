"""
OSAIO Task Leasing
Outlast Solutions LLC © 2026

Atomic task claiming via FOR UPDATE SKIP LOCKED.
Only one agent can execute a task at a time.
Lease expiry lets dead agents be recovered automatically.
"""

from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Optional


LEASE_MINUTES = 10
MAX_ATTEMPTS  = 3   # task goes BLOCKED after this many failures


def claim_task(conn, agent_id: str, director: Optional[str] = None,
               complexity: Optional[str] = None) -> Optional[dict]:
    """
    Atomically claim the next available planned task.
    Filters by director and/or complexity if provided.
    Returns the task row or None if nothing is available.
    """
    filters = ["status = 'planned'", "(leased_until IS NULL OR leased_until < NOW())"]
    params: list = []

    if director:
        filters.append("assigned_director = %s")
        params.append(director)
    if complexity:
        filters.append("complexity = %s")
        params.append(complexity)

    where = " AND ".join(filters)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH candidate AS (
                SELECT task_id FROM tasks
                WHERE {where}
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE tasks t
            SET status       = 'executing',
                leased_by    = %s,
                leased_until = NOW() + INTERVAL '{LEASE_MINUTES} minutes',
                attempt      = attempt + 1,
                updated_at   = NOW()
            FROM candidate
            WHERE t.task_id = candidate.task_id
            RETURNING t.*
            """,
            params + [agent_id]
        )
        row = cur.fetchone()

    if row:
        _log(conn, agent_id, "builder", "lease:claimed", row["task_id"])
    return dict(row) if row else None


def heartbeat(conn, task_id: str, agent_id: str) -> bool:
    """
    Extend the lease while a task is in progress.
    Call this every ~5 minutes from long-running agents.
    Returns True if the lease was extended, False if we lost it.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE tasks
            SET leased_until = NOW() + INTERVAL '{LEASE_MINUTES} minutes',
                updated_at   = NOW()
            WHERE task_id = %s AND leased_by = %s AND status = 'executing'
            RETURNING task_id
            """,
            (task_id, agent_id)
        )
        ok = cur.fetchone() is not None

    if ok:
        _log(conn, agent_id, "builder", "lease:heartbeat", task_id)
    return ok


def release_to_verifying(conn, task_id: str, agent_id: str) -> bool:
    """
    Builder is done — release lease and advance to 'verifying'.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET status       = 'verifying',
                leased_by    = NULL,
                leased_until = NULL,
                updated_at   = NOW()
            WHERE task_id = %s AND leased_by = %s AND status = 'executing'
            RETURNING task_id
            """,
            (task_id, agent_id)
        )
        ok = cur.fetchone() is not None

    if ok:
        _log(conn, agent_id, "builder", "lease:released→verifying", task_id)
    return ok


def fail_task(conn, task_id: str, agent_id: str, reason: str) -> dict:
    """
    Builder failed. Increment attempt counter.
    If attempt >= MAX_ATTEMPTS, block the task. Otherwise return to planned for retry.
    Returns updated task row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT attempt FROM tasks WHERE task_id = %s", (task_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Task '{task_id}' not found.")

        next_status = "blocked" if row["attempt"] >= MAX_ATTEMPTS else "planned"
        blocked_reason = reason if next_status == "blocked" else None

        # NULL plan_id on task FIRST (FK constraint), then delete the plan
        cur.execute(
            """
            UPDATE tasks
            SET status          = %s,
                leased_by       = NULL,
                leased_until    = NULL,
                blocked_reason  = %s,
                plan_id         = NULL,
                tool_calls_used = 0,
                updated_at      = NOW()
            WHERE task_id = %s
            RETURNING *
            """,
            (next_status, blocked_reason, task_id)
        )
        updated = cur.fetchone()
        if next_status == "planned":
            # Clear plan so planner re-plans with failure context
            cur.execute("DELETE FROM plans WHERE task_id = %s", (task_id,))

    action = f"lease:failed→{next_status}"
    _log(conn, agent_id, "builder", action, task_id,
         {"reason": reason, "attempt": row["attempt"]})
    return dict(updated)


def increment_tool_calls(conn, task_id: str) -> bool:
    """
    Increment tool_calls_used for a task.
    Returns True if still within budget, False if cap exceeded.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tasks
            SET tool_calls_used = tool_calls_used + 1,
                updated_at      = NOW()
            WHERE task_id = %s
            RETURNING tool_calls_used, max_tool_calls
            """,
            (task_id,)
        )
        row = cur.fetchone()

    return row["tool_calls_used"] <= row["max_tool_calls"]


def acquire_resource_lock(conn, lock_key: str, owner: str,
                          minutes: int = 15) -> bool:
    """
    Acquire a resource lock (file, branch, etc).
    Returns True if acquired, False if held by someone else.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO resource_locks (lock_key, owner, leased_until)
            VALUES (%s, %s, NOW() + INTERVAL '%s minutes')
            ON CONFLICT (lock_key) DO UPDATE
                SET owner        = EXCLUDED.owner,
                    leased_until = EXCLUDED.leased_until
            WHERE resource_locks.leased_until < NOW()
            RETURNING lock_key
            """,
            (lock_key, owner, minutes)
        )
        acquired = cur.fetchone() is not None

    return acquired


def release_resource_lock(conn, lock_key: str, owner: str) -> bool:
    """Release a resource lock held by this owner."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM resource_locks WHERE lock_key = %s AND owner = %s RETURNING lock_key",
            (lock_key, owner)
        )
        return cur.fetchone() is not None


def _log(conn, agent_name: str, role: str, action: str,
         task_id: Optional[str] = None, data: Optional[dict] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, task_id, log_data)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (agent_name, role, action, task_id, json.dumps(data or {}))
        )
