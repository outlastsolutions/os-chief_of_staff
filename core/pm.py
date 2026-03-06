"""
OSAIO Project Manager (PM) Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Receive and validate work requests
  - Use LLM to scope: priority, category, acceptance criteria, systems involved
  - Create scoped request in DB
  - Route to appropriate director
  - Maintain backlog and priorities
  - Transition requests to done/blocked/cancelled
  - Queue Slack notifications via outbox

The PM does NOT decompose tasks (that's APM). It owns outcomes and policy.
"""

from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from config.settings import PM_MODEL, SLACK_TASKS_CHANNEL
from core.llm import chat_json
from core.state_machine import Role, RequestState, transition_request
from core.idempotency import upsert_request, enqueue_outbox


# ── System prompt ────────────────────────────────────────────────────────

PM_SYSTEM = """You are the Project Manager (PM) for Outlast Solutions LLC, an AI-driven company.
Your role is to scope incoming work requests with precision and structure.

Business units: outlast (parent), xout, property_with_peter, low_volt_nyc, one_last, cyberlight

Director domains:
  - development: software, APIs, automation, code, infrastructure builds
  - operations: deployments, monitoring, reliability, SOPs, workflows
  - research: market intelligence, competitive analysis, technical research, regulatory
  - marketing: content, SEO, branding, campaigns, social media

Your job when scoping a request:
1. Determine the correct priority (low / medium / high / critical)
2. Determine the correct category / director domain
3. Write 3-5 clear, testable acceptance criteria
4. List systems likely involved
5. Flag any ambiguities that need clarification before work starts

Be concise. Be specific. Do not hallucinate capabilities or systems.
Respond with valid JSON only."""


# ── Core PM functions ────────────────────────────────────────────────────

def receive_request(conn, raw: dict) -> dict:
    """
    Entry point for all work entering the system.
    Deduplicates via idempotency_key, stores as 'received'.

    raw must include: requester, source, title, description, category
    Optional: business_unit, channel, priority, constraints, deadline,
              idempotency_key (auto-generated if absent)
    """
    if "idempotency_key" not in raw:
        raw["idempotency_key"] = f"req-{uuid.uuid4().hex}"

    request = upsert_request(conn, raw)
    _log(conn, "pm", "request:received", request_id=request["request_id"],
         data={"title": request["title"], "source": request["source"]})
    return request


def scope_request(conn, request_id: str) -> dict:
    """
    PM uses LLM to analyze the request and fill in:
    priority, category, acceptance_criteria, systems_involved.
    Transitions request from 'received' → 'scoped'.
    Returns updated request row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM requests WHERE request_id = %s", (request_id,))
        req = cur.fetchone()
    if not req:
        raise ValueError(f"Request '{request_id}' not found.")

    prompt = f"""Scope this work request:

Title: {req['title']}
Description: {req['description']}
Requester: {req['requester']} via {req['source']}
Business unit: {req.get('business_unit') or 'not specified'}
Existing priority: {req['priority']}
Existing category: {req['category']}

Return JSON with exactly these fields:
{{
  "priority": "low|medium|high|critical",
  "category": "development|operations|research|marketing",
  "assigned_director": "development|operations|research|marketing",
  "acceptance_criteria": ["criterion 1", "criterion 2", ...],
  "systems_involved": ["system1", "system2", ...],
  "ambiguities": ["question 1 if any"],
  "scoping_notes": "brief summary of what this work is and why"
}}"""

    scoping = chat_json(
        model=PM_MODEL,
        system=PM_SYSTEM,
        messages=[{"role": "user", "content": prompt}]
    )

    # Update request with scoped fields
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE requests SET
                priority         = %s,
                category         = %s,
                systems_involved = %s,
                updated_at       = NOW()
            WHERE request_id = %s
            RETURNING *
            """,
            (
                scoping.get("priority", req["priority"]),
                scoping.get("category", req["category"]),
                json.dumps(scoping.get("systems_involved", [])),
                request_id,
            )
        )
        updated = dict(cur.fetchone())

    # Store acceptance criteria as a director report scaffold
    _store_scoping(conn, request_id, scoping)

    final = transition_request(conn, request_id, Role.PM, RequestState.SCOPED)

    _log(conn, "pm", "request:scoped", request_id=request_id, data=scoping)

    # Queue Slack notification
    _notify_slack(conn, request_id, final["title"], scoping)

    return final


def get_backlog(conn, status: Optional[str] = None,
                business_unit: Optional[str] = None) -> list[dict]:
    """
    Return all requests ordered by priority then created_at.
    Optionally filter by status and/or business_unit.
    """
    priority_order = "CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    filters = []
    params = []

    if status:
        filters.append("status = %s")
        params.append(status)
    if business_unit:
        filters.append("business_unit = %s")
        params.append(business_unit)

    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM requests {where} ORDER BY {priority_order}, created_at",
            params
        )
        return [dict(r) for r in cur.fetchall()]


def mark_done(conn, request_id: str, summary: Optional[str] = None) -> dict:
    """PM marks a request as done. Queues final Slack notification."""
    updated = transition_request(conn, request_id, Role.PM, RequestState.DONE)
    _log(conn, "pm", "request:done", request_id=request_id, data={"summary": summary})
    _notify_slack_status(conn, request_id, updated["title"], "done", summary)
    return updated


def mark_blocked(conn, request_id: str, reason: str) -> dict:
    """PM marks a request as blocked with a reason."""
    updated = transition_request(conn, request_id, Role.PM, RequestState.BLOCKED,
                                 blocked_reason=reason)
    _log(conn, "pm", "request:blocked", request_id=request_id, data={"reason": reason})
    _notify_slack_status(conn, request_id, updated["title"], "blocked", reason)
    return updated


def mark_cancelled(conn, request_id: str, reason: Optional[str] = None) -> dict:
    """PM cancels a request."""
    updated = transition_request(conn, request_id, Role.PM, RequestState.CANCELLED)
    _log(conn, "pm", "request:cancelled", request_id=request_id, data={"reason": reason})
    return updated


# ── Internal helpers ──────────────────────────────────────────────────────

def _store_scoping(conn, request_id: str, scoping: dict) -> None:
    """Store acceptance criteria and scoping notes in a director_report row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO director_reports
                (report_id, request_id, director, overall_status, summary)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                f"SCOPE-{uuid.uuid4().hex[:8].upper()}",
                request_id,
                scoping.get("assigned_director", "development"),
                "scoped",
                json.dumps({
                    "acceptance_criteria": scoping.get("acceptance_criteria", []),
                    "ambiguities": scoping.get("ambiguities", []),
                    "notes": scoping.get("scoping_notes", ""),
                })
            )
        )


def _notify_slack(conn, request_id: str, title: str, scoping: dict) -> None:
    criteria = "\n".join(f"  • {c}" for c in scoping.get("acceptance_criteria", []))
    ambiguities = scoping.get("ambiguities", [])
    ambi_text = ("\n⚠️ Ambiguities:\n" + "\n".join(f"  • {a}" for a in ambiguities)) if ambiguities else ""

    text = (
        f"*[PM] New request scoped* — `{request_id}`\n"
        f"*{title}*\n"
        f"Priority: `{scoping.get('priority')}` | Director: `{scoping.get('assigned_director')}`\n"
        f"*Acceptance criteria:*\n{criteria}"
        f"{ambi_text}"
    )
    try:
        from core.secretary_client import notify
        notify(SLACK_TASKS_CHANNEL, text, agent="pm")
    except Exception as e:
        print(f"  [pm] Slack notify skipped (Secretary unavailable): {e}")


def _notify_slack_status(conn, request_id: str, title: str,
                         status: str, detail: Optional[str]) -> None:
    icons = {"done": "✅", "blocked": "🚫", "cancelled": "❌"}
    text = (
        f"{icons.get(status, '•')} *[PM] Request {status}* — `{request_id}`\n"
        f"*{title}*"
        + (f"\n{detail}" if detail else "")
    )
    try:
        from core.secretary_client import notify
        notify(SLACK_TASKS_CHANNEL, text, agent="pm")
    except Exception as e:
        print(f"  [pm] Slack notify skipped (Secretary unavailable): {e}")


def _log(conn, role: str, action: str, request_id: Optional[str] = None,
         task_id: Optional[str] = None, data: Optional[dict] = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO agent_logs (agent_name, role, action, request_id, task_id, log_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("pm", role, action, request_id, task_id, json.dumps(data or {}))
        )
