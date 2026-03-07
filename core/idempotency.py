"""
OSAIO Idempotency
Outlast Solutions LLC © 2026

Deduplication for work requests and outbound side effects.
Every request entering the system carries an idempotency_key.
Every Slack post / GitHub comment goes through the outbox.
"""

from __future__ import annotations
import json
import uuid
from typing import Optional


def upsert_request(conn, request_data: dict) -> dict:
    """
    Insert a new work request, or return the existing one if the
    idempotency_key has been seen before. Safe to call on retries.

    request_data must include: idempotency_key, requester, source,
    title, description, category. All other fields optional.
    """
    request_id = request_data.get("request_id") or f"REQ-{uuid.uuid4().hex[:8].upper()}"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO requests (
                request_id, idempotency_key, requester, source, channel, thread_ts,
                business_unit, title, description, priority, category,
                constraints, systems_involved, attachments, deadline
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (idempotency_key) DO UPDATE
                SET updated_at = requests.updated_at   -- no-op touch
            RETURNING *
            """,
            (
                request_id,
                request_data["idempotency_key"],
                request_data["requester"],
                request_data["source"],
                request_data.get("channel"),
                request_data.get("thread_ts"),
                request_data.get("business_unit"),
                request_data["title"],
                request_data["description"],
                request_data.get("priority", "medium"),
                request_data["category"],
                json.dumps(request_data.get("constraints", [])),
                json.dumps(request_data.get("systems_involved", [])),
                json.dumps(request_data.get("attachments", [])),
                request_data.get("deadline"),
            )
        )
        row = cur.fetchone()

    return dict(row)


def enqueue_outbox(conn, dedupe_key: str, type_: str, payload: dict) -> Optional[int]:
    """
    Write a side effect to the outbox (Slack post, GitHub comment, etc).
    If dedupe_key already exists, returns None (already queued or sent).
    Returns outbox_id if newly inserted.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO outbox (dedupe_key, type, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (dedupe_key) DO NOTHING
            RETURNING outbox_id
            """,
            (dedupe_key, type_, json.dumps(payload))
        )
        row = cur.fetchone()

    return row["outbox_id"] if row else None


def claim_pending_outbox(conn, limit: int = 10) -> list[dict]:
    """
    Claim a batch of pending outbox items for the sender worker.
    Marks them 'sending' so no other worker picks them up.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox
            SET status = 'sending'
            WHERE outbox_id IN (
                SELECT outbox_id FROM outbox
                WHERE status = 'pending'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            RETURNING *
            """,
            (limit,)
        )
        rows = cur.fetchall()

    return [dict(r) for r in rows]


def mark_outbox_sent(conn, outbox_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE outbox SET status = 'sent', sent_at = NOW() WHERE outbox_id = %s",
            (outbox_id,)
        )


def mark_outbox_failed(conn, outbox_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE outbox SET status = 'failed' WHERE outbox_id = %s",
            (outbox_id,)
        )
