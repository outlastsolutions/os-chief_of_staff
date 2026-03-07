"""
Slack intake — polls #tasks for new human messages and creates CoS work requests.
Outlast Solutions LLC © 2026

Uses message `ts` as idempotency key so no separate tracking table is needed.
Each cycle, fetches messages from the last 24h and skips any already in requests.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from config.settings import SLACK_BOT_TOKEN, SLACK_TASKS_CHANNEL
from core.pm import receive_request


_BOT_ID_CACHE: Optional[str] = None


def _web_client():
    from slack_sdk import WebClient
    return WebClient(token=SLACK_BOT_TOKEN)


def _get_bot_user_id() -> Optional[str]:
    global _BOT_ID_CACHE
    if _BOT_ID_CACHE:
        return _BOT_ID_CACHE
    try:
        resp = _web_client().auth_test()
        _BOT_ID_CACHE = resp.get("user_id")
        return _BOT_ID_CACHE
    except Exception:
        return None


def _parse_message(text: str) -> dict:
    """
    Extract title + description from a Slack message.
    First line → title. Remaining lines → description.
    Strips Slack formatting (bold, code, mentions).
    """
    clean = re.sub(r"<@[A-Z0-9]+>", "", text)       # strip @mentions
    clean = re.sub(r"<[^>]+>", "", clean)            # strip links/channels
    clean = re.sub(r"\*([^*]+)\*", r"\1", clean)     # strip bold
    clean = re.sub(r"`([^`]+)`", r"\1", clean)       # strip code
    clean = clean.strip()

    lines = [l.strip() for l in clean.splitlines() if l.strip()]
    if not lines:
        return {}

    title = lines[0][:200]
    description = " ".join(lines[1:])[:2000] if len(lines) > 1 else title

    return {"title": title, "description": description}


def _infer_category(text: str) -> str:
    text_l = text.lower()
    if any(w in text_l for w in ("code", "build", "implement", "api", "script", "module",
                                  "function", "class", "bug", "fix", "feature", "deploy",
                                  "refactor", "test", "repo", "github", "python", "js")):
        return "development"
    if any(w in text_l for w in ("research", "analyze", "analyse", "investigate", "compare",
                                  "market", "competitor", "report", "study")):
        return "research"
    if any(w in text_l for w in ("monitor", "alert", "incident", "ops", "infrastructure",
                                  "server", "database", "backup", "reliability")):
        return "operations"
    if any(w in text_l for w in ("content", "post", "seo", "social", "email", "campaign",
                                  "blog", "marketing", "brand")):
        return "marketing"
    return "development"  # default


def _reply_thread(channel: str, thread_ts: str, text: str) -> None:
    try:
        _web_client().chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=text,
        )
    except Exception as e:
        print(f"  [slack_intake] thread reply failed: {e}")


def ingest(conn, lookback_hours: int = 24) -> int:
    """
    Poll #tasks for recent messages and create work requests for any not yet ingested.
    Returns count of new requests created.
    """
    if not SLACK_BOT_TOKEN:
        return 0

    bot_id = _get_bot_user_id()
    oldest = str((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp())

    try:
        resp = _web_client().conversations_history(
            channel=SLACK_TASKS_CHANNEL,
            oldest=oldest,
            limit=50,
        )
    except Exception as e:
        print(f"  [slack_intake] Slack API error: {e}")
        return 0

    messages = resp.get("messages", [])
    created = 0

    for msg in reversed(messages):  # oldest first
        # Skip bots, system messages, subtypes (joins, etc.)
        if msg.get("subtype"):
            continue
        if msg.get("bot_id"):
            continue
        if bot_id and msg.get("user") == bot_id:
            continue

        text = msg.get("text", "").strip()
        if not text or len(text) < 10:
            continue

        ts = msg.get("ts", "")
        idem_key = f"slack-{ts}"

        parsed = _parse_message(text)
        if not parsed:
            continue

        requester = msg.get("user", "unknown")
        category  = _infer_category(text)

        # Pre-generate a request_id so we can detect whether upsert_request
        # created a new row vs. returned the existing one (atomic idempotency).
        local_req_id = f"REQ-{uuid.uuid4().hex[:8].upper()}"
        req = receive_request(conn, {
            "request_id":      local_req_id,
            "idempotency_key": idem_key,
            "requester":       requester,
            "source":          "slack",
            "channel":         SLACK_TASKS_CHANNEL,
            "thread_ts":       ts,
            "title":           parsed["title"],
            "description":     parsed["description"],
            "category":        category,
            "priority":        "medium",
        })
        is_new = req["request_id"] == local_req_id
        if not is_new:
            continue  # already ingested by another worker — skip confirmation reply

        conn.commit()
        created += 1

        print(f"  [slack_intake] new request {req['request_id']} — {parsed['title'][:60]}")

        # Thread reply to confirm intake — only sent when we actually created the row
        _reply_thread(
            SLACK_TASKS_CHANNEL,
            ts,
            f":briefcase: *Chief of Staff received* — `{req['request_id']}`\n"
            f"Category: `{category}` · Status: scoping…",
        )

    return created
