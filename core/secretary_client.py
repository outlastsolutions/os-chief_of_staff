"""
Secretary API client for Chief of Staff.
Outlast Solutions LLC © 2026

Thin HTTP wrapper around Secretary's /tools/ and /chat/ endpoints.
All agent-to-Secretary calls go through here.
"""
from __future__ import annotations
import urllib.request
import urllib.error
import json
from typing import Any, Optional

from config.settings import SECRETARY_URL, SECRETARY_API_KEY


def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if SECRETARY_API_KEY:
        h["Authorization"] = f"Bearer {SECRETARY_API_KEY}"
    return h


def _request(method: str, path: str, body: Any = None, timeout: int = 15) -> dict:
    url = SECRETARY_URL.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Secretary API {method} {path} → {e.code}: {body[:200]}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Secretary unreachable at {url}: {e.reason}")


# ── Health ─────────────────────────────────────────────────────────────────

def ping() -> bool:
    """Returns True if Secretary is reachable."""
    try:
        _request("GET", "/health")
        return True
    except Exception:
        return False


# ── Tools ──────────────────────────────────────────────────────────────────

def call_tool(name: str, input: dict[str, Any] = None) -> dict:
    """Call a Secretary tool by name. Returns the tool result dict."""
    return _request("POST", f"/tools/{name}", {"input": input or {}})


# ── Convenience wrappers ───────────────────────────────────────────────────

def post_slack(channel: str, text: str,
               username: str = None, icon_emoji: str = None,
               thread_ts: str = None) -> dict:
    """Post a Slack message via Secretary.
    username overrides the display name (e.g. 'Builder', 'Chief of Staff').
    icon_emoji sets the avatar (e.g. ':robot_face:', ':briefcase:').
    """
    payload = {"channel": channel, "text": text}
    if username:
        payload["username"] = username
    if icon_emoji:
        payload["icon_emoji"] = icon_emoji
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return call_tool("slack_post_message", payload)


def send_slack_dm(user_id: str, text: str) -> dict:
    """Send a Slack DM via Secretary."""
    return call_tool("slack_dm", {"user_id": user_id, "text": text})


def send_email(to: str, subject: str, body: str, unit: str = "outlast") -> dict:
    """Send an email via Secretary."""
    return call_tool("send_email", {"to": to, "subject": subject, "body": body, "unit": unit})


def create_task(title: str, description: str = "", unit: str = "outlast",
                priority: str = "medium", assigned_to: str = None) -> dict:
    """Create a human-facing task in Secretary's task system."""
    payload = {"title": title, "description": description,
               "unit": unit, "priority": priority}
    if assigned_to:
        payload["assigned_to"] = assigned_to
    return call_tool("create_task", payload)


def file_to_drive(title: str, content: str, unit: str = "outlast") -> dict:
    """Create a Google Doc in Secretary's Drive."""
    return call_tool("docs_create", {"title": title, "content": content, "unit": unit})


# ── Work Request (structured delegation) ───────────────────────────────────

def work_request(action: str, payload: dict = None,
                 task_id: str = None, note: str = None,
                 source: str = "chief_of_staff") -> dict:
    """
    Submit a structured work request to Secretary.
    Returns the WorkResponse dict.
    Raises RuntimeError if Secretary returns an error.
    """
    body = {
        "action": action,
        "payload": payload or {},
        "source": source,
    }
    if task_id:
        body["task_id"] = task_id
    if note:
        body["note"] = note
    return _request("POST", "/work/", body)


# ── Agent Slack identities ─────────────────────────────────────────────────
# Standard username/icon_emoji pairs for each OSAIO agent.
# Pass these to post_slack() so messages are identifiable in Slack.

AGENT_IDENTITY = {
    "pm":        {"username": "Chief of Staff · PM",      "icon_emoji": ":briefcase:"},
    "apm":       {"username": "Chief of Staff · APM",     "icon_emoji": ":clipboard:"},
    "planner":   {"username": "Planner",                  "icon_emoji": ":pencil:"},
    "builder":   {"username": "Builder",                  "icon_emoji": ":hammer:"},
    "auditor":   {"username": "Auditor",                  "icon_emoji": ":white_check_mark:"},
    "secretary": {"username": "Secretary",                "icon_emoji": ":envelope:"},
}


def notify(channel: str, text: str, agent: str = "pm",
           thread_ts: str = None, task_id: str = None) -> dict:
    """
    Post a Slack notification via Secretary with the correct agent identity.
    agent: one of pm | apm | planner | builder | auditor | secretary
    """
    identity = AGENT_IDENTITY.get(agent, {"username": agent.title(), "icon_emoji": ":robot_face:"})
    return work_request(
        action="slack_post_message",
        payload={
            "channel": channel,
            "text": text,
            "username": identity["username"],
            "icon_emoji": identity["icon_emoji"],
            **({"thread_ts": thread_ts} if thread_ts else {}),
        },
        task_id=task_id,
        source=agent,
        note=f"Notification from {agent}",
    )
