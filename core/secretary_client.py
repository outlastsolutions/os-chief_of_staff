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

def post_slack(channel: str, text: str, unit: str = "outlast") -> dict:
    """Post a Slack message via Secretary."""
    return call_tool("slack_post", {"channel": channel, "text": text, "unit": unit})


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
