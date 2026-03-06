"""
OSAIO Builder Agent — v0.1
Outlast Solutions LLC © 2026

Responsibilities:
  - Claim a planned task that has a plan attached
  - Generate an execution manifest (single LLM call for all steps)
  - Execute each step using the appropriate tool handler
  - Enforce tool call budget
  - Produce a structured Execution Report
  - Transition task to verifying on success, or fail_task on error

Design:
  One LLM call per task (not per step) to keep costs low.
  The LLM produces exact commands/content for every step upfront.
  Execution is then deterministic — no LLM in the inner loop.
"""

from __future__ import annotations
import json
import os
import uuid
import subprocess
import traceback
from datetime import datetime, timezone
from typing import Optional

from config.settings import BUILDER_MODEL
from core.llm import chat_json
from core.lease import (
    claim_task, heartbeat, release_to_verifying,
    fail_task, increment_tool_calls,
)


BUILDER_SYSTEM = """You are the Builder Agent for Outlast Solutions LLC.
Given a task plan, produce an execution manifest: for each step, the exact
content, command, or code to execute.

Rules:
- Be precise and literal. The manifest is executed directly.
- For file_edit steps: provide the full file content to write.
- For code_run steps: provide the exact Python code to run.
- For shell steps: provide the exact shell command.
- For web_search steps: provide the exact search query string.
- For docs_api steps: provide the document title and content.
- For none/thinking steps: provide a brief note (no tool action needed).
- Do NOT include markdown code fences in file content or code.
- Paths should be relative to the workspace root unless absolute is required.

Respond with valid JSON only."""


# ── Public interface ──────────────────────────────────────────────────────

def execute_task(conn, agent_id: str,
                 director: Optional[str] = None,
                 task_id: Optional[str] = None) -> Optional[dict]:
    """
    Claim and execute one task.
    If task_id is given, targets that specific task.
    Otherwise claims the next available planned+planned task.
    Returns the execution report dict, or None if nothing to claim.
    """
    task = _claim(conn, agent_id, director, task_id)
    if not task:
        return None

    tid = task["task_id"]
    print(f"  [builder:{agent_id}] claimed {tid} — {task['title']}")

    try:
        plan, dod = _load_plan_and_dod(conn, tid)
        if not plan:
            raise ValueError("Task has no plan. Run planner first.")

        manifest = _generate_manifest(task, plan, dod)
        artifacts, logs = _execute_manifest(conn, tid, agent_id, manifest, plan)

        report = _create_report(conn, tid, agent_id, "completed", artifacts, logs)
        release_to_verifying(conn, tid, agent_id)
        print(f"  [builder:{agent_id}] {tid} → verifying ({len(artifacts)} artifacts)")
        return report

    except Exception as e:
        tb = traceback.format_exc()
        print(f"  [builder:{agent_id}] {tid} failed: {e}")
        fail_task(conn, tid, agent_id, str(e))
        _create_report(conn, tid, agent_id, "failed", [], [tb])
        return None


def get_report(conn, task_id: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM execution_reports WHERE task_id = %s ORDER BY created_at DESC LIMIT 1",
            (task_id,)
        )
        row = cur.fetchone()
    return dict(row) if row else None


# ── Claim ─────────────────────────────────────────────────────────────────

def _claim(conn, agent_id: str, director: Optional[str],
           task_id: Optional[str]) -> Optional[dict]:
    if task_id:
        # Direct claim of a specific task
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tasks
                SET status = 'executing', leased_by = %s,
                    leased_until = NOW() + INTERVAL '10 minutes',
                    attempt = attempt + 1, updated_at = NOW()
                WHERE task_id = %s AND status = 'planned' AND plan_id IS NOT NULL
                  AND (leased_until IS NULL OR leased_until < NOW())
                RETURNING *
                """,
                (agent_id, task_id)
            )
            row = cur.fetchone()
        return dict(row) if row else None
    else:
        # Claim next available task with a plan
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH candidate AS (
                    SELECT task_id FROM tasks
                    WHERE status = 'planned' AND plan_id IS NOT NULL
                      AND (leased_until IS NULL OR leased_until < NOW())
                      %s
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE tasks t
                SET status = 'executing', leased_by = %s,
                    leased_until = NOW() + INTERVAL '10 minutes',
                    attempt = attempt + 1, updated_at = NOW()
                FROM candidate
                WHERE t.task_id = candidate.task_id
                RETURNING t.*
                """ % ("AND assigned_director = %s" if director else ""),
                ([director, agent_id] if director else [agent_id])
            )
            row = cur.fetchone()
        return dict(row) if row else None


# ── Load plan + DoD ───────────────────────────────────────────────────────

def _load_plan_and_dod(conn, task_id: str) -> tuple[Optional[dict], dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM plans WHERE task_id = %s", (task_id,))
        plan_row = cur.fetchone()
        cur.execute("SELECT * FROM definitions_of_done WHERE task_id = %s", (task_id,))
        dod_row = cur.fetchone()

    plan = dict(plan_row) if plan_row else None
    dod  = dict(dod_row)  if dod_row  else {}
    return plan, dod


# ── Manifest generation (single LLM call) ─────────────────────────────────

def _generate_manifest(task: dict, plan: dict, dod: dict) -> list[dict]:
    """
    One LLM call: given all plan steps, produce exact execution content for each.
    Returns a list of manifest steps with 'action' and 'content' fields added.
    """
    steps = plan["steps"]
    if isinstance(steps, str):
        steps = json.loads(steps)

    steps_text = "\n".join(
        f"{s['order']}. [{s['tool']}] {s['title']}\n"
        f"   Description: {s['description']}\n"
        f"   Resource: {s.get('resource', 'n/a')}"
        for s in steps
    )

    prompt = f"""Task: {task['title']}
Description: {task['description']}
Complexity: {task['complexity']}

Plan steps to execute:
{steps_text}

Test strategy: {plan.get('test_strategy', 'verify each step output')}

For each step, provide the exact execution content.
Return JSON:
{{
  "manifest": [
    {{
      "order": 1,
      "tool": "file_edit|code_run|shell|web_search|docs_api|none",
      "resource": "path/to/file or search query or 'n/a'",
      "content": "exact file content, code, command, or search query",
      "description": "one line summary of what this does"
    }}
  ]
}}"""

    result = chat_json(
        model=BUILDER_MODEL,
        system=BUILDER_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8192,
    )
    return result.get("manifest", [])


# ── Step execution ────────────────────────────────────────────────────────

def _execute_manifest(conn, task_id: str, agent_id: str,
                      manifest: list[dict], plan: dict) -> tuple[list, list]:
    artifacts = []
    logs      = []

    for step in manifest:
        tool     = step.get("tool", "none")
        resource = step.get("resource", "n/a")
        content  = step.get("content", "")
        desc     = step.get("description", "")

        # Check budget before each tool call
        if tool != "none":
            within_budget = increment_tool_calls(conn, task_id)
            if not within_budget:
                raise RuntimeError(
                    f"Tool budget exceeded at step {step.get('order')}. "
                    "Task blocked — APM will escalate."
                )

        log_entry = f"Step {step.get('order')}: [{tool}] {desc}"
        print(f"    {log_entry}")

        try:
            if tool == "file_edit":
                result = _tool_file_edit(resource, content)
                artifacts.append({"type": "file", "path": resource})

            elif tool == "code_run":
                result = _tool_code_run(content)
                artifacts.append({"type": "code_output", "path": resource,
                                   "output": result[:500]})

            elif tool == "shell":
                result = _tool_shell(content)
                artifacts.append({"type": "shell_output", "command": content,
                                   "output": result[:500]})

            elif tool == "web_search":
                result = _tool_web_search(content or resource)
                artifacts.append({"type": "research", "query": content,
                                   "snippet": result[:300]})

            elif tool == "docs_api":
                result = f"[docs_api] would create/update doc: {resource}"

            elif tool == "none":
                result = f"[note] {content[:100]}"

            else:
                result = f"[unknown tool: {tool}]"

            logs.append(f"✓ {log_entry} → {str(result)[:120]}")

        except Exception as e:
            logs.append(f"✗ {log_entry} → ERROR: {e}")
            raise

    return artifacts, logs


# ── Tool handlers ─────────────────────────────────────────────────────────

def _tool_file_edit(path: str, content: str) -> str:
    if not path or path in ("n/a", "N/A", ""):
        raise ValueError("file_edit requires a valid path.")
    # Restrict writes to safe locations (workspace only, no system paths)
    abs_path = os.path.abspath(path)
    cwd = os.path.abspath(os.getcwd())
    if not abs_path.startswith(cwd):
        raise PermissionError(f"file_edit: path '{path}' is outside workspace.")
    os.makedirs(os.path.dirname(abs_path), exist_ok=True) if os.path.dirname(abs_path) else None
    with open(abs_path, "w") as f:
        f.write(content)
    return f"Written {len(content)} chars to {path}"


def _tool_code_run(code: str, timeout: int = 15) -> str:
    """Run Python code in a subprocess. Captures stdout+stderr.
    If content looks like a shell invocation rather than Python code, runs as shell."""
    code = code.strip()
    # Detect shell-style invocation (e.g. "python3 health_check.py" or "python script.py")
    first_token = code.split()[0] if code else ""
    looks_like_shell = (
        "\n" not in code and
        first_token in ("python", "python3", "sh", "bash", "node", "ruby") and
        not code.startswith("import") and
        "=" not in code and
        "def " not in code
    )
    if looks_like_shell:
        return _tool_shell(code, timeout)

    result = subprocess.run(
        ["python3", "-c", code],
        capture_output=True, text=True, timeout=timeout
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(f"code_run exited {result.returncode}: {output[:300]}")
    return output or "(no output)"


def _tool_shell(command: str, timeout: int = 15) -> str:
    """Run a shell command. Captures stdout+stderr."""
    # Normalize python → python3 on systems without a `python` alias
    command = command.replace("python ", "python3 ").replace("python\n", "python3\n")
    if command.strip() == "python":
        command = "python3"
    result = subprocess.run(
        command, shell=True, capture_output=True, text=True, timeout=timeout
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(f"shell exited {result.returncode}: {output[:300]}")
    return output or "(no output)"


def _tool_web_search(query: str) -> str:
    """Lightweight DuckDuckGo search — returns top result snippets."""
    import urllib.request
    import urllib.parse
    import ssl

    url = f"https://lite.duckduckgo.com/lite/?q={urllib.parse.quote(query)}"
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
        html = resp.read().decode("utf-8", errors="ignore")

    # Extract text snippets (very lightweight — no dependencies)
    import re
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:800]


# ── Execution report ──────────────────────────────────────────────────────

def _create_report(conn, task_id: str, executor: str, status: str,
                   artifacts: list, logs: list) -> dict:
    report_id = f"EXEC-{uuid.uuid4().hex[:8].upper()}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO execution_reports
                (report_id, task_id, executor, status, artifacts, logs)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (report_id, task_id, executor, status,
             json.dumps(artifacts), json.dumps(logs))
        )
        row = cur.fetchone()
    return dict(row)
