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
You execute one plan step at a time. For each step you are given:
- The step details (tool, description, resource, expected output)
- Results from all previous steps

Produce the exact content to execute for the current step only.

Rules:
- Be precise and literal. Your output is executed directly.
- file_edit: provide the full file content to write (no markdown fences).
- code_run: provide executable Python code only (no shell commands, no markdown).
- shell: provide the exact shell command string.
- web_search: provide the exact search query string.
- docs_api: provide {"title": "...", "content": "..."}.
- none: provide a brief note (nothing is executed).
- Use results from previous steps to inform your output — adapt if something failed.
- File paths must be relative to the workspace root.

Respond with valid JSON only:
{"tool": "...", "resource": "path or query or n/a", "content": "exact content to execute", "reason": "one line why"}"""


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

    workspace = _workspace_dir(tid)

    try:
        plan, dod = _load_plan_and_dod(conn, tid)
        if not plan:
            raise ValueError("Task has no plan. Run planner first.")

        artifacts, logs = _execute_steps(conn, tid, agent_id, task, plan, dod, workspace)

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
        director_clause = "AND assigned_director = %s" if director else ""
        params = [director, agent_id] if director else [agent_id]
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidate AS (
                    SELECT task_id FROM tasks
                    WHERE status = 'planned' AND plan_id IS NOT NULL
                      AND (leased_until IS NULL OR leased_until < NOW())
                      {director_clause}
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
                """,
                params
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


# ── Per-step execution (one LLM call per step) ───────────────────────────

def _workspace_dir(task_id: str) -> str:
    """
    Create and return a per-task workspace directory.
    Initialised as a git repo so builder can run git commands inside it.
    """
    path = os.path.join(os.path.abspath(os.getcwd()), "tmp", "workspace", task_id)
    os.makedirs(path, exist_ok=True)
    # Init git repo if not already done
    git_dir = os.path.join(path, ".git")
    if not os.path.exists(git_dir):
        subprocess.run(
            ["git", "init"],
            cwd=path, capture_output=True
        )
        subprocess.run(
            ["git", "commit", "--allow-empty", "-m", "init workspace"],
            cwd=path, capture_output=True,
            env={**os.environ, "GIT_AUTHOR_NAME": "Builder", "GIT_AUTHOR_EMAIL": "builder@osaio",
                 "GIT_COMMITTER_NAME": "Builder", "GIT_COMMITTER_EMAIL": "builder@osaio"}
        )
    return path


def _execute_steps(conn, task_id: str, agent_id: str,
                   task: dict, plan: dict, dod: dict,
                   workspace: str) -> tuple[list, list]:
    """
    Execute each plan step with its own LLM call.
    Previous step results are fed as context so the Builder can adapt.
    All file writes and shell commands are scoped to the workspace directory.
    """
    steps = plan["steps"]
    if isinstance(steps, str):
        steps = json.loads(steps)

    artifacts    = []
    logs         = []
    history      = []   # running LLM conversation for context
    step_results = []   # previous step outcomes for context

    # Seed the conversation with task context
    task_context = (
        f"Task: {task['title']}\n"
        f"Description: {task['description']}\n"
        f"Complexity: {task['complexity']}\n"
        f"Test strategy: {plan.get('test_strategy', '')}\n"
        f"Total steps: {len(steps)}\n"
        f"Workspace directory: {workspace} — all file paths are relative to this directory"
    )
    history.append({"role": "user", "content": task_context})
    history.append({"role": "assistant", "content": '{"acknowledged": true}'})

    for step in steps:
        tool_hint = step.get("tool", "none")
        order     = step.get("order", "?")

        # Build per-step prompt with previous results as context
        prev_context = ""
        if step_results:
            prev_context = "\nPrevious step results:\n" + "\n".join(
                f"  Step {r['order']}: [{r['tool']}] {r['status']} — {r['summary']}"
                for r in step_results
            )

        step_prompt = (
            f"Execute step {order} of {len(steps)}:{prev_context}\n\n"
            f"Step {order}: [{tool_hint}] {step['title']}\n"
            f"Description: {step['description']}\n"
            f"Resource: {step.get('resource', 'n/a')}\n"
            f"Expected output: {step.get('expected_output', '')}\n"
            f"Risk: {step.get('risk', 'low')}"
        )

        history.append({"role": "user", "content": step_prompt})

        action = chat_json(
            model=BUILDER_MODEL,
            system=BUILDER_SYSTEM,
            messages=history,
            max_tokens=4096,
        )

        history.append({"role": "assistant", "content": json.dumps(action)})

        tool     = action.get("tool", tool_hint) or "none"
        resource = action.get("resource", step.get("resource", "n/a")) or "n/a"
        content  = action.get("content") or ""
        log_entry = f"Step {order}: [{tool}] {step['title']}"

        # Budget check
        if tool != "none":
            within_budget = increment_tool_calls(conn, task_id)
            if not within_budget:
                raise RuntimeError(
                    f"Tool budget exceeded at step {order}. Task blocked."
                )

        print(f"    {log_entry}")

        try:
            if tool == "file_edit":
                exec_result = _tool_file_edit(resource, content, workspace)
                artifacts.append({"type": "file", "path": resource, "preview": content[:1200]})

            elif tool == "code_run":
                exec_result = _tool_code_run(content, workspace=workspace)
                artifacts.append({"type": "code_output", "path": resource,
                                   "output": exec_result[:1000]})

            elif tool == "shell":
                exec_result = _tool_shell(content, workspace=workspace)
                artifacts.append({"type": "shell_output", "command": content,
                                   "output": exec_result[:1000]})

            elif tool == "web_search":
                exec_result = _tool_web_search(content or resource)
                artifacts.append({"type": "research", "query": content,
                                   "snippet": exec_result[:500]})

            elif tool == "docs_api":
                exec_result = f"[docs_api] would create/update doc: {resource}"

            elif tool == "none":
                exec_result = f"[note] {content[:200]}"

            else:
                exec_result = f"[unknown tool: {tool}] — skipped"

            step_results.append({
                "order": order, "tool": tool,
                "status": "ok", "summary": str(exec_result)[:150]
            })
            logs.append(f"✓ {log_entry} → {str(exec_result)[:120]}")

        except Exception as e:
            step_results.append({
                "order": order, "tool": tool,
                "status": "error", "summary": str(e)[:150]
            })
            logs.append(f"✗ {log_entry} → ERROR: {e}")
            raise

    return artifacts, logs


# ── Tool handlers ─────────────────────────────────────────────────────────

def _tool_file_edit(path: str, content: str, workspace: str) -> str:
    if not path or path in ("n/a", "N/A", ""):
        raise ValueError("file_edit requires a valid path.")
    # Always resolve relative to workspace
    if not os.path.isabs(path):
        abs_path = os.path.join(workspace, path)
    else:
        abs_path = path
    # Restrict writes to workspace only
    if not os.path.abspath(abs_path).startswith(workspace):
        raise PermissionError(f"file_edit: path '{path}' is outside workspace.")
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(abs_path, "w") as f:
        f.write(content)
    return f"Written {len(content)} chars to {path}"


def _tool_code_run(code: str, timeout: int = 15, workspace: str = None) -> str:
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
        return _tool_shell(code, timeout, workspace=workspace)

    result = subprocess.run(
        ["python3", "-c", code],
        capture_output=True, text=True, timeout=timeout,
        cwd=workspace,
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(f"code_run exited {result.returncode}: {output[:300]}")
    return output or "(no output)"


def _tool_shell(command: str, timeout: int = 30, workspace: str = None) -> str:
    """Run a shell command from within workspace. Captures stdout+stderr."""
    # Normalize python → python3 on systems without a `python` alias
    command = command.replace("python ", "python3 ").replace("python\n", "python3\n")
    if command.strip() == "python":
        command = "python3"
    result = subprocess.run(
        command, shell=True, capture_output=True, text=True, timeout=timeout,
        cwd=workspace,
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
