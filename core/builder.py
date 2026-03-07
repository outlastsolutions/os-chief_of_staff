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
    acquire_resource_lock, release_resource_lock,
    FailureCode,
)
from core import escalation as esc


# ── Subprocess sandbox ─────────────────────────────────────────────────────
# Sensitive env vars are stripped before any code_run or shell step so that
# Builder-executed code cannot exfiltrate credentials even if compromised.
_SUBPROCESS_ENV_ALLOWLIST = {
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TERM",
    "SHELL", "TMPDIR", "TMP", "TEMP", "PWD",
}

def _safe_env() -> dict:
    """Return a clean environment for Builder subprocesses."""
    return {k: v for k, v in os.environ.items()
            if k in _SUBPROCESS_ENV_ALLOWLIST}


BUILDER_SYSTEM = """You are the Builder Agent for Outlast Solutions LLC.
You execute one plan step at a time. For each step you are given:
- The step details (tool, description, resource, expected output)
- Results from all previous steps

Produce the exact content to execute for the current step only.

Rules:
- Be precise and literal. Your output is executed directly.
- file_edit: provide the full file content to write (no markdown fences).
- code_run: provide executable Python code only (no shell commands, no markdown). Do NOT use code_run to run test files — use shell with "python3 -m pytest <path>" instead.
- shell: provide the exact shell command string. Use this for running tests: "python3 -m pytest tests/ -v" or "python3 -m pytest path/to/test_file.py -v".
- web_search: provide the exact search query string.
- github_api: provide a JSON string: {"action":"create_file|update_file|push_workspace|create_pr","repo":"owner/repo","path":"file/path","content":"...","message":"commit msg","branch":"main"}. For push_workspace, omit path/content — all files in the task workspace are pushed automatically.
- docs_api: provide {"title": "...", "content": "..."}.
- none: provide a brief note (nothing is executed).
- Use results from previous steps to inform your output — adapt if something failed.
- File paths must be relative to the workspace root.

Respond with valid JSON only:
{"tool": "...", "resource": "path or query or n/a", "content": "exact content to execute", "reason": "one line why"}"""


# ── Typed builder exception ───────────────────────────────────────────────

class _BuilderError(Exception):
    """Raised inside execute_task with a typed FailureCode for clean routing."""
    def __init__(self, message: str, code: str):
        super().__init__(message)
        self.code = code


# ── Public interface ──────────────────────────────────────────────────────

def execute_task(conn, agent_id: str,
                 director: Optional[str] = None,
                 task_id: Optional[str] = None,
                 request_id: Optional[str] = None) -> Optional[dict]:
    """
    Claim and execute one task.
    If task_id is given, targets that specific task.
    Otherwise claims the next available planned+planned task.
    Returns the execution report dict, or None if nothing to claim.
    """
    task = _claim(conn, agent_id, director, task_id, request_id)
    if not task:
        return None

    tid = task["task_id"]
    print(f"  [builder:{agent_id}] claimed {tid} — {task['title']}")

    workspace = _workspace_dir(tid)

    try:
        plan, dod = _load_plan_and_dod(conn, tid)
        if not plan:
            raise _BuilderError("Task has no plan. Run planner first.", FailureCode.PLAN_MISSING)

        # ── Escalation checks (pre-execution) ─────────────────────────────
        violation = esc.run_builder_checks(task, plan, dod)
        if violation:
            esc.escalate_task(conn, tid, violation, agent_id=agent_id)
            _create_report(conn, tid, agent_id, "failed", [], [f"Escalated: {violation}"])
            return None

        artifacts, logs = _execute_steps(conn, tid, agent_id, task, plan, dod, workspace)

        if not release_to_verifying(conn, tid, agent_id):
            raise _BuilderError(
                "release_to_verifying matched 0 rows — lease lost or task in wrong state.",
                FailureCode.LEASE_LOST,
            )
        report = _create_report(conn, tid, agent_id, "completed", artifacts, logs)
        print(f"  [builder:{agent_id}] {tid} → verifying ({len(artifacts)} artifacts)")
        return report

    except _BuilderError as e:
        tb = traceback.format_exc()
        print(f"  [builder:{agent_id}] {tid} failed [{e.code}]: {e}")
        fail_task(conn, tid, agent_id, str(e), failure_code=e.code)
        _create_report(conn, tid, agent_id, "failed", [], [tb])
        return None

    except Exception as e:
        tb = traceback.format_exc()
        print(f"  [builder:{agent_id}] {tid} failed [INTERNAL_ERROR]: {e}")
        fail_task(conn, tid, agent_id, str(e), failure_code=FailureCode.INTERNAL_ERROR)
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

_BUILDER_LEASE_MINUTES = 10


def _claim(conn, agent_id: str, director: Optional[str],
           task_id: Optional[str],
           request_id: Optional[str] = None) -> Optional[dict]:
    # Dependency gate: only tasks whose every dependency is done are claimable.
    _DEP_GATE = """
        NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(COALESCE(t.dependencies, '[]'::jsonb)) AS dep
            WHERE NOT EXISTS (
                SELECT 1 FROM tasks d WHERE d.task_id = dep AND d.status = 'done'
            )
        )
    """

    if task_id:
        # Direct claim of a specific task
        req_clause = "AND t.request_id = %s" if request_id else ""
        params = [agent_id, _BUILDER_LEASE_MINUTES, task_id]
        if request_id:
            params.append(request_id)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE tasks t
                SET status = 'executing', leased_by = %s,
                    leased_until = NOW() + (%s * INTERVAL '1 minute'),
                    attempt = attempt + 1, updated_at = NOW()
                WHERE t.task_id = %s AND t.status = 'planned' AND t.plan_id IS NOT NULL
                  AND (t.leased_until IS NULL OR t.leased_until < NOW())
                  {req_clause}
                  AND {_DEP_GATE}
                RETURNING t.*
                """,
                params
            )
            row = cur.fetchone()
        return dict(row) if row else None
    else:
        # Claim next available task with a plan and all dependencies satisfied
        director_clause = "AND t.assigned_director = %s" if director else ""
        req_clause      = "AND t.request_id = %s" if request_id else ""
        params = []
        if director:
            params.append(director)
        if request_id:
            params.append(request_id)
        params += [agent_id, _BUILDER_LEASE_MINUTES]
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidate AS (
                    SELECT t.task_id FROM tasks t
                    WHERE t.status = 'planned' AND t.plan_id IS NOT NULL
                      AND (t.leased_until IS NULL OR t.leased_until < NOW())
                      {director_clause}
                      {req_clause}
                      AND {_DEP_GATE}
                    ORDER BY t.created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE tasks t
                SET status = 'executing', leased_by = %s,
                    leased_until = NOW() + (%s * INTERVAL '1 minute'),
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

        # ── Heartbeat: extend lease before every step ──────────────────────
        if not heartbeat(conn, task_id, agent_id):
            raise _BuilderError(
                f"Lease lost before step {order} — another agent took over.",
                FailureCode.LEASE_LOST
            )

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

        # ── Budget check ───────────────────────────────────────────────────
        if tool != "none":
            within_budget = increment_tool_calls(conn, task_id)
            if not within_budget:
                raise _BuilderError(
                    f"Tool budget exceeded at step {order}.",
                    FailureCode.BUDGET_EXCEEDED
                )

        print(f"    {log_entry}")

        # ── Resource lock ──────────────────────────────────────────────────
        lock_key = _compute_lock_key(tool, resource, content, task_id)
        if lock_key and not acquire_resource_lock(conn, lock_key, agent_id):
            raise _BuilderError(
                f"Resource '{lock_key}' locked by another agent at step {order}.",
                FailureCode.LOCK_CONTENTION
            )

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

            elif tool == "github_api":
                exec_result = _tool_github_api(content, resource, workspace,
                                               task_id=task_id)
                artifacts.append({"type": "github", "resource": resource,
                                   "output": exec_result[:500]})

            elif tool == "docs_api":
                exec_result = f"[docs_api] would create/update doc: {resource}"

            elif tool == "none":
                exec_result = f"[note] {content[:200]}"

            else:
                raise _BuilderError(
                    f"Unknown tool '{tool}' at step {order} — plan contains an invalid tool name.",
                    FailureCode.TOOL_FAILURE,
                )

            step_results.append({
                "order": order, "tool": tool,
                "status": "ok", "summary": str(exec_result)[:150]
            })
            logs.append(f"✓ {log_entry} → {str(exec_result)[:120]}")

        except _BuilderError:
            # Already typed — propagate as-is
            step_results.append({
                "order": order, "tool": tool,
                "status": "error", "summary": "lease/lock/budget failure"
            })
            logs.append(f"✗ {log_entry} → INTERNAL FAILURE")
            raise

        except RuntimeError as e:
            # code_run / shell exit non-zero → TEST_FAILURE
            msg = str(e)
            step_results.append({
                "order": order, "tool": tool,
                "status": "error", "summary": msg[:150]
            })
            logs.append(f"✗ {log_entry} → ERROR: {msg[:120]}")
            raise _BuilderError(msg, FailureCode.TEST_FAILURE)

        except Exception as e:
            msg = str(e)
            step_results.append({
                "order": order, "tool": tool,
                "status": "error", "summary": msg[:150]
            })
            logs.append(f"✗ {log_entry} → ERROR: {msg[:120]}")
            raise _BuilderError(msg, FailureCode.TOOL_FAILURE)

        finally:
            if lock_key:
                release_resource_lock(conn, lock_key, agent_id)

    return artifacts, logs


# ── Resource lock key helper ──────────────────────────────────────────────

def _compute_lock_key(tool: str, resource: str, content: str,
                      task_id: str) -> Optional[str]:
    """
    Return a lock key for the resource being modified, or None if no lock needed.
    file_edit: workspace-scoped per task (prevents concurrent writes to same path).
    github_api writes: locks the repo+branch being modified.
    """
    if tool == "file_edit":
        safe_resource = resource.replace("/", "_").replace("..", "")
        return f"workspace:{task_id}:{safe_resource}"

    if tool == "github_api":
        try:
            params = json.loads(content) if content else {}
            action = params.get("action", "")
            if action in ("create_file", "update_file", "push_workspace", "create_pr"):
                repo   = params.get("repo", resource or "unknown")
                branch = params.get("branch", f"task/{task_id}")
                return f"github:{repo}:{branch}"
        except (json.JSONDecodeError, TypeError):
            pass

    return None


# ── Tool handlers ─────────────────────────────────────────────────────────

def _within_workspace(abs_path: str, workspace: str) -> bool:
    """Return True iff abs_path is workspace itself or a descendant of it.
    Uses realpath to resolve symlinks and os.sep to prevent prefix-match bypass
    (e.g. /tmp/work matching /tmp/working)."""
    real_ws   = os.path.realpath(workspace)
    real_path = os.path.realpath(abs_path)
    return real_path == real_ws or real_path.startswith(real_ws + os.sep)


def _tool_file_edit(path: str, content: str, workspace: str) -> str:
    if not path or path in ("n/a", "N/A", ""):
        raise ValueError("file_edit requires a valid path.")
    # Always resolve relative to workspace
    if not os.path.isabs(path):
        abs_path = os.path.join(workspace, path)
    else:
        abs_path = path
    # Restrict writes to workspace only (realpath + sep prevents prefix-match bypass)
    if not _within_workspace(abs_path, workspace):
        raise PermissionError(f"file_edit: path '{path}' is outside workspace.")
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(abs_path, "w") as f:
        f.write(content)
    return f"Written {len(content)} chars to {path}"


def _tool_code_run(code: str, timeout: int = 15, workspace: str = None) -> str:
    """Run Python code in a subprocess. Captures stdout+stderr.
    If content looks like a shell invocation rather than Python code, runs as shell.
    Code is written to a temp file so __file__ is a real path (avoids import issues)."""
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

    # Write to a temp file so __file__ is a real path — avoids sys.path/__file__ issues
    # when LLM-generated test runners use os.path.dirname(__file__) for imports.
    import tempfile
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", dir=workspace or None,
        delete=False, prefix="_cos_run_"
    ) as f:
        f.write(code)
        tmp_path = f.name
    try:
        result = subprocess.run(
            ["python3", tmp_path],
            capture_output=True, text=True, timeout=timeout,
            cwd=workspace,
            env=_safe_env(),
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
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
        env=_safe_env(),
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


def _tool_github_api(content: str, resource: str, workspace: str,
                     task_id: str = "unknown") -> str:
    """
    Interact with GitHub REST API.
    `content` must be a JSON string with:
      action: create_file | update_file | push_workspace | create_pr | get_file | list_files
      repo:   "owner/repo"  (or just "repo" — GITHUB_ORG is prepended)
      path:   file path in the repo
      content: file content (for create/update)
      message: commit message
      branch:  target branch (default: main)
      pr_title / pr_body / base_branch: for create_pr
      local_dir: for push_workspace — push all files from a local dir to repo
    """
    import base64
    import requests as _req

    from config.settings import GITHUB_TOKEN, GITHUB_ORG

    try:
        params = json.loads(content) if content else {}
    except json.JSONDecodeError:
        params = {"action": "create_file", "content": content, "path": resource}

    action  = params.get("action", "create_file")

    # Boundary check for push_workspace fires before any API call or token check,
    # so the security gate is enforced even when GITHUB_TOKEN is not configured.
    if action == "push_workspace":
        early_local_dir = params.get("local_dir") or workspace
        if not _within_workspace(early_local_dir, workspace):
            raise _BuilderError(
                f"push_workspace: local_dir '{early_local_dir}' resolves outside task workspace — aborted.",
                FailureCode.TOOL_FAILURE,
            )

    if not GITHUB_TOKEN:
        return "[github_api] GITHUB_TOKEN not configured — skipped"
    repo    = params.get("repo", resource or "")
    if repo and "/" not in repo:
        repo = f"{GITHUB_ORG}/{repo}"

    branch  = params.get("branch") or f"task/{task_id}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    base_url = f"https://api.github.com/repos/{repo}"

    if action == "list_files":
        path = params.get("path", "")
        r = _req.get(f"{base_url}/contents/{path}",
                     headers=headers, params={"ref": branch}, timeout=15)
        r.raise_for_status()
        items = [f["path"] for f in r.json() if isinstance(f, dict)]
        return f"Files in {repo}/{path}: {', '.join(items[:20])}"

    elif action == "get_file":
        path = params.get("path", "")
        r = _req.get(f"{base_url}/contents/{path}",
                     headers=headers, params={"ref": branch}, timeout=15)
        r.raise_for_status()
        data = r.json()
        decoded = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        return f"Content of {path}:\n{decoded[:1000]}"

    elif action in ("create_file", "update_file"):
        path    = params.get("path", resource or "")
        message = params.get("message", f"chore: update {path} via CoS builder")
        fc      = params.get("content", "")

        # Check if file exists to get SHA (needed for update)
        sha = None
        r_check = _req.get(f"{base_url}/contents/{path}",
                           headers=headers, params={"ref": branch}, timeout=10)
        if r_check.status_code == 200:
            sha = r_check.json().get("sha")

        body: dict = {
            "message": message,
            "content": base64.b64encode(fc.encode()).decode(),
            "branch":  branch,
        }
        if sha:
            body["sha"] = sha

        r = _req.put(f"{base_url}/contents/{path}",
                     headers=headers, json=body, timeout=15)
        r.raise_for_status()
        verb = "Updated" if sha else "Created"
        return f"{verb} {repo}/{path} on {branch}"

    elif action == "push_workspace":
        # local_dir already validated above (before GITHUB_TOKEN check).
        local_dir = params.get("local_dir") or workspace
        message   = params.get("message", "feat: builder output via CoS")
        pushed    = []
        errors    = []
        for root, dirs, files in os.walk(local_dir):
            dirs[:] = [d for d in dirs if d != ".git"]
            for fname in files:
                abs_path = os.path.join(root, fname)
                rel_path = os.path.relpath(abs_path, local_dir)
                try:
                    with open(abs_path, "rb") as f:
                        raw = f.read()
                    sha = None
                    r_check = _req.get(f"{base_url}/contents/{rel_path}",
                                       headers=headers, params={"ref": branch}, timeout=10)
                    if r_check.status_code == 200:
                        sha = r_check.json().get("sha")
                    body = {
                        "message": f"{message} — {rel_path}",
                        "content": base64.b64encode(raw).decode(),
                        "branch":  branch,
                    }
                    if sha:
                        body["sha"] = sha
                    r = _req.put(f"{base_url}/contents/{rel_path}",
                                 headers=headers, json=body, timeout=15)
                    r.raise_for_status()
                    pushed.append(rel_path)
                except Exception as e:
                    errors.append(f"{rel_path}: {e}")
        result = f"Pushed {len(pushed)} file(s) to {repo}/{branch}"
        if errors:
            result += f" | {len(errors)} error(s): {'; '.join(errors[:3])}"
        return result

    elif action == "create_pr":
        head        = params.get("head_branch", branch)
        base_branch = params.get("base_branch", "main")
        pr_title    = params.get("pr_title", "CoS builder output")
        pr_body     = params.get("pr_body", "Automated PR created by CoS Builder agent.")
        r = _req.post(f"{base_url}/pulls",
                      headers=headers,
                      json={"title": pr_title, "body": pr_body,
                            "head": head, "base": base_branch},
                      timeout=15)
        r.raise_for_status()
        pr = r.json()
        return f"PR #{pr['number']} created: {pr['html_url']}"

    else:
        return f"[github_api] unknown action: {action}"


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
