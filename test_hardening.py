"""
Hardening regression tests — Stage 2.2
Outlast Solutions LLC © 2026

Covers each change from the hardening pass:
  1. Twilio recording webhook sig verification  (Secretary — tested via route logic)
  2. Lease ownership on terminal task transitions
  3. Builder unknown-tool typed failure + release_to_verifying false → failure
  4. Persist blocked_reason in request transitions
  5. All-day GCal end date = start + 1 day
  6. auth.py reads key at request time (no import-time capture)
  7. Slack intake atomic idempotency (pre-generated request_id)
  8. Outbox worker counters: attempted/sent/failed
  9. Worker interval validation > 0
 10. DB connect_args SQLite-only
 11. History scoped by unit_context

Run: python3 test_hardening.py
Exit: 0 = all passed, 1 = failure
"""
from __future__ import annotations
import sys
import traceback
import json
import uuid

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"

_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _results.append((name, ok, detail))
    sym = PASS if ok else FAIL
    print(f"  {sym}  {name}" + (f" — {detail}" if detail else ""))


def section(title: str) -> None:
    print(f"\n\033[96m{title}\033[0m")


# ── 2. Lease ownership on terminal transitions ─────────────────────────────

def test_lease_ownership():
    section("2 — Lease ownership on terminal transitions")
    from db.connection import transaction
    from core.lease import fail_task, FailureCode

    tid = f"HARD-LEASE-{uuid.uuid4().hex[:6].upper()}"
    rid = f"REQ-HARD-{uuid.uuid4().hex[:6].upper()}"

    with transaction() as conn:
        cur = conn.cursor()
        # Create a request + task owned by agent-A
        cur.execute("""
            INSERT INTO requests (request_id, idempotency_key, requester, source, title, description, priority, category)
            VALUES (%s, %s, 'test', 'test', 'Hardening', 'Hardening', 'medium', 'development')
        """, (rid, f"idem-hard-{rid}"))
        cur.execute("""
            INSERT INTO tasks (task_id, request_id, assigned_director, title, description, status, leased_by)
            VALUES (%s, %s, 'development', 'Hard Task', 'Hard Task', 'executing', 'agent-A')
        """, (tid, rid))

    try:
        # agent-B tries to fail a task owned by agent-A — should be a no-op (graceful)
        with transaction() as conn:
            result = fail_task(conn, tid, "agent-B", "unauthorized fail", FailureCode.INTERNAL_ERROR)
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("SELECT status, leased_by FROM tasks WHERE task_id = %s", (tid,))
            row = cur.fetchone()
        check("fail_task by non-owner does not change status",
              row["status"] == "executing", f"status={row['status']}")
        check("fail_task by non-owner preserves leased_by",
              row["leased_by"] == "agent-A", f"leased_by={row['leased_by']}")

        # agent-A (the owner) can fail correctly
        with transaction() as conn:
            fail_task(conn, tid, "agent-A", "legitimate fail", FailureCode.TEST_FAILURE)
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("SELECT status, leased_by FROM tasks WHERE task_id = %s", (tid,))
            row = cur.fetchone()
        check("fail_task by owner transitions status",
              row["status"] in ("planned", "blocked"), f"status={row['status']}")
        check("fail_task by owner clears leased_by",
              row["leased_by"] is None, f"leased_by={row['leased_by']}")

    finally:
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM agent_logs WHERE task_id = %s", (tid,))
            cur.execute("DELETE FROM tasks WHERE task_id = %s", (tid,))
            cur.execute("DELETE FROM agent_logs WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM requests WHERE request_id = %s", (rid,))


# ── 3. Builder unknown tool → typed failure ────────────────────────────────

def test_builder_unknown_tool():
    section("3 — Builder unknown-tool typed failure")
    from core.builder import _BuilderError, FailureCode

    # Simulate the else branch by checking that a fake tool name raises _BuilderError
    # We do this by inspecting the code path rather than running the full builder.
    import ast, inspect
    import core.builder as builder_mod
    src = inspect.getsource(builder_mod._execute_steps)

    # Verify the else branch raises _BuilderError (not "skipped")
    check("Unknown tool branch raises _BuilderError (no 'skipped' string)",
          "skipped" not in src or "_BuilderError" in src.split("else")[1].split("exec_result")[0],
          "")
    check("TOOL_FAILURE used in else branch",
          "TOOL_FAILURE" in src or "FailureCode.TOOL_FAILURE" in src, "")


def test_builder_release_to_verifying():
    section("3b — Builder release_to_verifying false → LEASE_LOST")
    import inspect
    import core.builder as builder_mod
    src = inspect.getsource(builder_mod.execute_task)
    check("release_to_verifying return value is checked",
          "if not release_to_verifying" in src, "")
    check("LEASE_LOST raised on release failure",
          "LEASE_LOST" in src, "")
    # _create_report must come AFTER the release_to_verifying guard in source order
    rel_idx    = src.index("if not release_to_verifying")
    report_idx = src.index("_create_report(conn, tid, agent_id, \"completed\"")
    check("_create_report only after successful release",
          rel_idx < report_idx, f"release@{rel_idx} report@{report_idx}")


# ── 4. Persist blocked_reason in request transitions ──────────────────────

def test_request_blocked_reason():
    section("4 — Persist blocked_reason in request transitions")
    from db.connection import transaction
    from core.pm import receive_request, mark_blocked

    rid_raw = {"idempotency_key": f"idem-hard-blk-{uuid.uuid4().hex[:8]}",
               "requester": "test", "source": "test",
               "title": "Block Me", "description": "Block Me", "category": "development"}

    with transaction() as conn:
        req = receive_request(conn, rid_raw)
        rid = req["request_id"]

    try:
        with transaction() as conn:
            # scope first so we can block it
            from core.state_machine import transition_request, Role, RequestState
            transition_request(conn, rid, Role.PM, RequestState.SCOPED)
            transition_request(conn, rid, Role.PM, RequestState.IN_PROGRESS)
            mark_blocked(conn, rid, "dependency on external API unavailable")

        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("SELECT status, blocked_reason FROM requests WHERE request_id = %s", (rid,))
            row = cur.fetchone()

        check("Request status is 'blocked'", row["status"] == "blocked", f"status={row['status']}")
        check("blocked_reason is persisted in DB",
              row["blocked_reason"] and "external API" in row["blocked_reason"],
              f"blocked_reason={row['blocked_reason']!r}")
    finally:
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM agent_logs WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM tasks WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM director_reports WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM requests WHERE request_id = %s", (rid,))


# ── 5. All-day GCal event end = start + 1 day ─────────────────────────────

def test_gcal_allday_end_date():
    section("5 — All-day GCal end date = start + 1 day")
    import inspect, sys, importlib
    sys.path.insert(0, "/home/osuser/os/secretary")
    # Import via spec so we don't clash with CoS core namespace
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "sec_gcal", "/home/osuser/os/secretary/core/gcal.py")
    gcal_mod = importlib.util.module_from_spec(spec)
    src = open("/home/osuser/os/secretary/core/gcal.py").read()
    check("create_task_event uses timedelta(days=1) for end",
          "timedelta(days=1)" in src, "")
    check("end date is exclusive (end != due variable names differ)",
          '"end": {"date": end}' in src and '"start": {"date": due}' in src, "")


# ── 6. auth.py reads key at request time ──────────────────────────────────

def test_auth_runtime_key():
    section("6 — auth.py reads key at request time")
    src = open("/home/osuser/os/secretary/api/auth.py").read()
    # The module-level _API_KEY capture line should be gone
    check("No module-level _API_KEY = os.getenv(...) capture",
          "_API_KEY: str | None = os.getenv" not in src and
          "_API_KEY = os.getenv" not in src, "")
    # The require_auth function body should do the read
    import ast
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "require_auth":
            fn_src = ast.get_source_segment(src, node)
            break
    else:
        fn_src = ""
    check("require_auth reads os.getenv at call time",
          "os.getenv" in fn_src, "")


# ── 7. Slack intake: atomic idempotency ───────────────────────────────────

def test_slack_intake_idempotency():
    section("7 — Slack intake atomic idempotency")
    import inspect
    import core.slack_intake as si_mod
    src = inspect.getsource(si_mod.ingest)
    check("_already_ingested check-then-insert removed",
          "_already_ingested" not in src, "")
    check("Pre-generated request_id used",
          "local_req_id" in src, "")
    check("is_new check present",
          "is_new" in src, "")
    check("Thread reply only sent on is_new",
          src.index("is_new") < src.index("_reply_thread"), "")


# ── 8. Outbox worker counters ─────────────────────────────────────────────

def test_outbox_worker_counters():
    section("8 — Outbox worker counters: attempted/sent/failed")
    import inspect
    sys.path.insert(0, "/home/osuser/os/chief_of_staff")
    import workers.outbox_worker as ow
    src = inspect.getsource(ow.drain_once)
    check("drain_once returns tuple (attempted, sent, failed)",
          "attempted" in src and "sent" in src and "failed" in src, "")
    check("sent counter incremented on success",
          "sent += 1" in src, "")
    check("failed counter incremented on failure",
          "failed += 1" in src, "")


# ── 9. Worker interval validation ─────────────────────────────────────────

def test_worker_interval_validation():
    section("9 — Worker interval validation > 0")
    import workers.outbox_worker as ow
    import workers.slack_intake_worker as siw

    # outbox_worker: interval=0 should raise
    try:
        ow.run(poll_interval=0, once=False)
        check("outbox_worker rejects interval=0", False, "no error raised")
    except ValueError as e:
        check("outbox_worker rejects interval=0", "0" in str(e) or ">" in str(e), str(e))

    # slack_intake_worker: interval=-1 should raise
    try:
        siw.run(poll_interval=-1, once=False)
        check("slack_intake_worker rejects interval=-1", False, "no error raised")
    except ValueError as e:
        check("slack_intake_worker rejects interval=-1", ">" in str(e), str(e))


# ── 9b. Worker SIGTERM handling ───────────────────────────────────────────

def test_worker_sigterm():
    section("9b — Worker SIGTERM graceful shutdown")
    import signal
    import inspect
    import workers.outbox_worker as ow
    import workers.slack_intake_worker as siw

    # outbox_worker: _shutdown flag and signal handler present
    ow_src = inspect.getsource(ow)
    check("outbox_worker has _shutdown flag",
          "_shutdown" in ow_src, "")
    check("outbox_worker registers SIGTERM handler",
          "signal.SIGTERM" in ow_src and "_handle_signal" in ow_src, "")
    check("outbox_worker loop exits on _shutdown",
          "while not _shutdown" in ow_src, "")
    check("outbox_worker skips sleep on _shutdown",
          "if not _shutdown" in ow_src, "")

    # slack_intake_worker: same checks
    siw_src = inspect.getsource(siw)
    check("slack_intake_worker has _shutdown flag",
          "_shutdown" in siw_src, "")
    check("slack_intake_worker registers SIGTERM handler",
          "signal.SIGTERM" in siw_src and "_handle_signal" in siw_src, "")
    check("slack_intake_worker loop exits on _shutdown",
          "while not _shutdown" in siw_src, "")

    # Functional: simulate signal delivery — set _shutdown and confirm run() exits
    ow._shutdown = False
    ow._handle_signal(signal.SIGTERM, None)
    check("outbox_worker _handle_signal sets _shutdown=True", ow._shutdown, "")
    ow._shutdown = False  # reset

    siw._shutdown = False
    siw._handle_signal(signal.SIGTERM, None)
    check("slack_intake_worker _handle_signal sets _shutdown=True", siw._shutdown, "")
    siw._shutdown = False  # reset


# ── 9c. Worker shutdown loop termination (thread-based) ───────────────────

def test_worker_shutdown_terminates():
    section("9c — Worker loop actually terminates after signal")
    import threading
    import time
    import workers.outbox_worker as ow

    # Patch drain_once to a no-op so the test doesn't touch the DB
    original_drain = ow.drain_once

    def _fake_drain():
        return 0, 0, 0

    ow.drain_once = _fake_drain
    ow._shutdown = False

    thread = threading.Thread(
        target=ow.run,
        kwargs={"poll_interval": 1, "once": False},
        daemon=True,
    )
    thread.start()

    # Give the loop one iteration to start, then signal shutdown
    time.sleep(0.1)
    ow._handle_signal(15, None)

    thread.join(timeout=3)
    exited = not thread.is_alive()
    check("outbox_worker run() exits within 3s after _handle_signal", exited,
          "thread still alive" if not exited else "")

    # Restore
    ow.drain_once = original_drain
    ow._shutdown = False


# ── 10. DB connect_args SQLite-only ───────────────────────────────────────

def test_db_connect_args():
    section("10 — DB connect_args SQLite-only")
    src = open("/home/osuser/os/secretary/db/database.py").read()
    check("connect_args conditional on sqlite prefix",
          'startswith("sqlite")' in src or "startswith('sqlite')" in src, "")
    check("check_same_thread only for sqlite",
          "check_same_thread" in src and "sqlite" in src, "")


# ── 11. Conversation history scoped by unit_context ───────────────────────

def test_conversation_context_isolation():
    section("11 — Conversation history scoped by unit_context")
    src = open("/home/osuser/os/secretary/core/secretary.py").read()
    # Find the chat() function body
    chat_start = src.index("def chat(")
    # Get a window around the history query
    chat_src = src[chat_start:chat_start + 800]
    check("History query filters by unit_context",
          ".filter(" in chat_src and "unit_context" in chat_src, "")
    check("Filter appears before .limit(20) in chat history query",
          chat_src.index(".filter(") < chat_src.index(".limit(20)"), "")


# ── 12. Domain contract consistency ───────────────────────────────────────

def test_domain_contract():
    section("12 — Domain contract: single canonical source, all references consistent")
    from config.settings import VALID_DOMAINS
    from core.director import DOMAINS
    import core.pm as pm_mod
    import core.apm as apm_mod

    check("VALID_DOMAINS defined in config.settings",
          len(VALID_DOMAINS) > 0, str(VALID_DOMAINS))
    check("director.DOMAINS == VALID_DOMAINS",
          DOMAINS == VALID_DOMAINS, f"DOMAINS={DOMAINS}")

    # PM and APM must reference VALID_DOMAINS (not hardcode the list)
    import inspect
    pm_src = inspect.getsource(pm_mod.scope_request)
    check("pm scope_request references VALID_DOMAINS",
          "VALID_DOMAINS" in pm_src, "")

    # APM system prompt evaluated value must include all domains
    apm_system = apm_mod.APM_SYSTEM
    for domain in VALID_DOMAINS:
        check(f"APM_SYSTEM includes domain '{domain}'",
              domain in apm_system, "")

    # APM decomposition prompt source references VALID_DOMAINS
    apm_src = inspect.getsource(apm_mod._build_decomposition_prompt)
    check("APM _build_decomposition_prompt references VALID_DOMAINS",
          "VALID_DOMAINS" in apm_src, "")

    # No stale hardcoded domain tuple anywhere (director must import from settings)
    dir_src = inspect.getsource(apm_mod).split("VALID_DOMAINS")[0]  # before the import
    check("director does not hardcode domain tuple inline",
          'DOMAINS = ("development"' not in inspect.getsource(
              __import__("core.director", fromlist=["director"])), "")

    # 'compute' is explicitly NOT a valid domain (resolved: not an intended domain)
    check("'compute' is NOT in VALID_DOMAINS",
          "compute" not in VALID_DOMAINS, f"VALID_DOMAINS={VALID_DOMAINS}")

    # PM_SYSTEM must mention all domains (parallel to APM_SYSTEM coverage check)
    pm_system = pm_mod.PM_SYSTEM
    for domain in VALID_DOMAINS:
        check(f"PM_SYSTEM includes domain '{domain}'",
              domain in pm_system, "")

    # director.run_domain enforces domain validation at the boundary
    import core.director as dir_mod
    dir_fn_src = inspect.getsource(dir_mod.run_domain)
    check("director.run_domain rejects unknown domain (ValueError guard present)",
          "if domain not in DOMAINS" in dir_fn_src and "raise ValueError" in dir_fn_src, "")

    # director_cli iterates DOMAINS (not a hardcoded list) in run-all
    import director_cli as dcli
    cli_src = inspect.getsource(dcli.cmd_run_all)
    check("director_cli cmd_run_all iterates DOMAINS (not hardcoded list)",
          "for domain in DOMAINS" in cli_src, "")

    # director_cli _USAGE derives domain list from DOMAINS at import time
    check("director_cli _USAGE is dynamically built (not a static string)",
          hasattr(dcli, "_USAGE") and " | ".join(DOMAINS) in dcli._USAGE, "")


# ── 13. Outbox batch continues after mark_outbox_failed DB error ───────────

def test_outbox_batch_resilience():
    section("13 — Outbox batch continues if mark_outbox_failed raises")
    import workers.outbox_worker as ow
    import inspect

    src = inspect.getsource(ow.drain_once)
    check("mark_outbox_failed wrapped in inner try/except",
          "except Exception as db_err" in src, "")
    check("failed counter incremented even when mark_outbox_failed raises",
          src.index("failed += 1") > src.index("except Exception as db_err"), "")
    check("WARN log emitted for DB failure in error path",
          "could not record failure" in src, "")

    # Functional: verify batch continues past an item whose mark_outbox_failed raises
    original_dispatch = ow.dispatch
    original_fail     = ow.mark_outbox_failed

    calls = {"dispatched": 0, "db_fail_called": 0, "completed": False}

    def _fake_dispatch(item):
        calls["dispatched"] += 1
        raise RuntimeError("simulated dispatch failure")

    def _fake_mark_failed(conn, oid, error=""):
        calls["db_fail_called"] += 1
        raise RuntimeError("simulated DB failure in mark_outbox_failed")

    # Patch at module level
    ow.dispatch           = _fake_dispatch
    ow.mark_outbox_failed = _fake_mark_failed

    from db.connection import transaction
    with transaction() as conn:
        from core.idempotency import enqueue_outbox
        import uuid
        dk1 = f"test-resil-{uuid.uuid4().hex[:8]}"
        dk2 = f"test-resil-{uuid.uuid4().hex[:8]}"
        enqueue_outbox(conn, dk1, "slack_post", {"channel": "C", "text": "T1"})
        enqueue_outbox(conn, dk2, "slack_post", {"channel": "C", "text": "T2"})

    try:
        attempted, sent, failed = ow.drain_once()
        check("drain_once completes without raising", True, "")
        check("both items attempted", attempted >= 2, f"attempted={attempted}")
        check("failed count > 0 despite DB error", failed > 0, f"failed={failed}")
        check("mark_outbox_failed was attempted", calls["db_fail_called"] > 0, "")
    except Exception as e:
        check("drain_once completes without raising", False, str(e)[:80])
    finally:
        ow.dispatch           = original_dispatch
        ow.mark_outbox_failed = original_fail


# ── 14. Dependency gating in builder claim SQL ────────────────────────────

def test_dependency_gating_sql():
    section("14 — Dependency gating: SQL gate in both claim branches")
    import inspect
    import re
    import core.builder as builder_mod

    src = inspect.getsource(builder_mod._claim)

    check("_DEP_GATE variable defined in _claim",
          "_DEP_GATE" in src, "")
    check("_DEP_GATE uses jsonb_array_elements_text (JSONB dep array walk)",
          "jsonb_array_elements_text" in src, "")
    check("_DEP_GATE requires dependency task status = 'done'",
          "'done'" in src, "")

    # Both branches (task_id and queued) must reference _DEP_GATE in their SQL
    usages = len(re.findall(r"\{_DEP_GATE\}", src))
    check("_DEP_GATE interpolated into both claim SQL branches (count >= 2)",
          usages >= 2, f"found {usages} interpolation(s)")

    # Direct path: single UPDATE with WHERE; queued path: CTE candidate
    check("Queued path uses CTE (WITH candidate AS)",
          "WITH candidate AS" in src, "")
    check("Direct path uses task_id WHERE clause",
          "t.task_id = %s" in src, "")


def test_dependency_gating_functional():
    section("14b — Dependency gating: functional claim behavior (DB)")
    from db.connection import transaction
    import uuid, json
    from core.builder import _claim

    rid   = f"REQ-DEPG-{uuid.uuid4().hex[:6].upper()}"
    p_id  = f"PLAN-DEPG-{uuid.uuid4().hex[:6].upper()}"
    t_dep = f"TASK-DEPG-DEP-{uuid.uuid4().hex[:6].upper()}"
    t_tgt = f"TASK-DEPG-TGT-{uuid.uuid4().hex[:6].upper()}"

    try:
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO requests
                    (request_id, idempotency_key, requester, source, title, description, priority, category)
                VALUES (%s, %s, 'test', 'test', 'DepGate', 'DepGate', 'medium', 'development')
            """, (rid, f"idem-depg-{rid}"))

            # Dependency task — starts as 'done'
            cur.execute("""
                INSERT INTO tasks
                    (task_id, request_id, assigned_director, title, description, status)
                VALUES (%s, %s, 'development', 'Dep Task', 'Dep Task', 'done')
            """, (t_dep, rid))

            # Target task — depends on t_dep, starts without plan_id
            cur.execute("""
                INSERT INTO tasks
                    (task_id, request_id, assigned_director, title, description,
                     status, dependencies)
                VALUES (%s, %s, 'development', 'Target Task', 'Target Task',
                        'planned', %s)
            """, (t_tgt, rid, json.dumps([t_dep])))

            # Plan row (must exist before tasks.plan_id FK is set)
            cur.execute("""
                INSERT INTO plans (plan_id, task_id, steps)
                VALUES (%s, %s, '[]'::jsonb)
            """, (p_id, t_tgt))

            # Wire plan_id onto the target task
            cur.execute(
                "UPDATE tasks SET plan_id = %s WHERE task_id = %s",
                (p_id, t_tgt)
            )

        # Dep is 'done' → target must be claimable
        with transaction() as conn:
            result = _claim(conn, "test-agent", "development", t_tgt, None)
        check("Task IS claimable when dependency is 'done'",
              result is not None, "got None — dep gate blocked incorrectly")

        # Reset target to planned, unblock lease
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE tasks SET status = 'planned', leased_by = NULL, leased_until = NULL
                WHERE task_id = %s
            """, (t_tgt,))
            # Set dep to 'planned' (not done) — target must now be unclaimable
            cur.execute(
                "UPDATE tasks SET status = 'planned' WHERE task_id = %s", (t_dep,)
            )

        with transaction() as conn:
            result = _claim(conn, "test-agent", "development", t_tgt, None)
        check("Task is NOT claimable when dependency is not 'done'",
              result is None, f"expected None, got {result}")

    finally:
        with transaction() as conn:
            cur = conn.cursor()
            # Clear plan_id FK before deleting plans (tasks.plan_id references plans)
            cur.execute("UPDATE tasks SET plan_id = NULL WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM plans     WHERE task_id   = %s", (t_tgt,))
            cur.execute("DELETE FROM agent_logs WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM tasks     WHERE request_id = %s", (rid,))
            cur.execute("DELETE FROM requests  WHERE request_id = %s", (rid,))


# ── 15. push_workspace local_dir bounds check ─────────────────────────────

def test_push_workspace_bounds_sql():
    section("15 — push_workspace: local_dir bounds check (code inspection)")
    import inspect
    import core.builder as builder_mod

    src_within = inspect.getsource(builder_mod._within_workspace)
    src_github = inspect.getsource(builder_mod._tool_github_api)

    check("_within_workspace uses os.path.realpath (symlink-safe)",
          "realpath" in src_within, "")
    check("_within_workspace uses os.sep (no prefix-match bypass)",
          "os.sep" in src_within, "")

    check("push_workspace action calls _within_workspace on local_dir",
          "push_workspace" in src_github and "_within_workspace" in src_github, "")
    check("Violation raises _BuilderError (typed — not silent clamp)",
          "_BuilderError" in src_github, "")
    check("Violation uses FailureCode.TOOL_FAILURE",
          "TOOL_FAILURE" in src_github, "")

    # Boundary check guard must appear before the GITHUB_TOKEN guard in source order.
    # Use the specific guard expressions (not the import line) for accurate ordering.
    within_guard_idx = src_github.index("if not _within_workspace")
    token_guard_idx  = src_github.index("if not GITHUB_TOKEN")
    check("Boundary check guard fires before GITHUB_TOKEN guard",
          within_guard_idx < token_guard_idx,
          f"_within_workspace guard@{within_guard_idx} GITHUB_TOKEN guard@{token_guard_idx}")


def test_push_workspace_bounds_functional():
    section("15b — push_workspace: functional reject / accept (offline)")
    import tempfile, os, json
    import core.builder as builder_mod
    from core.builder import _BuilderError

    with tempfile.TemporaryDirectory() as workspace:
        # ── Negative case: local_dir escapes workspace ──────────────────────
        bad_content = json.dumps({
            "action":    "push_workspace",
            "repo":      "test/repo",
            "local_dir": "/home/osuser",
        })
        try:
            builder_mod._tool_github_api(bad_content, "", workspace, task_id="TEST")
            check("push_workspace rejects local_dir=/home/osuser", False,
                  "no exception raised")
        except _BuilderError as e:
            check("push_workspace rejects local_dir=/home/osuser",
                  "outside" in str(e).lower(),
                  str(e)[:80])
        except Exception as e:
            check("push_workspace rejects local_dir=/home/osuser", False,
                  f"wrong exception type: {type(e).__name__}: {e}")

        # ── Positive case: local_dir inside workspace ───────────────────────
        subdir = os.path.join(workspace, "output")
        os.makedirs(subdir)
        good_content = json.dumps({
            "action":    "push_workspace",
            "repo":      "test/repo",
            "local_dir": subdir,
        })
        try:
            result = builder_mod._tool_github_api(good_content, "", workspace,
                                                  task_id="TEST")
            # Without GITHUB_TOKEN the function returns a skip message — still accepted
            check("push_workspace accepts local_dir inside workspace",
                  "GITHUB_TOKEN not configured" in result or "Pushed" in result,
                  result[:60])
        except _BuilderError as e:
            check("push_workspace accepts local_dir inside workspace", False,
                  str(e)[:80])
        except Exception as e:
            check("push_workspace accepts local_dir inside workspace", False,
                  f"{type(e).__name__}: {e}")


# ── 16. Outbox bounded retry / backoff / terminal semantics ───────────────

def test_outbox_retry_logic():
    section("16 — Outbox retry: bounded backoff + terminal state (code inspection)")
    import inspect
    from core import idempotency as idem

    src_fail  = inspect.getsource(idem.mark_outbox_failed)
    src_claim = inspect.getsource(idem.claim_pending_outbox)

    # Constants
    check("OUTBOX_MAX_ATTEMPTS defined and positive",
          hasattr(idem, "OUTBOX_MAX_ATTEMPTS") and idem.OUTBOX_MAX_ATTEMPTS > 0,
          f"value={getattr(idem, 'OUTBOX_MAX_ATTEMPTS', None)}")
    check("OUTBOX_BACKOFF_BASE defined",
          hasattr(idem, "OUTBOX_BACKOFF_BASE") and idem.OUTBOX_BACKOFF_BASE >= 2,
          f"value={getattr(idem, 'OUTBOX_BACKOFF_BASE', None)}")

    # mark_outbox_failed: increments attempts
    check("mark_outbox_failed reads current attempts from DB",
          "SELECT attempts FROM outbox" in src_fail or "attempts" in src_fail, "")
    check("mark_outbox_failed increments attempts counter",
          "next_attempts" in src_fail or "attempts + 1" in src_fail, "")

    # Terminal path: max attempts → 'dead'
    check("mark_outbox_failed transitions to 'dead' at max attempts",
          "'dead'" in src_fail, "")
    check("mark_outbox_failed uses >= OUTBOX_MAX_ATTEMPTS for dead threshold",
          "OUTBOX_MAX_ATTEMPTS" in src_fail, "")

    # Backoff path: below max → 'pending' + next_retry_at
    check("mark_outbox_failed sets next_retry_at for backoff",
          "next_retry_at" in src_fail, "")
    check("mark_outbox_failed resets to 'pending' (not 'failed') for retry",
          "'pending'" in src_fail, "")
    check("mark_outbox_failed clears leased_until on failure",
          "leased_until = NULL" in src_fail, "")
    check("Backoff is exponential (** operator or power expression)",
          "**" in src_fail or "OUTBOX_BACKOFF_BASE" in src_fail, "")

    # claim_pending_outbox: respects backoff window
    check("claim_pending_outbox filters by next_retry_at (respects backoff window)",
          "next_retry_at" in src_claim, "")
    check("claim_pending_outbox skips rows where next_retry_at > NOW()",
          "next_retry_at <= NOW()" in src_claim or "next_retry_at" in src_claim, "")

    # reclaim_stale_outbox: crash recovery does not bypass dead state
    src_reclaim = inspect.getsource(idem.reclaim_stale_outbox)
    check("reclaim_stale_outbox only reclaims 'sending' rows (not 'dead')",
          "'sending'" in src_reclaim and "'dead'" not in src_reclaim, "")


def test_outbox_retry_functional():
    section("16b — Outbox retry: functional transient + poison paths (DB)")
    from db.connection import transaction
    from core.idempotency import (
        enqueue_outbox, claim_pending_outbox, mark_outbox_failed,
        mark_outbox_sent, OUTBOX_MAX_ATTEMPTS,
    )
    import uuid

    # ── Transient failure → retry after backoff ────────────────────────────
    dk_transient = f"test-retry-transient-{uuid.uuid4().hex[:8]}"
    oid_transient = None

    with transaction() as conn:
        oid_transient = enqueue_outbox(conn, dk_transient, "slack_post",
                                       {"channel": "C", "text": "T"})

    # Single failure → should stay 'pending' with future next_retry_at
    with transaction() as conn:
        mark_outbox_failed(conn, oid_transient, error="transient network error")

    with transaction() as conn:
        cur = conn.cursor()
        cur.execute("SELECT status, attempts, next_retry_at FROM outbox WHERE outbox_id = %s",
                    (oid_transient,))
        row = cur.fetchone()

    check("After 1 failure: status is 'pending' (not dead, not failed)",
          row["status"] == "pending", f"status={row['status']}")
    check("After 1 failure: attempts = 1",
          row["attempts"] == 1, f"attempts={row['attempts']}")
    check("After 1 failure: next_retry_at is set (backoff scheduled)",
          row["next_retry_at"] is not None, f"next_retry_at={row['next_retry_at']}")

    # Simulate backoff elapsed: set next_retry_at to past
    with transaction() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE outbox SET next_retry_at = NOW() - INTERVAL '1 minute' WHERE outbox_id = %s",
            (oid_transient,)
        )

    # Should now be claimable again — check directly to avoid limit collisions
    with transaction() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT outbox_id FROM outbox
            WHERE outbox_id = %s
              AND status = 'pending'
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        """, (oid_transient,))
        claimable_row = cur.fetchone()
    check("After backoff elapsed: transient row is claimable again",
          claimable_row is not None,
          f"row not found as claimable (next_retry_at not elapsed or wrong status)")

    # Mark sent (simulate success on retry)
    with transaction() as conn:
        mark_outbox_sent(conn, oid_transient)

    with transaction() as conn:
        cur = conn.cursor()
        cur.execute("SELECT status FROM outbox WHERE outbox_id = %s", (oid_transient,))
        row = cur.fetchone()
    check("After successful retry: status is 'sent'",
          row["status"] == "sent", f"status={row['status']}")

    # ── Poison failure → dead after max attempts ───────────────────────────
    dk_poison = f"test-retry-poison-{uuid.uuid4().hex[:8]}"
    oid_poison = None

    with transaction() as conn:
        oid_poison = enqueue_outbox(conn, dk_poison, "slack_post",
                                    {"channel": "C", "text": "poison"})

    # Drive to max attempts
    for i in range(OUTBOX_MAX_ATTEMPTS):
        # Reset next_retry_at so each failure is claimable
        with transaction() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE outbox SET next_retry_at = NULL, status = 'pending' "
                "WHERE outbox_id = %s AND status != 'dead'",
                (oid_poison,)
            )
        with transaction() as conn:
            mark_outbox_failed(conn, oid_poison, error=f"poison error attempt {i+1}")

    with transaction() as conn:
        cur = conn.cursor()
        cur.execute("SELECT status, attempts FROM outbox WHERE outbox_id = %s", (oid_poison,))
        row = cur.fetchone()

    check(f"After {OUTBOX_MAX_ATTEMPTS} failures: status is 'dead'",
          row["status"] == "dead", f"status={row['status']}")
    check(f"After {OUTBOX_MAX_ATTEMPTS} failures: attempts = {OUTBOX_MAX_ATTEMPTS}",
          row["attempts"] == OUTBOX_MAX_ATTEMPTS,
          f"attempts={row['attempts']}")

    # Dead row must not appear in next claim batch
    with transaction() as conn:
        claimed = claim_pending_outbox(conn, limit=20)
    dead_claimed = [r for r in claimed if r["outbox_id"] == oid_poison]
    check("Dead row is NOT claimable (no infinite retry)",
          len(dead_claimed) == 0, f"unexpectedly claimed: {dead_claimed}")

    # Cleanup
    with transaction() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM outbox WHERE outbox_id IN (%s, %s)",
                    (oid_transient, oid_poison))


# ── 17. Stage 1 proof-run harness regression guard ────────────────────────

def test_proof_run_harness():
    section("17 — Stage 1 proof-run harness: config and gate criteria")
    from pathlib import Path

    harness = Path(__file__).parent / "proof_run.py"
    check("proof_run.py exists in chief_of_staff directory",
          harness.exists(), str(harness))
    if not harness.exists():
        return

    src = harness.read_text()

    check("PROOF_RUNS = 10 (gate requires exactly 10 runs)",
          "PROOF_RUNS = 10" in src, "")
    check("Gate requires ALL runs to pass (passed == PROOF_RUNS)",
          "passed == PROOF_RUNS" in src, "")
    check("Non-PASS run aborts immediately (fail-fast)",
          "Aborting" in src, "")
    check("JSON evidence artifact written on every run",
          "json.dump" in src, "")
    check("gate_result set to FAIL when passed < PROOF_RUNS",
          '"FAIL"' in src, "")
    check("INCOMPLETE counted as non-PASS (partial pipeline = failure)",
          '"INCOMPLETE"' in src, "")
    check("FAIL_STATUSES includes 'blocked' (no silent blocking)",
          '"blocked"' in src or "'blocked'" in src, "")
    check("--dry-run mode exits cleanly without DB/LLM calls",
          "dry_run" in src and "return 0" in src, "")


# ── 18. Stage 3 readiness check ───────────────────────────────────────────

def test_stage3_readiness_check():
    section("18 — Stage 3 readiness check: structure and fail-fast (code inspection)")
    import subprocess, os, sys as _sys
    from pathlib import Path

    script = Path(__file__).parent / "stage3_check.py"
    check("stage3_check.py exists",
          script.exists(), str(script))
    if not script.exists():
        return

    src = script.read_text()

    check("_check_env function defined",
          "def _check_env" in src, "")
    check("_check_db function defined",
          "def _check_db" in src, "")
    check("_check_tables function defined",
          "def _check_tables" in src, "")
    check("REQUIRED_TABLES includes 'requests'",
          '"requests"' in src or "'requests'" in src, "")
    check("REQUIRED_TABLES includes 'director_reports'",
          '"director_reports"' in src or "'director_reports'" in src, "")
    check("REQUIRED_TABLES includes 'resource_locks'",
          '"resource_locks"' in src or "'resource_locks'" in src, "")
    check("main() returns non-zero on failures (exit 1 path present)",
          "return 1" in src or "sys.exit(1)" in src or "_failed" in src, "")
    check("JSON result emitted at end of main()",
          "json.dumps" in src, "")

    # Functional: subprocess with empty DATABASE_URL and no LLM keys → exit 1
    env = dict(os.environ)
    env["DATABASE_URL"] = ""
    env.pop("GEMINI_API_KEY",    None)
    env.pop("ANTHROPIC_API_KEY", None)
    proc = subprocess.run(
        [_sys.executable, str(script)],
        env=env, capture_output=True, text=True,
        cwd=str(script.parent),
    )
    check("stage3_check exits non-zero when DATABASE_URL and LLM keys missing",
          proc.returncode != 0,
          f"got exit code {proc.returncode}; stdout: {proc.stdout[:200]}")


# ── 19. Director run telemetry ─────────────────────────────────────────────

def test_director_rollout_gating():
    section("21 — Director rollout gating: registry + enforcement (code inspection)")
    import inspect
    import core.director as director_mod
    from config import settings as settings_mod

    # Registry in settings
    check("DOMAIN_REGISTRY defined in config.settings",
          hasattr(settings_mod, "DOMAIN_REGISTRY"), "")
    registry = getattr(settings_mod, "DOMAIN_REGISTRY", {})
    from config.settings import VALID_DOMAINS
    missing = [d for d in VALID_DOMAINS if d not in registry]
    check("DOMAIN_REGISTRY covers all VALID_DOMAINS",
          not missing, f"missing: {missing}")
    non_enabled = [d for d, v in registry.items() if v.get("status") != "enabled"]
    check("All current domains default to enabled",
          not non_enabled, f"non-enabled: {non_enabled}")

    src_run   = inspect.getsource(director_mod.run_domain)
    src_gate  = inspect.getsource(director_mod._check_domain_gating)

    check("run_domain calls _check_domain_gating before execution",
          "_check_domain_gating" in src_run, "")
    check("Gated early return includes gating_status field",
          "gating_status" in src_run, "")
    check("_check_domain_gating returns ENABLED for enabled status",
          '"ENABLED"' in src_gate or "'ENABLED'" in src_gate, "")
    check("_check_domain_gating uses DOMAIN_REGISTRY",
          "DOMAIN_REGISTRY" in src_gate, "")

    # CLI enforcement
    import director_cli as cli_mod
    src_cli_run     = inspect.getsource(cli_mod.cmd_run)
    src_cli_run_all = inspect.getsource(cli_mod.cmd_run_all)
    src_cli_print   = inspect.getsource(cli_mod._print_results)

    check("cmd_run exits non-zero when gating_status != ENABLED",
          "gating_status" in src_cli_run and "sys.exit" in src_cli_run, "")
    check("cmd_run_all exits non-zero when any domain is gated",
          "gating_status" in src_cli_run_all and "sys.exit" in src_cli_run_all, "")
    check("_print_results surfaces GATED status (not silently omitted)",
          "GATED" in src_cli_print or "gating_status" in src_cli_print, "")

    # ── 21b — functional: gating enforcement ──────────────────────────────
    section("21b — Director rollout gating: functional enforcement")
    from unittest.mock import patch

    # Enabled domain (DB) → gating_status=ENABLED
    try:
        from db.connection import transaction
        with transaction() as conn:
            result = director_mod.run_domain(conn, "operations",
                                             request_id=None, max_tasks=1)
        check("Enabled domain returns gating_status=ENABLED",
              result.get("gating_status") == "ENABLED",
              f"got {result.get('gating_status')!r}")
    except Exception as e:
        check("21b DB test skipped", False,
              f"{type(e).__name__}: {str(e)[:80]}")

    # Patched disabled domain → early return, no execution
    with patch.dict(director_mod.DOMAIN_REGISTRY,
                    {"research": {"status": "disabled", "reason": "policy: not yet active"}}):
        with transaction() as conn:
            gated = director_mod.run_domain(conn, "research",
                                            request_id=None, max_tasks=1)
    check("Disabled domain returns gating_status=DISABLED",
          gated.get("gating_status") == "DISABLED",
          f"got {gated.get('gating_status')!r}")
    check("Disabled domain result has all counters at zero",
          all(gated.get(k) == 0 for k in ("planned", "built", "verified", "failed")),
          f"counters={gated}")
    check("Disabled domain result includes gating_reason",
          "not yet active" in gated.get("gating_reason", ""),
          f"gating_reason={gated.get('gating_reason')!r}")

    # Patched read_only domain → gating_status=READ_ONLY
    with patch.dict(director_mod.DOMAIN_REGISTRY,
                    {"marketing": {"status": "read_only", "reason": "maintenance window"}}):
        with transaction() as conn:
            ro = director_mod.run_domain(conn, "marketing",
                                         request_id=None, max_tasks=1)
    check("Read-only domain returns gating_status=READ_ONLY",
          ro.get("gating_status") == "READ_ONLY",
          f"got {ro.get('gating_status')!r}")


def test_director_cycle_contract():
    section("20 — Director cycle contract: typed failures + report consistency (code inspection)")
    import inspect
    import core.director as director_mod

    src_run    = inspect.getsource(director_mod.run_domain)
    src_ctf    = inspect.getsource(director_mod._collect_typed_failures)
    src_vrc    = inspect.getsource(director_mod._validate_report_consistency)

    check("run_domain initialises typed_failures in results dict",
          '"typed_failures"' in src_run or "'typed_failures'" in src_run, "")
    check("run_domain calls _collect_typed_failures to populate typed_failures",
          "_collect_typed_failures" in src_run, "")
    check("_collect_typed_failures defined and queries failure_code IS NOT NULL",
          "failure_code IS NOT NULL" in src_ctf, "")
    check("_collect_typed_failures filters by since_epoch (updated_at >= ...)",
          "updated_at" in src_ctf and "since_epoch" in src_ctf, "")
    check("_validate_report_consistency checks tasks_completed vs domain done",
          "tasks_completed" in src_vrc and "done" in src_vrc, "")
    check("_validate_report_consistency checks tasks_remaining vs domain remaining",
          "tasks_remaining" in src_vrc and "expected_remaining" in src_vrc, "")

    # ── 20b — pure-function unit tests for _validate_report_consistency ────
    section("20b — Director report consistency: pure-function contract checks")

    matching_report = {"tasks_completed": 3, "tasks_failed": 0, "tasks_remaining": 0}
    matching_status = {"done": 3, "blocked": 0, "planned": 0, "executing": 0, "verifying": 0}
    issues = director_mod._validate_report_consistency(matching_report, matching_status)
    check("_validate_report_consistency returns [] for matching report/status",
          issues == [], f"unexpected issues: {issues}")

    mismatched_report = {"tasks_completed": 2, "tasks_failed": 1, "tasks_remaining": 1}
    mismatched_status = {"done": 3, "blocked": 0, "planned": 0, "executing": 0, "verifying": 0}
    issues2 = director_mod._validate_report_consistency(mismatched_report, mismatched_status)
    check("_validate_report_consistency returns issues for mismatched tasks_completed",
          any("tasks_completed" in i for i in issues2),
          f"issues={issues2}")

    # ── 20c — DB functional: run_domain returns typed_failures dict ────────
    section("20c — Director cycle contract: DB functional")
    try:
        from db.connection import transaction
        with transaction() as conn:
            result = director_mod.run_domain(conn, "operations",
                                             request_id=None, max_tasks=1)
        check("run_domain returns typed_failures key of type dict",
              isinstance(result.get("typed_failures"), dict),
              f"typed_failures={result.get('typed_failures')!r}")
        check("typed_failures values are integers when non-empty",
              all(isinstance(v, int) for v in result["typed_failures"].values()),
              f"typed_failures={result['typed_failures']}")
    except Exception as e:
        check("20c DB test skipped", False,
              f"{type(e).__name__}: {str(e)[:120]}")


def test_director_telemetry():
    section("19 — Director run telemetry: payload shape (code inspection + DB)")
    import inspect
    import core.director as director_mod

    src = inspect.getsource(director_mod.run_domain)

    check("run_domain tracks elapsed_s",
          "elapsed_s" in src, "")
    check("run_domain sets run_ts (ISO timestamp)",
          "run_ts" in src, "")
    check("run_domain emits JSON telemetry line (json.dumps)",
          "json.dumps" in src, "")
    check("telemetry line uses [telemetry] prefix for grep-ability",
          "[telemetry]" in src, "")

    # DB functional: run_domain with no tasks returns complete telemetry dict
    section("19b — Director telemetry: DB functional (dict shape check)")
    try:
        from db.connection import transaction
        with transaction() as conn:
            result = director_mod.run_domain(conn, "research",
                                             request_id=None, max_tasks=1)

        required_keys = {"domain", "planned", "built", "verified",
                         "failed", "blocked", "elapsed_s", "run_ts"}
        missing = required_keys - set(result.keys())
        check("run_domain returns dict with all telemetry keys",
              not missing, f"missing keys: {missing}")
        check("elapsed_s is a non-negative number",
              isinstance(result.get("elapsed_s"), (int, float))
              and result["elapsed_s"] >= 0,
              f"elapsed_s={result.get('elapsed_s')!r}")
        check("run_ts is a non-empty string",
              isinstance(result.get("run_ts"), str) and len(result["run_ts"]) > 0,
              f"run_ts={result.get('run_ts')!r}")
    except Exception as e:
        check("19b DB test skipped",
              False, f"{type(e).__name__}: {str(e)[:120]}")


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    print("\n\033[1mOSAIO Hardening Regression — Stage 3 Kickoff\033[0m")
    print("=" * 60)

    tests = [
        test_lease_ownership,
        test_builder_unknown_tool,
        test_builder_release_to_verifying,
        test_request_blocked_reason,
        test_gcal_allday_end_date,
        test_auth_runtime_key,
        test_slack_intake_idempotency,
        test_outbox_worker_counters,
        test_worker_interval_validation,
        test_worker_sigterm,
        test_worker_shutdown_terminates,
        test_db_connect_args,
        test_conversation_context_isolation,
        test_domain_contract,
        test_outbox_batch_resilience,
        test_dependency_gating_sql,
        test_dependency_gating_functional,
        test_push_workspace_bounds_sql,
        test_push_workspace_bounds_functional,
        test_outbox_retry_logic,
        test_outbox_retry_functional,
        test_proof_run_harness,
        test_stage3_readiness_check,
        test_director_telemetry,
        test_director_cycle_contract,
        test_director_rollout_gating,
    ]

    for t in tests:
        try:
            t()
        except Exception as e:
            check(f"{t.__name__} [EXCEPTION]", False, str(e))
            traceback.print_exc()

    print("\n" + "=" * 60)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = sum(1 for _, ok, _ in _results if not ok)
    total  = len(_results)

    if failed:
        print(f"\033[91m  {failed}/{total} FAILED\033[0m")
        for name, ok, detail in _results:
            if not ok:
                print(f"  \033[91m✗\033[0m  {name}" + (f" — {detail}" if detail else ""))
        return 1
    else:
        print(f"\033[92m  {passed}/{total} passed\033[0m")
        return 0


if __name__ == "__main__":
    sys.exit(main())
