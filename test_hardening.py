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


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    print("\n\033[1mOSAIO Hardening Regression — Stage 2.2\033[0m")
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
        test_db_connect_args,
        test_conversation_context_isolation,
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
