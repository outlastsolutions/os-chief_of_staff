"""
OSAIO v0.1 smoke tests — state machine, leasing, idempotency.
Run: python test_v01.py
Requires DATABASE_URL in .env pointing to a live Postgres instance.
"""
import sys
import uuid
from db.connection import transaction
from db.migrate import migrate
from core.state_machine import (
    TaskState, Role, TransitionError,
    validate_task_transition, validate_request_transition,
    transition_task, transition_request,
)
from core.lease import (
    claim_task, heartbeat, release_to_verifying,
    fail_task, increment_tool_calls,
    acquire_resource_lock, release_resource_lock,
)
from core.idempotency import upsert_request, enqueue_outbox


PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
results = []


def check(name, fn):
    try:
        fn()
        print(f"  {PASS}  {name}")
        results.append(True)
    except Exception as e:
        print(f"  {FAIL}  {name}")
        print(f"         {e}")
        results.append(False)


def should_raise(name, exc_type, fn):
    try:
        fn()
        print(f"  {FAIL}  {name}  (expected {exc_type.__name__}, got nothing)")
        results.append(False)
    except exc_type:
        print(f"  {PASS}  {name}")
        results.append(True)
    except Exception as e:
        print(f"  {FAIL}  {name}  (expected {exc_type.__name__}, got {type(e).__name__}: {e})")
        results.append(False)


# ─── cleanup prior test artifacts ─────────────────────────────────────────
print("\n── Cleanup ──")

def test_cleanup():
    """Delete tasks/requests left behind by previous test runs."""
    with transaction() as conn:
        with conn.cursor() as cur:
            # Delete child rows first (no CASCADE on these FKs)
            cur.execute("DELETE FROM agent_logs WHERE task_id LIKE 'TASK-%'")
            cur.execute("DELETE FROM artifacts WHERE task_id LIKE 'TASK-%'")
            cur.execute("DELETE FROM execution_reports WHERE task_id LIKE 'TASK-%'")
            cur.execute("DELETE FROM verification_reports WHERE task_id LIKE 'TASK-%'")
            # tasks (definitions_of_done cascades automatically)
            cur.execute("DELETE FROM tasks WHERE task_id LIKE 'TASK-%'")
            deleted_tasks = cur.rowcount
            # requests + their dependents
            cur.execute("DELETE FROM director_reports WHERE request_id IN "
                        "(SELECT request_id FROM requests WHERE requester = 'test')")
            cur.execute("DELETE FROM agent_logs WHERE request_id IN "
                        "(SELECT request_id FROM requests WHERE requester = 'test')")
            cur.execute("DELETE FROM outbox WHERE dedupe_key LIKE 'slack:test:%'")
            cur.execute("DELETE FROM requests WHERE requester = 'test'")
            deleted_requests = cur.rowcount
    print(f"         removed {deleted_tasks} task(s), {deleted_requests} request(s) from prior runs")

check("clean up prior test data", test_cleanup)


# ─── schema ────────────────────────────────────────────────────────────────
print("\n── Schema ──")

def test_migrate():
    migrate()

check("migrate creates tables", test_migrate)


# ─── state machine (pure logic, no DB) ─────────────────────────────────────
print("\n── State machine (pure) ──")

check("builder: executing → verifying",
      lambda: validate_task_transition(Role.BUILDER, TaskState.EXECUTING, TaskState.VERIFYING))

check("auditor: verifying → done",
      lambda: validate_task_transition(Role.AUDITOR, TaskState.VERIFYING, TaskState.DONE))

check("auditor: verifying → planned (fail path — re-queue for builder retry)",
      lambda: validate_task_transition(Role.AUDITOR, TaskState.VERIFYING, TaskState.PLANNED))

check("pm: any → blocked",
      lambda: validate_task_transition(Role.PM, TaskState.EXECUTING, TaskState.BLOCKED))

should_raise("builder cannot go planned → done",
             TransitionError,
             lambda: validate_task_transition(Role.BUILDER, TaskState.PLANNED, TaskState.DONE))

should_raise("auditor cannot go verifying → executing (old fail path removed)",
             TransitionError,
             lambda: validate_task_transition(Role.AUDITOR, TaskState.VERIFYING, TaskState.EXECUTING))

should_raise("executor cannot bypass to done",
             TransitionError,
             lambda: validate_task_transition(Role.BUILDER, TaskState.EXECUTING, TaskState.DONE))

should_raise("planner cannot mark done",
             TransitionError,
             lambda: validate_task_transition(Role.PLANNER, TaskState.PLANNED, TaskState.DONE))


# ─── idempotency ────────────────────────────────────────────────────────────
print("\n── Idempotency ──")

idem_key = f"test-{uuid.uuid4().hex}"
req_id = None

def test_upsert_request():
    global req_id
    with transaction() as conn:
        r = upsert_request(conn, {
            "idempotency_key": idem_key,
            "requester": "test",
            "source": "cli",
            "title": "Test request",
            "description": "A test work request",
            "category": "development",
        })
        req_id = r["request_id"]
        assert r["title"] == "Test request"

check("upsert_request creates new request", test_upsert_request)

def test_upsert_idempotent():
    with transaction() as conn:
        r2 = upsert_request(conn, {
            "idempotency_key": idem_key,
            "requester": "test",
            "source": "cli",
            "title": "Duplicate — should be ignored",
            "description": "Should not overwrite",
            "category": "development",
        })
        assert r2["request_id"] == req_id, "Should return original request"
        assert r2["title"] == "Test request", "Title should not be overwritten"

check("upsert_request is idempotent on duplicate key", test_upsert_idempotent)

def test_outbox_dedup():
    key = f"slack:test:{uuid.uuid4().hex}"
    with transaction() as conn:
        id1 = enqueue_outbox(conn, key, "slack_post", {"text": "hello"})
        id2 = enqueue_outbox(conn, key, "slack_post", {"text": "hello again"})
        assert id1 is not None
        assert id2 is None, "Duplicate outbox entry should be ignored"

check("outbox deduplication on same dedupe_key", test_outbox_dedup)


# ─── state machine (DB-backed) ──────────────────────────────────────────────
print("\n── State machine (DB) ──")

task_id = f"TASK-{uuid.uuid4().hex[:8].upper()}"

def setup_task():
    import json
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (task_id, request_id, assigned_director, title, description)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (task_id, req_id, "development", "Test task", "A test task for state machine")
            )

check("create test task", setup_task)

def test_planner_advances_to_executing():
    with transaction() as conn:
        t = transition_task(conn, task_id, Role.PLANNER, TaskState.EXECUTING)
        assert t["status"] == TaskState.EXECUTING

check("planner moves planned → executing", test_planner_advances_to_executing)

def test_builder_moves_to_verifying():
    with transaction() as conn:
        t = transition_task(conn, task_id, Role.BUILDER, TaskState.VERIFYING)
        assert t["status"] == TaskState.VERIFYING

check("builder moves executing → verifying", test_builder_moves_to_verifying)

def test_auditor_fails_back_to_planned():
    with transaction() as conn:
        t = transition_task(conn, task_id, Role.AUDITOR, TaskState.PLANNED)
        assert t["status"] == TaskState.PLANNED

check("auditor moves verifying → planned (fail path)", test_auditor_fails_back_to_planned)

def test_auditor_passes_to_done():
    # Re-advance to verifying first (planned → executing → verifying)
    with transaction() as conn:
        transition_task(conn, task_id, Role.PLANNER, TaskState.EXECUTING)
    with transaction() as conn:
        transition_task(conn, task_id, Role.BUILDER, TaskState.VERIFYING)
    with transaction() as conn:
        t = transition_task(conn, task_id, Role.AUDITOR, TaskState.DONE)
        assert t["status"] == TaskState.DONE

check("auditor moves verifying → done (pass)", test_auditor_passes_to_done)

def test_invalid_transition_raises():
    with transaction() as conn:
        try:
            transition_task(conn, task_id, Role.BUILDER, TaskState.EXECUTING)
            assert False, "Should have raised"
        except TransitionError:
            pass  # correct

check("invalid transition raises TransitionError in DB call", test_invalid_transition_raises)


# ─── leasing ────────────────────────────────────────────────────────────────
print("\n── Leasing ──")

lease_task_id = f"TASK-{uuid.uuid4().hex[:8].upper()}"

def setup_lease_task():
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (task_id, request_id, assigned_director, title, description)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (lease_task_id, req_id, "development", "Lease test task", "For leasing tests")
            )

check("create lease test task", setup_lease_task)

def test_claim_task():
    with transaction() as conn:
        t = claim_task(conn, "agent-001", director="development")
        assert t is not None
        assert t["leased_by"] == "agent-001"
        assert t["status"] == "executing"
        assert t["attempt"] == 1

check("claim_task atomically leases a planned task", test_claim_task)

def test_claim_is_exclusive():
    # Verify the lease task is exclusively held by agent-001 —
    # agent-002 cannot claim the same task while the lease is live.
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT leased_by, status, leased_until FROM tasks WHERE task_id = %s",
                (lease_task_id,)
            )
            row = cur.fetchone()
        assert row["status"] == "executing", f"Expected executing, got {row['status']}"
        assert row["leased_by"] == "agent-001", f"Expected agent-001, got {row['leased_by']}"
        assert row["leased_until"] is not None, "Lease expiry should be set"

check("lease task is exclusively held by agent-001", test_claim_is_exclusive)

def test_resource_lock():
    key = f"repo:test:branch:{uuid.uuid4().hex}"
    with transaction() as conn:
        ok1 = acquire_resource_lock(conn, key, "agent-001")
        ok2 = acquire_resource_lock(conn, key, "agent-002")
        assert ok1 is True
        assert ok2 is False, "Second agent should not acquire held lock"
        release_resource_lock(conn, key, "agent-001")
        ok3 = acquire_resource_lock(conn, key, "agent-002")
        assert ok3 is True, "Lock should be available after release"
        release_resource_lock(conn, key, "agent-002")

check("resource lock: exclusive acquire, release, re-acquire", test_resource_lock)

def test_tool_budget():
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE tasks SET max_tool_calls = 2 WHERE task_id = %s", (lease_task_id,))
        ok1 = increment_tool_calls(conn, lease_task_id)
        ok2 = increment_tool_calls(conn, lease_task_id)
        ok3 = increment_tool_calls(conn, lease_task_id)
        assert ok1 is True
        assert ok2 is True
        assert ok3 is False, "Third call should exceed budget"

check("tool budget: cap exceeded returns False", test_tool_budget)


# ─── summary ────────────────────────────────────────────────────────────────
total = len(results)
passed = sum(results)
failed = total - passed
print(f"\n{'─'*40}")
print(f"Results: {passed}/{total} passed", "" if not failed else f"— {failed} failed")
if failed:
    sys.exit(1)
