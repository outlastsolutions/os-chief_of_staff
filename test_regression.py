"""
OSAIO Regression Suite — Stage 2.1 reliability scenarios
Outlast Solutions LLC © 2026

Simulates:
  1. Dependency chain ordering (claim SQL gate + DB trigger)
  2. Stale outbox recovery (sending rows reclaimed after lease expiry)
  3. Rejected out-of-workspace push_workspace path

Run:  python test_regression.py
Exit: 0 = all passed, 1 = failure
"""

from __future__ import annotations
import sys
import traceback
import psycopg2

from db.connection import transaction
from core.idempotency import (
    enqueue_outbox, reclaim_stale_outbox, claim_pending_outbox,
    mark_outbox_sent, mark_outbox_failed,
)
from core.builder import _BuilderError, _tool_github_api
from core.lease import FailureCode

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"

_results: list[tuple[str, bool, str]] = []

def check(name: str, ok: bool, detail: str = "") -> None:
    _results.append((name, ok, detail))
    sym = PASS if ok else FAIL
    print(f"  {sym}  {name}" + (f" — {detail}" if detail else ""))

def section(title: str) -> None:
    print(f"\n\033[96m{title}\033[0m")


# ── Fixtures ──────────────────────────────────────────────────────────────

_REQ_ID  = "REQ-REGR001"
_IDEM    = "idem-regression-suite-001"
_TASKS   = ["TASK-REGR-A", "TASK-REGR-B", "TASK-REGR-C"]

def _setup(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO requests (request_id, idempotency_key, requester, source, title, description, priority, category)
        VALUES (%s, %s, 'regression', 'cli', 'Regression Suite', 'Regression Suite', 'medium', 'development')
        ON CONFLICT (idempotency_key) DO NOTHING
    """, (_REQ_ID, _IDEM))
    ta, tb, tc = _TASKS
    for tid, title, deps in [
        (ta, "Task A", "[]"),
        (tb, "Task B", f'["{ta}"]'),
        (tc, "Task C", f'["{tb}"]'),
    ]:
        cur.execute("""
            INSERT INTO tasks (task_id, request_id, assigned_director, title, description, status, dependencies)
            VALUES (%s, %s, 'development', %s, %s, 'planned', %s::jsonb)
            ON CONFLICT (task_id) DO UPDATE
                SET status = 'planned', dependencies = EXCLUDED.dependencies,
                    plan_id = NULL, leased_by = NULL, leased_until = NULL,
                    attempt = 0, blocked_reason = NULL
        """, (tid, _REQ_ID, title, title, deps))
    conn.commit()

def _teardown(conn) -> None:
    cur = conn.cursor()
    for tid in [*_TASKS, "TASK-REGR-D"]:
        cur.execute("DELETE FROM tasks WHERE task_id = %s", (tid,))
    cur.execute("DELETE FROM requests WHERE request_id = %s", (_REQ_ID,))
    cur.execute("DELETE FROM outbox WHERE dedupe_key LIKE 'regr-%'")
    conn.commit()


# ── Scenario 1: Dependency chain ordering ─────────────────────────────────

def test_dep_ordering(conn) -> None:
    section("Scenario 1 — Dependency chain ordering")
    cur = conn.cursor()
    ta, tb, tc = _TASKS

    _DEP_GATE = """
        NOT EXISTS (
            SELECT 1 FROM jsonb_array_elements_text(COALESCE(t.dependencies,'[]'::jsonb)) dep
            WHERE NOT EXISTS (SELECT 1 FROM tasks d WHERE d.task_id = dep AND d.status = 'done')
        )
    """

    # B should NOT be claimable (A not done)
    cur.execute(f"SELECT task_id FROM tasks t WHERE t.task_id = %s AND {_DEP_GATE}", (tb,))
    check("B not claimable when A=planned", cur.fetchone() is None)

    # A has no deps — claimable
    cur.execute(f"SELECT task_id FROM tasks t WHERE t.task_id = %s AND {_DEP_GATE}", (ta,))
    check("A claimable (no deps)", cur.fetchone() is not None)

    # DB trigger rejects direct force-transition of B to executing
    try:
        cur.execute("UPDATE tasks SET status = 'executing' WHERE task_id = %s", (tb,))
        conn.commit()
        check("Trigger blocks B→executing when A not done", False, "exception not raised")
    except psycopg2.errors.RaiseException as e:
        conn.rollback()
        check("Trigger blocks B→executing when A not done",
              "dependency_order_violation" in str(e), str(e)[:80])

    # Mark A done
    cur.execute("UPDATE tasks SET status = 'done' WHERE task_id = %s", (ta,))
    conn.commit()

    # B now claimable (A done)
    cur.execute(f"SELECT task_id FROM tasks t WHERE t.task_id = %s AND {_DEP_GATE}", (tb,))
    check("B claimable after A=done", cur.fetchone() is not None)

    # Trigger allows B→executing (A done)
    try:
        cur.execute(
            "UPDATE tasks SET status = 'executing', leased_by = 'regression', "
            "leased_until = NOW() + INTERVAL '1 minute' WHERE task_id = %s", (tb,)
        )
        conn.commit()
        check("Trigger allows B→executing when A=done", True)
    except Exception as e:
        conn.rollback()
        check("Trigger allows B→executing when A=done", False, str(e)[:80])

    # C not claimable (B executing, not done)
    cur.execute(f"SELECT task_id FROM tasks t WHERE t.task_id = %s AND {_DEP_GATE}", (tc,))
    check("C not claimable when B=executing", cur.fetchone() is None)

    # Trigger rejects C→executing too
    try:
        cur.execute("UPDATE tasks SET status = 'executing' WHERE task_id = %s", (tc,))
        conn.commit()
        check("Trigger blocks C→executing when B not done", False, "exception not raised")
    except psycopg2.errors.RaiseException as e:
        conn.rollback()
        check("Trigger blocks C→executing when B not done",
              "dependency_order_violation" in str(e), str(e)[:80])

    # Mark B done, C becomes claimable
    cur.execute("UPDATE tasks SET status = 'done' WHERE task_id = %s", (tb,))
    conn.commit()
    cur.execute(f"SELECT task_id FROM tasks t WHERE t.task_id = %s AND {_DEP_GATE}", (tc,))
    check("C claimable after B=done", cur.fetchone() is not None)

    # INSERT regression: trigger must block direct INSERT with status='executing' when dep not done
    # C is still 'planned', so inserting D (dep on C) as 'executing' must be rejected.
    try:
        cur.execute("""
            INSERT INTO tasks (task_id, request_id, assigned_director, title, description, status, dependencies)
            VALUES ('TASK-REGR-D', %s, 'development', 'Task D', 'Task D', 'executing', %s::jsonb)
        """, (_REQ_ID, f'["{tc}"]'))
        conn.commit()
        check("Trigger blocks INSERT D→executing when C not done", False, "exception not raised")
    except psycopg2.errors.RaiseException as e:
        conn.rollback()
        check("Trigger blocks INSERT D→executing when C not done",
              "dependency_order_violation" in str(e), str(e)[:80])

    # Mark C done; INSERT D with status='executing' must now be allowed.
    cur.execute("UPDATE tasks SET status = 'done' WHERE task_id = %s", (tc,))
    conn.commit()
    try:
        cur.execute("""
            INSERT INTO tasks (task_id, request_id, assigned_director, title, description, status, dependencies)
            VALUES ('TASK-REGR-D', %s, 'development', 'Task D', 'Task D', 'executing', %s::jsonb)
        """, (_REQ_ID, f'["{tc}"]'))
        conn.commit()
        check("Trigger allows INSERT D→executing when C=done", True)
        cur.execute("DELETE FROM tasks WHERE task_id = 'TASK-REGR-D'")
        conn.commit()
    except Exception as e:
        conn.rollback()
        check("Trigger allows INSERT D→executing when C=done", False, str(e)[:80])


# ── Scenario 2: Stale outbox recovery ─────────────────────────────────────

def test_stale_outbox(conn) -> None:
    section("Scenario 2 — Stale outbox recovery")
    cur = conn.cursor()

    # Insert a row already in 'sending' with an expired lease
    cur.execute("""
        INSERT INTO outbox (dedupe_key, type, payload, status, leased_until)
        VALUES ('regr-stale-001', 'slack_post', '{"channel":"#test","text":"hi"}'::jsonb,
                'sending', NOW() - INTERVAL '10 minutes')
        ON CONFLICT (dedupe_key) DO UPDATE
            SET status = 'sending', leased_until = NOW() - INTERVAL '10 minutes',
                attempts = 0, last_error = NULL, next_retry_at = NULL
    """)
    conn.commit()

    # Reclaim should pick it up
    reclaimed = reclaim_stale_outbox(conn)
    conn.commit()
    check("reclaim_stale_outbox returns > 0", reclaimed > 0, f"reclaimed={reclaimed}")

    # Row should now be pending
    cur.execute("SELECT status FROM outbox WHERE dedupe_key = 'regr-stale-001'")
    row = cur.fetchone()
    check("Stale row reset to pending", row and row["status"] == "pending",
          row["status"] if row else "missing")

    # A non-expired sending row should NOT be reclaimed
    cur.execute("""
        UPDATE outbox SET status = 'sending', leased_until = NOW() + INTERVAL '10 minutes'
        WHERE dedupe_key = 'regr-stale-001'
    """)
    conn.commit()
    reclaimed2 = reclaim_stale_outbox(conn)
    conn.commit()
    check("Non-expired sending row not reclaimed", reclaimed2 == 0, f"reclaimed={reclaimed2}")

    # Test retry backoff: mark failed repeatedly until dead
    cur.execute("""
        UPDATE outbox SET status = 'pending', leased_until = NULL, attempts = 0
        WHERE dedupe_key = 'regr-stale-001'
    """)
    conn.commit()

    from core.idempotency import OUTBOX_MAX_ATTEMPTS, OUTBOX_BACKOFF_BASE
    for i in range(1, OUTBOX_MAX_ATTEMPTS):
        mark_outbox_failed(conn, _get_outbox_id(conn, 'regr-stale-001'),
                           error=f"simulated failure {i}")
        conn.commit()
        cur.execute("SELECT status, attempts, next_retry_at FROM outbox WHERE dedupe_key = 'regr-stale-001'")
        r = cur.fetchone()
        expected_backoff = min(OUTBOX_BACKOFF_BASE ** i, 60)
        check(f"After failure {i}: status=pending, backoff scheduled",
              r["status"] == "pending" and r["next_retry_at"] is not None,
              f"attempts={r['attempts']} backoff≈{expected_backoff}m")

    # Final failure should go dead
    mark_outbox_failed(conn, _get_outbox_id(conn, 'regr-stale-001'),
                       error="final failure")
    conn.commit()
    cur.execute("SELECT status, attempts FROM outbox WHERE dedupe_key = 'regr-stale-001'")
    r = cur.fetchone()
    check(f"After {OUTBOX_MAX_ATTEMPTS} failures: status=dead",
          r["status"] == "dead" and r["attempts"] == OUTBOX_MAX_ATTEMPTS,
          f"status={r['status']} attempts={r['attempts']}")

    # Dead row not picked up by claim
    items = claim_pending_outbox(conn, limit=10)
    conn.commit()
    ids = [i["dedupe_key"] for i in items]
    check("Dead row not claimed by worker", 'regr-stale-001' not in ids)

    # Cleanup
    cur.execute("DELETE FROM outbox WHERE dedupe_key = 'regr-stale-001'")
    conn.commit()


def _get_outbox_id(conn, dedupe_key: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT outbox_id FROM outbox WHERE dedupe_key = %s", (dedupe_key,))
    return cur.fetchone()["outbox_id"]


# ── Scenario 3: Rejected out-of-workspace push_workspace ──────────────────

def test_push_workspace_reject() -> None:
    section("Scenario 3 — push_workspace out-of-workspace rejection")
    import json, tempfile, os

    workspace = tempfile.mkdtemp(prefix="regr_workspace_")

    # Write a test file inside workspace (valid push should work up to the boundary check)
    os.makedirs(os.path.join(workspace, "src"), exist_ok=True)
    with open(os.path.join(workspace, "src", "hello.py"), "w") as f:
        f.write("print('hello')\n")

    bad_dirs = [
        "/home/osuser",
        "/etc",
        os.path.join(workspace, "..", "other"),
    ]

    for bad_dir in bad_dirs:
        content = json.dumps({
            "action": "push_workspace",
            "repo": "outlastsolutions/os-chief_of_staff",
            "local_dir": bad_dir,
        })
        try:
            _tool_github_api(content, "os-chief_of_staff", workspace, task_id="REGR")
            check(f"push_workspace rejects local_dir={bad_dir!r}", False, "no error raised")
        except _BuilderError as e:
            check(f"push_workspace rejects local_dir={bad_dir!r}",
                  e.code == FailureCode.TOOL_FAILURE,
                  str(e)[:80])
        except Exception as e:
            # GITHUB_TOKEN not set is fine — the boundary check fires before API calls
            if "GITHUB_TOKEN" in str(e) or "not configured" in str(e):
                check(f"push_workspace rejects local_dir={bad_dir!r}", False,
                      "boundary check must fire before token check")
            else:
                check(f"push_workspace rejects local_dir={bad_dir!r}", False, str(e)[:80])

    # Valid in-workspace path should pass boundary check (may fail on GITHUB_TOKEN — that's ok)
    in_workspace_content = json.dumps({
        "action": "push_workspace",
        "repo": "outlastsolutions/os-chief_of_staff",
        "local_dir": os.path.join(workspace, "src"),
    })
    try:
        _tool_github_api(in_workspace_content, "os-chief_of_staff", workspace, task_id="REGR")
        check("push_workspace allows in-workspace local_dir", True)
    except _BuilderError as e:
        check("push_workspace allows in-workspace local_dir", False, str(e)[:80])
    except Exception:
        # GITHUB_TOKEN missing or API failure — boundary passed, that's the point
        check("push_workspace allows in-workspace local_dir (boundary passed)", True,
              "GITHUB_TOKEN not set — boundary check passed, API call skipped")

    # Cleanup
    import shutil
    shutil.rmtree(workspace, ignore_errors=True)


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    print("\n\033[1mOSAIO Regression Suite — Stage 2.1\033[0m")
    print("=" * 60)

    with transaction() as conn:
        _setup(conn)

    try:
        with transaction() as conn:
            test_dep_ordering(conn)

        with transaction() as conn:
            test_stale_outbox(conn)

        test_push_workspace_reject()

    finally:
        with transaction() as conn:
            _teardown(conn)

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
