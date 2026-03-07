# OSAIO Escalation Policy
Outlast Solutions LLC © 2026

This document defines when the autonomous pipeline must stop and surface a decision
to a human. These rules are enforced in code — not advisory.

---

## What escalation means

Escalation sets the task to `blocked` with `failure_code = HUMAN_DECISION_REQUIRED`.
A Slack notification is posted to #tasks via the outbox. The task stays blocked until
a human reviews it and either:
- Unblocks it manually (edits and re-queues it with corrected DoD or description)
- Cancels it

No autonomous agent will retry an escalated task.

---

## Rule 1 — File count exceeds limit

**Fires when:** A plan contains more than `MAX_FILES_PER_TASK` (default: 8) `file_edit` steps.

**Checked by:** Builder, after plan is loaded.

**Why:** A task touching many files is almost certainly doing too many things at once.
Split it. One task, one coherent change.

**Action:** Split the request into smaller tasks through APM.

---

## Rule 2 — Schema migration alongside application code

**Fires when:** A plan contains both schema migration steps (ALTER TABLE, CREATE TABLE,
.sql files) and application code changes (.py, .js, .ts, etc.) in the same task.

**Checked by:** Builder, after plan is loaded.

**Why:** Migrations and application code changes must be separate, ordered, and
independently rollback-able. Combining them creates deploy-order risk.

**Action:** Split into two tasks: (1) migration, (2) application changes, with explicit dependency.

---

## Rule 3 — Sensitive domain

**Fires when:** The task description or plan steps reference high-risk patterns:
passwords, credentials, private keys, API keys, billing, payments, Stripe secrets,
webhook secrets, DROP TABLE, TRUNCATE TABLE, IAM roles, root access, .env files.

**Checked by:** Planner (on task description), Builder (on plan steps).

**Why:** These operations carry irreversible risk. A human must review before execution.

**Action:** Human reviews the task, validates the plan is safe, and re-queues it.

---

## Rule 4 — Repeated failure class

**Fires when:** A task has failed >= 2 times with the same failure_code.

**Checked by:** Planner, before generating a new plan.

**Why:** Two identical failures mean autonomous retry is not converging.

**Action:** Human inspects agent_logs, corrects the root cause, resets attempt = 0, unblocks.

---

## Rule 5 — Missing acceptance criteria

**Fires when:** A task has no acceptance criteria in its Definition of Done.

**Checked by:** Planner, before generating a plan.

**Why:** The Auditor verifies against acceptance criteria. Without them, completion cannot be verified.

**Action:** Human adds testable acceptance criteria to the task's DoD, then re-queues.

---

## Director approval checkpoint (optional)

When DIRECTOR_APPROVAL_ENABLED=true, the Director reviews each plan before Builder claims it.

Verdicts:
- approve  — proceed to build
- revise   — plan cleared; Planner re-plans with Director feedback
- escalate — task blocked with HUMAN_DECISION_REQUIRED

Disabled by default. Enable for production workloads where plan quality is critical.

---

## Failure codes

| Code                     | Source                          | Retry policy        |
|--------------------------|---------------------------------|---------------------|
| TOOL_FAILURE             | Tool call raised an exception   | Up to MAX_ATTEMPTS  |
| TEST_FAILURE             | code_run / shell non-zero exit  | Up to MAX_ATTEMPTS  |
| BUDGET_EXCEEDED          | Tool call budget exhausted      | Up to MAX_ATTEMPTS  |
| LEASE_LOST               | Heartbeat failed mid-execution  | Up to MAX_ATTEMPTS  |
| LOCK_CONTENTION          | Resource locked by another agent| Up to MAX_ATTEMPTS  |
| PLAN_MISSING             | No plan at claim time           | Up to MAX_ATTEMPTS  |
| INTERNAL_ERROR           | Unexpected exception            | Up to MAX_ATTEMPTS  |
| HUMAN_DECISION_REQUIRED  | Escalation rule fired           | No autonomous retry |

MAX_ATTEMPTS = 3 (core/lease.py). After MAX_ATTEMPTS with any other code, the task
goes blocked and Rule 4 fires on the next planning cycle.
