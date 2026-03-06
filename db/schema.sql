-- OSAIO Core Schema v0.1
-- Outlast Solutions LLC © 2026

-- ─────────────────────────────────────────────
-- REQUESTS
-- Normalized work request object. Every unit of
-- work entering the system maps to one request.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS requests (
    request_id      TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    requester       TEXT NOT NULL,          -- name of person/system
    source          TEXT NOT NULL,          -- 'slack' | 'cli' | 'api'
    channel         TEXT,                   -- slack channel or endpoint
    business_unit   TEXT,                   -- e.g. 'xout', 'cyberlight'
    title           TEXT NOT NULL,
    description     TEXT NOT NULL,
    priority        TEXT NOT NULL DEFAULT 'medium', -- low | medium | high | critical
    category        TEXT NOT NULL,          -- development | operations | research | marketing
    constraints     JSONB NOT NULL DEFAULT '[]',
    systems_involved JSONB NOT NULL DEFAULT '[]',
    attachments     JSONB NOT NULL DEFAULT '[]',
    deadline        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'received',
    -- received | scoped | in_progress | done | blocked | cancelled
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS requests_idempotency_key_uq
    ON requests (idempotency_key);

CREATE INDEX IF NOT EXISTS requests_status_idx ON requests (status);
CREATE INDEX IF NOT EXISTS requests_business_unit_idx ON requests (business_unit);


-- ─────────────────────────────────────────────
-- TASKS
-- APM decomposes each request into tasks.
-- Only one agent can hold a task lease at a time.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tasks (
    task_id             TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL REFERENCES requests(request_id),
    created_by          TEXT NOT NULL DEFAULT 'apm',
    assigned_director   TEXT NOT NULL,      -- development | operations | marketing | research | compute
    title               TEXT NOT NULL,
    description         TEXT NOT NULL,
    tools_allowed       JSONB NOT NULL DEFAULT '[]',
    dependencies        JSONB NOT NULL DEFAULT '[]',  -- list of task_ids this depends on
    -- State machine states:
    -- received | scoped | planned | executing | verifying | done | blocked | cancelled
    status              TEXT NOT NULL DEFAULT 'planned',
    blocked_reason      TEXT,
    -- Leasing (concurrency control)
    leased_by           TEXT,               -- agent identifier
    leased_until        TIMESTAMPTZ,
    attempt             INT NOT NULL DEFAULT 0,
    -- Tool budget enforcement
    tool_calls_used     INT NOT NULL DEFAULT 0,
    max_tool_calls      INT NOT NULL DEFAULT 20,
    -- Complexity routing
    complexity          TEXT NOT NULL DEFAULT 'medium', -- low | medium | high
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS tasks_request_id_idx ON tasks (request_id);
CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status);
CREATE INDEX IF NOT EXISTS tasks_assigned_director_idx ON tasks (assigned_director);
CREATE INDEX IF NOT EXISTS tasks_lease_idx ON tasks (status, leased_until);

-- Prevent duplicate task kinds within the same request
CREATE UNIQUE INDEX IF NOT EXISTS tasks_unique_kind_per_request
    ON tasks (request_id, title);


-- ─────────────────────────────────────────────
-- DEFINITIONS OF DONE
-- Every task must have acceptance criteria before
-- execution begins. Auditor checks these exactly.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS definitions_of_done (
    dod_id              TEXT PRIMARY KEY,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    goal                TEXT NOT NULL,
    acceptance_criteria JSONB NOT NULL DEFAULT '[]',
    constraints         JSONB NOT NULL DEFAULT '[]',
    evidence_required   JSONB NOT NULL DEFAULT '[]',
    security_checks     JSONB NOT NULL DEFAULT '[]',
    rollback_plan       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS dod_task_id_uq ON definitions_of_done (task_id);


-- ─────────────────────────────────────────────
-- EXECUTION REPORTS
-- Builder produces this when work is complete.
-- Must include artifacts + evidence.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS execution_reports (
    report_id   TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks(task_id),
    executor    TEXT NOT NULL,
    status      TEXT NOT NULL,  -- completed | failed | partial
    artifacts   JSONB NOT NULL DEFAULT '[]',
    git_commit  TEXT,
    logs        JSONB NOT NULL DEFAULT '[]',
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS exec_reports_task_id_idx ON execution_reports (task_id);


-- ─────────────────────────────────────────────
-- VERIFICATION REPORTS
-- Auditor produces this. PASS gates DONE.
-- FAIL returns task to Builder with issues list.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS verification_reports (
    report_id        TEXT PRIMARY KEY,
    task_id          TEXT NOT NULL REFERENCES tasks(task_id),
    verifier         TEXT NOT NULL,
    result           TEXT NOT NULL,  -- pass | fail
    checks           JSONB NOT NULL DEFAULT '{}',
    issues           JSONB NOT NULL DEFAULT '[]',
    confidence_score FLOAT,
    evidence         JSONB NOT NULL DEFAULT '[]',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS verif_reports_task_id_idx ON verification_reports (task_id);


-- ─────────────────────────────────────────────
-- DIRECTOR REPORTS
-- Directors aggregate task completion per request.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS director_reports (
    report_id           TEXT PRIMARY KEY,
    request_id          TEXT NOT NULL REFERENCES requests(request_id),
    director            TEXT NOT NULL,
    tasks_completed     INT NOT NULL DEFAULT 0,
    tasks_failed        INT NOT NULL DEFAULT 0,
    tasks_remaining     INT NOT NULL DEFAULT 0,
    overall_status      TEXT NOT NULL,  -- in_progress | complete | blocked
    summary             TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS director_reports_request_id_idx ON director_reports (request_id);


-- ─────────────────────────────────────────────
-- AGENT LOGS
-- Structured audit trail for every agent action.
-- Required for debugging + replaying tasks.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agent_logs (
    log_id      BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_name  TEXT NOT NULL,
    role        TEXT NOT NULL,  -- pm | apm | director | planner | builder | auditor | secretary
    action      TEXT NOT NULL,
    task_id     TEXT REFERENCES tasks(task_id),
    request_id  TEXT REFERENCES requests(request_id),
    log_data    JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS agent_logs_task_id_idx ON agent_logs (task_id);
CREATE INDEX IF NOT EXISTS agent_logs_created_at_idx ON agent_logs (created_at);
CREATE INDEX IF NOT EXISTS agent_logs_role_idx ON agent_logs (role);


-- ─────────────────────────────────────────────
-- OUTBOX
-- All side effects (Slack posts, GitHub comments)
-- go through here. Dedupe key prevents doubles.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id   BIGSERIAL PRIMARY KEY,
    dedupe_key  TEXT NOT NULL,
    type        TEXT NOT NULL,   -- slack_post | github_comment | email | webhook
    payload     JSONB NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | sent | failed
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS outbox_dedupe_key_uq ON outbox (dedupe_key);
CREATE INDEX IF NOT EXISTS outbox_status_idx ON outbox (status);


-- ─────────────────────────────────────────────
-- RESOURCE LOCKS
-- Prevents two agents editing the same file,
-- branch, or shared resource simultaneously.
-- TTL-based so dead agents don't hold locks forever.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS resource_locks (
    lock_key    TEXT PRIMARY KEY,   -- e.g. 'repo:xout:branch:task/TASK-001' or 'file:agents.py'
    owner       TEXT NOT NULL,      -- agent identifier
    leased_until TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS resource_locks_leased_until_idx ON resource_locks (leased_until);


-- ─────────────────────────────────────────────
-- ARTIFACTS
-- All deliverables produced by Builders.
-- Referenced in execution + verification reports.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL REFERENCES tasks(task_id),
    type        TEXT NOT NULL,   -- file | git_commit | doc | report | image
    path        TEXT,            -- local or repo path
    url         TEXT,            -- remote URL if applicable
    metadata    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS artifacts_task_id_idx ON artifacts (task_id);
