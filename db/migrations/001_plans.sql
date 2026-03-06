-- Migration 001 — Plans table
-- Outlast Solutions LLC © 2026

CREATE TABLE IF NOT EXISTS plans (
    plan_id         TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    created_by      TEXT NOT NULL DEFAULT 'planner',
    -- The full plan as structured JSON
    steps           JSONB NOT NULL DEFAULT '[]',
    -- Each step: {order, title, description, tool, expected_output, risk}
    test_strategy   TEXT,
    risks           JSONB NOT NULL DEFAULT '[]',
    estimated_tool_calls INT,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS plans_task_id_uq ON plans (task_id);

-- Add plan_id reference to tasks
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS plan_id TEXT REFERENCES plans(plan_id);
