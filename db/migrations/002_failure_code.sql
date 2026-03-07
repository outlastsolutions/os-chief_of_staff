-- Migration 002: Add failure_code column to tasks
-- Outlast Solutions LLC © 2026
--
-- Typed failure taxonomy so APM can route/learn from failures.
-- Values: TOOL_FAILURE | TEST_FAILURE | BUDGET_EXCEEDED | LEASE_LOST |
--         PLAN_MISSING | LOCK_CONTENTION | INTERNAL_ERROR

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS failure_code TEXT;
