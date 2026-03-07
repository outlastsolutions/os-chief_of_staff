-- Migration 003: Add thread_ts to requests for Slack thread-aware replies
-- Outlast Solutions LLC © 2026

ALTER TABLE requests ADD COLUMN IF NOT EXISTS thread_ts TEXT;
