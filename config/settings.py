import os
from dotenv import load_dotenv

load_dotenv()

# ── Database ──────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── LLM providers ────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")

# Model selection per agent role.
# Values: any Claude model ID, or any Gemini model ID.
# Defaults to Gemini Flash if not set.
PM_MODEL      = os.getenv("PM_MODEL",      "gemini-2.5-flash")
APM_MODEL     = os.getenv("APM_MODEL",     "gemini-2.5-flash")
PLANNER_MODEL = os.getenv("PLANNER_MODEL", "gemini-2.5-flash")
BUILDER_MODEL = os.getenv("BUILDER_MODEL", "gemini-2.5-pro")
AUDITOR_MODEL   = os.getenv("AUDITOR_MODEL",   "gemini-2.5-flash")
DIRECTOR_MODEL  = os.getenv("DIRECTOR_MODEL",  "gemini-2.5-flash")

# ── Director domains ──────────────────────────────────────────────────────
# Single canonical source for the set of valid director domains.
# All domain references in PM/APM prompts, Director, and CLIs derive from this.
VALID_DOMAINS: tuple[str, ...] = ("development", "operations", "research", "marketing")

# ── Director domain rollout registry ──────────────────────────────────────
# Per-domain execution eligibility. Enforced by run_domain() and director CLI.
# status values:
#   enabled   — normal autonomous execution
#   read_only — domain tasks can be queried/reported but not executed (maintenance)
#   disabled  — domain is not activated for autonomous execution
DOMAIN_REGISTRY: dict = {
    "development": {"status": "enabled",  "reason": ""},
    "operations":  {"status": "enabled",  "reason": ""},
    "research":    {"status": "enabled",  "reason": ""},
    "marketing":   {"status": "enabled",  "reason": ""},
}

# ── Director domain cycle execution budgets ───────────────────────────────
# Per-domain cycle limits enforced by run_domain().
# max_tasks:     max task iterations per cycle (None = use run_domain caller default)
# max_runtime_s: max wall-clock seconds per cycle (None = no runtime limit)
# DOMAIN_REGISTRY overrides max_tasks when both are set; None means no override.
DOMAIN_BUDGETS: dict = {
    "development": {"max_tasks": None, "max_runtime_s": None},
    "operations":  {"max_tasks": None, "max_runtime_s": None},
    "research":    {"max_tasks": None, "max_runtime_s": None},
    "marketing":   {"max_tasks": None, "max_runtime_s": None},
}

# ── Director cycle SLO guardrails ─────────────────────────────────────────
# Per-domain thresholds evaluated after each run_domain() cycle.
# max_blocked:   max tolerated blocked task count before SLO breach
# max_failed:    max tolerated failed task count before SLO breach
# max_elapsed_s: max tolerated wall-clock seconds before SLO breach
# None = no threshold on that dimension (never breaches — backward-compatible default)
DOMAIN_SLOS: dict = {
    "development": {"max_blocked": None, "max_failed": None, "max_elapsed_s": None},
    "operations":  {"max_blocked": None, "max_failed": None, "max_elapsed_s": None},
    "research":    {"max_blocked": None, "max_failed": None, "max_elapsed_s": None},
    "marketing":   {"max_blocked": None, "max_failed": None, "max_elapsed_s": None},
}

# ── Director cycle typed response policy ──────────────────────────────────
# Maps typed cycle outcome keys to bounded automatic response actions.
# Actions:
#   observe       — log only; no outbox side effect (default safe value)
#   escalate_once — enqueue one outbox notification, idempotent per cycle identity
# Outcome keys:
#   gating_non_enabled — domain is not enabled (read_only / disabled)
#   budget_hit         — cycle exhausted max_tasks or max_runtime_s
#   slo_breach         — cycle results exceeded a DOMAIN_SLOS threshold
CYCLE_RESPONSE_POLICY: dict = {
    "gating_non_enabled": "observe",
    "budget_hit":         "escalate_once",
    "slo_breach":         "escalate_once",
}

# ── Director approval checkpoint ──────────────────────────────────────────
# When enabled, the Development Director reviews every plan before Builder
# claims the task. approve | revise | escalate.
# Set to "true" to activate. Off by default to keep the loop fast.
DIRECTOR_APPROVAL_ENABLED = os.getenv("DIRECTOR_APPROVAL_ENABLED", "false").lower() == "true"

# ── Slack ─────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_TASKS_CHANNEL = os.getenv("SLACK_TASKS_CHANNEL", "C0AJL6RCYKU")

# ── Secretary integration ─────────────────────────────────────────────────
SECRETARY_URL     = os.getenv("SECRETARY_URL", "http://localhost:8000")
SECRETARY_API_KEY = os.getenv("SECRETARY_API_KEY", "")

# ── GitHub ─────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")       # Personal access token
GITHUB_ORG   = os.getenv("GITHUB_ORG", "outlastsolutions")
