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
