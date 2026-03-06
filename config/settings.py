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
AUDITOR_MODEL = os.getenv("AUDITOR_MODEL", "gemini-2.5-flash")

# ── Slack ─────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_TASKS_CHANNEL = os.getenv("SLACK_TASKS_CHANNEL", "C0AJL6RCYKU")
