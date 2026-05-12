"""
config/settings.py  –  All configuration loaded from .env / environment.

ANTHROPIC_API_KEY is the only required variable.
The Anthropic SDK reads it automatically from os.environ —
you never pass it explicitly in code.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    _root = Path(__file__).resolve().parent.parent
    load_dotenv(_root / ".env", override=False)
except ImportError:
    pass  # fall back to real environment variables

def _require(key: str) -> str:
    val = os.getenv(key, "").strip()
    if not val:
        raise EnvironmentError(
            f"\n\n  Missing required environment variable: {key}\n"
            f"  Fix:\n"
            f"    1. Copy .env.example → .env\n"
            f"    2. Set {key}=<your value> in .env\n"
            f"    3. Re-run the script\n"
        )
    return val

def _opt(key: str, default: str) -> str:
    return os.getenv(key, default).strip() or default

def get_anthropic_api_key() -> str:
    """No-op when using Bedrock — kept for backwards compatibility."""
    return os.getenv("ANTHROPIC_API_KEY", "")

CLAUDE_MODEL    = _opt("CLAUDE_MODEL",    "us.anthropic.claude-opus-4-5-20251101-v1:0")
AGENT_MAX_TURNS = int(_opt("AGENT_MAX_TURNS", "40"))
ODBC_DRIVER     = _opt("ODBC_DRIVER",     "ODBC Driver 17 for SQL Server")
CONN_TIMEOUT    = int(_opt("CONN_TIMEOUT",    "30"))
