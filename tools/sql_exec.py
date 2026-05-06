"""
tools/sql_exec.py  –  The execute_sql escape-hatch tool.

Claude calls this when a standard tool returns {"_error": "..."} and it
needs to run a corrected, version-appropriate query to recover the data.

Safety:
  - Only SELECT / WITH / USE statements are permitted
  - All write keywords (INSERT, UPDATE, DROP, EXEC, xp_cmdshell etc.) are blocked
  - Results are capped at 500 rows to prevent context overflow
"""
from __future__ import annotations
from db.connection import ServerContext
from db.query import validate_readonly, rq

MAX_ROWS = 500

def tool_execute_sql(ctx: ServerContext, sql: str,
                     purpose: str, database: str | None = None) -> dict:
    """
    Run a custom SELECT query composed by Claude as a version-appropriate
    replacement for a failed standard-tool query.
    """
    # Safety check — block all write operations
    blocked = validate_readonly(sql)
    if blocked:
        return {
            "_error":   blocked,
            "_blocked": True,
            "advice":   "Rewrite the query using only SELECT statements.",
        }

    rows = rq(ctx, sql, db=database or None)

    # Truncate to avoid flooding the context window
    truncated = False
    if len(rows) > MAX_ROWS:
        rows = rows[:MAX_ROWS]
        truncated = True

    return {
        "purpose":   purpose,
        "database":  database or "master",
        "row_count": len(rows),
        "truncated": truncated,
        "results":   rows,
    }
