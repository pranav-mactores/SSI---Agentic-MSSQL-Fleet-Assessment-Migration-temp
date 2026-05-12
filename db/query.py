"""
db/query.py  –  Run a query against a ServerContext connection.

On failure, returns a structured error row that gives Claude enough
context to compose a corrected version-appropriate query.
"""
from __future__ import annotations
from db.connection import ServerContext

# SQL keywords that are never allowed via execute_sql
_BLOCKED = {
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
    "TRUNCATE", "MERGE", "GRANT", "REVOKE", "DENY",
    "EXEC", "EXECUTE", "XP_CMDSHELL", "SP_CONFIGURE",
    "BULK INSERT", "OPENROWSET", "OPENDATASOURCE",
}

def validate_readonly(sql: str) -> str | None:
    """
    Returns an error message if sql contains write/exec keywords,
    or None if the query looks safe to run.
    Only used for the execute_sql escape-hatch tool.
    """
    upper = sql.upper()
    for kw in _BLOCKED:
        # match as whole word to avoid false positives (e.g. "CREATED")
        import re
        if re.search(rf"\b{re.escape(kw)}\b", upper):
            return f"Blocked keyword detected: {kw}. Only SELECT queries are permitted."
    return None


def rq(ctx: ServerContext, sql: str, db: str | None = None) -> list[dict]:
    """
    Execute SQL, return list[dict].
    Never raises — returns a structured error row on failure so Claude
    can see the error details and issue a corrected query via execute_sql.
    """
    try:
        cur = ctx.conn.cursor()
        if db:
            cur.execute(f"USE [{db}];")
        cur.execute(sql)
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        err = str(e)
        # Give Claude everything it needs to write a fix
        return [{
            "_error":          err,
            "_failed_on_db":   db or "master",
            "_server_version": ctx.version_str,
            "_server_year":    str(ctx.year),
            "_edition":        ctx.edition,
            "_hint": _version_hint(err, ctx),
        }]


def _version_hint(error: str, ctx: ServerContext) -> str:
    """Return a human-readable hint about why the query likely failed."""
    err = error.lower()
    year = ctx.year

    if "string_agg" in err:
        return f"STRING_AGG requires SQL 2017+. This server is SQL {year}. Use STUFF(...FOR XML PATH) instead."
    if "invalid column name" in err or "invalid object name" in err:
        col = _extract_quoted(error)
        hints = {
            "is_node":                      "Graph column — requires SQL 2017+",
            "is_edge":                      "Graph column — requires SQL 2017+",
            "ledger_type":                  "Ledger column — requires SQL 2022+",
            "is_dropped_ledger_table":      "Ledger column — requires SQL 2022+",
            "is_accelerated_database_recovery_on": "ADR column — requires SQL 2019+",
            "is_change_feed_enabled":       "Synapse Link column — requires SQL 2022+",
            "is_contained":                 "Contained AG column — requires SQL 2022+",
            "is_remote_data_archive_enabled": "Stretch DB column — available SQL 2016–2022, removed in SQL 2025+.",
            "temporal_type":                "Temporal Tables column — requires SQL 2016+",
            "masked_columns":               "Dynamic Data Masking — requires SQL 2016+",
            "security_policies":            "Row-Level Security — requires SQL 2016+",
            "column_master_keys":           "Always Encrypted — requires SQL 2016+",
            "database_query_store_options": "Query Store — requires SQL 2016+",
        }
        for key, hint in hints.items():
            if key in col.lower() or key in err:
                return hint
        return f"Column or object does not exist on SQL {year}. Check version compatibility."
    if "xml" in err and "path" in err:
        return "FOR XML PATH syntax issue — check string concatenation logic."
    if "permission" in err or "denied" in err:
        return "Permission denied. Ensure migration_analyst login has VIEW SERVER STATE and VIEW ANY DEFINITION."
    if "timeout" in err:
        return "Query timed out. Consider adding NOLOCK hint or reducing scope."
    if year > 2022:
        return (f"Query failed on SQL Server {ctx.version_str} (unrecognised version — "
                f"treat as SQL 2022+ equivalent). Some catalog view columns may have changed; "
                f"check sys.objects joins and remove is_ms_shipped filters if needed.")
    return f"Query failed on SQL Server {year} ({ctx.edition}). May need version-specific rewrite."


def _extract_quoted(text: str) -> str:
    import re
    m = re.search(r"['\"]([^'\"]+)['\"]", text)
    return m.group(1) if m else text


def na_row(reason: str) -> list[dict]:
    return [{"_not_applicable": reason}]
