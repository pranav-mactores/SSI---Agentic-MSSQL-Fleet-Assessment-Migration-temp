"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

def tool_analyze_procedural_code(ctx: ServerContext, database: str, out_dir: str) -> dict:
    """
    Deep analysis of all stored procedures, scalar/TVF/inline functions,
    and CLR objects in a database.

    Produces 5 CSVs:
      09_proc_overview_{db}.csv   – one row per object, metrics + risk flags
      09_proc_risks_{db}.csv      – one row per risk finding (pivoted)
      09_proc_params_{db}.csv     – all parameters with types
      09_clr_detail_{db}.csv      – CLR assembly + object mapping
      09_proc_deps_{db}.csv       – what each object references
    """
    slug = safe_slug(database)

    def qdb(sql: str) -> list[dict]:
        return rq(ctx, sql, database)

    # ── 1. Per-object overview + complexity metrics ───────────────────────────
    # All risk patterns are checked inline with CASE WHEN … LIKE so we make
    # one pass over sys.sql_modules instead of N separate queries.
    overview_rows = qdb(r"""
        SELECT
            s.name                          AS schema_name,
            o.name                          AS object_name,
            o.type_desc                     AS object_type,
            o.create_date,
            o.modify_date,

            -- size metrics
            LEN(m.definition)                                       AS source_chars,
            LEN(m.definition)
              - LEN(REPLACE(m.definition, CHAR(10), ''))            AS line_count,

            -- parameter count
            (SELECT COUNT(*) FROM sys.parameters p
             WHERE p.object_id = o.object_id
               AND p.parameter_id > 0)                             AS param_count,

            -- dependency count (what this object calls)
            (SELECT COUNT(*) FROM sys.sql_expression_dependencies d
             WHERE d.referencing_id = o.object_id)                 AS dependency_count,

            -- function-specific flags
            ISNULL(OBJECTPROPERTY(o.object_id, 'IsDeterministic'), 0)  AS is_deterministic,
            ISNULL(OBJECTPROPERTY(o.object_id, 'IsSchemaBound'),    0)  AS is_schema_bound,
            CASE WHEN m.uses_native_compilation = 1 THEN 1 ELSE 0 END  AS uses_native_compilation,

            -- ── risk patterns (1 = present) ────────────────────────────────
            CASE WHEN m.definition LIKE '%xp_cmdshell%'
                 THEN 1 ELSE 0 END                                 AS risk_xp_cmdshell,

            CASE WHEN m.definition LIKE '%OPENROWSET%'
                  OR m.definition LIKE '%OPENQUERY%'
                  OR m.definition LIKE '%OPENDATASOURCE%'
                 THEN 1 ELSE 0 END                                 AS risk_external_data,

            CASE WHEN m.definition LIKE '%].%].%].%]%'
                 THEN 1 ELSE 0 END                                 AS risk_four_part_name,

            -- dynamic SQL via EXEC('...') string concat
            CASE WHEN m.definition LIKE '%EXEC%(%''%'
                  OR m.definition LIKE '%EXEC%(@%'
                  OR m.definition LIKE '%EXECUTE%(@%'
                 THEN 1 ELSE 0 END                                 AS risk_dynamic_exec,

            CASE WHEN m.definition LIKE '%sp_executesql%'
                 THEN 1 ELSE 0 END                                 AS risk_sp_executesql,

            -- cursor usage
            CASE WHEN UPPER(m.definition) LIKE '%DECLARE%CURSOR%'
                 THEN 1 ELSE 0 END                                 AS risk_cursor,

            -- global temp tables
            CASE WHEN m.definition LIKE '%##%'
                 THEN 1 ELSE 0 END                                 AS risk_global_temp_table,

            -- old-style error handling
            CASE WHEN m.definition LIKE '%@@ERROR%'
                  AND m.definition NOT LIKE '%TRY%'
                 THEN 1 ELSE 0 END                                 AS risk_no_try_catch,

            -- deprecated outer join syntax
            CASE WHEN m.definition LIKE '%*=%'
                  OR m.definition LIKE '%=*%'
                 THEN 1 ELSE 0 END                                 AS risk_deprecated_join,

            -- SET ROWCOUNT (deprecated; prefer TOP)
            CASE WHEN UPPER(m.definition) LIKE '%SET ROWCOUNT%'
                 THEN 1 ELSE 0 END                                 AS risk_set_rowcount,

            -- NOLOCK / NOLOCK hints (dirty reads)
            CASE WHEN UPPER(m.definition) LIKE '%NOLOCK%'
                  OR UPPER(m.definition) LIKE '%READUNCOMMITTED%'
                 THEN 1 ELSE 0 END                                 AS risk_nolock_hint,

            -- undocumented system procs
            CASE WHEN m.definition LIKE '%sp_MSforeachtable%'
                  OR m.definition LIKE '%sp_MSforeachdb%'
                 THEN 1 ELSE 0 END                                 AS risk_undocumented_proc,

            -- GOTO control flow
            CASE WHEN UPPER(m.definition) LIKE '%' + CHAR(10) + 'GOTO %'
                  OR UPPER(m.definition) LIKE '% GOTO %'
                 THEN 1 ELSE 0 END                                 AS risk_goto,

            -- SELECT * (non-deterministic columns)
            CASE WHEN m.definition LIKE '%SELECT *%'
                  OR m.definition LIKE '%SELECT%*%FROM%'
                 THEN 1 ELSE 0 END                                 AS risk_select_star,

            -- WAITFOR (blocking)
            CASE WHEN UPPER(m.definition) LIKE '%WAITFOR%'
                 THEN 1 ELSE 0 END                                 AS risk_waitfor,

            -- RECOMPILE hint (masks plan instability)
            CASE WHEN UPPER(m.definition) LIKE '%WITH RECOMPILE%'
                  OR UPPER(m.definition) LIKE '%OPTION (RECOMPILE)%'
                 THEN 1 ELSE 0 END                                 AS risk_recompile,

            -- local temp tables (informational)
            CASE WHEN m.definition LIKE '%INTO #%'
                  OR m.definition LIKE '%CREATE TABLE #%'
                 THEN 1 ELSE 0 END                                 AS uses_temp_table,

            -- table variables
            CASE WHEN UPPER(m.definition) LIKE '%DECLARE %TABLE%'
                 THEN 1 ELSE 0 END                                 AS uses_table_variable,

            -- BEGIN TRY / CATCH (positive signal)
            CASE WHEN UPPER(m.definition) LIKE '%BEGIN TRY%'
                 THEN 1 ELSE 0 END                                 AS has_try_catch,

            -- EXECUTE AS (impersonation)
            CASE WHEN UPPER(m.definition) LIKE '%EXECUTE AS%'
                  OR UPPER(m.definition) LIKE '%WITH EXECUTE AS%'
                 THEN 1 ELSE 0 END                                 AS uses_execute_as,

            -- approximate nesting depth via BEGIN count
            LEN(UPPER(m.definition))
              - LEN(REPLACE(UPPER(m.definition), 'BEGIN', ''))     AS begin_count_x5,

            -- xp_ extended procedure calls
            CASE WHEN m.definition LIKE '%xp_%'
                  AND m.definition NOT LIKE '%xp_cmdshell%'
                 THEN 1 ELSE 0 END                                 AS risk_xp_other,

            -- RAISERROR with old two-arg form
            CASE WHEN UPPER(m.definition) LIKE '%RAISERROR%'
                 THEN 1 ELSE 0 END                                 AS uses_raiserror,

            -- THROW (modern, 2012+)
            CASE WHEN UPPER(m.definition) LIKE '%THROW%'
                 THEN 1 ELSE 0 END                                 AS uses_throw

        FROM sys.objects o
        JOIN sys.sql_modules m ON o.object_id = m.object_id
        JOIN sys.schemas     s ON o.schema_id = s.schema_id
        WHERE o.type IN ('P')
          AND o.is_ms_shipped = 0
          AND s.name NOT IN ('sys', 'cdc')
        ORDER BY o.type_desc, s.name, o.name;
    """)

    # ── 2. Parameters ─────────────────────────────────────────────────────────
    param_rows = qdb("""
        SELECT
            s.name                          AS schema_name,
            o.name                          AS object_name,
            o.type_desc                     AS object_type,
            p.parameter_id,
            p.name                          AS param_name,
            t.name                          AS param_type,
            p.max_length,
            p.precision,
            p.scale,
            p.is_output,
            p.has_default_value,
            p.default_value
        FROM sys.parameters  p
        JOIN sys.objects      o ON p.object_id   = o.object_id
        JOIN sys.schemas      s ON o.schema_id   = s.schema_id
        JOIN sys.types        t ON p.user_type_id = t.user_type_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF')
          AND o.is_ms_shipped = 0
          AND s.name NOT IN ('sys', 'cdc')
          AND p.parameter_id > 0
        ORDER BY s.name, o.name, p.parameter_id;
    """)

    # ── 3. Dependencies – what each proc/function references ─────────────────
    dep_rows = qdb("""
        SELECT
            s.name                              AS referencing_schema,
            o.name                              AS referencing_object,
            o.type_desc                         AS referencing_type,
            d.referenced_server_name,
            d.referenced_database_name,
            d.referenced_schema_name,
            d.referenced_entity_name,
            d.is_caller_dependent,
            d.is_ambiguous
        FROM sys.sql_expression_dependencies d
        JOIN sys.objects o ON d.referencing_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'AF')
          AND o.is_ms_shipped = 0
          AND s.name NOT IN ('sys', 'cdc')
        ORDER BY s.name, o.name, d.referenced_entity_name;
    """)

    # ── 4. CLR detail ─────────────────────────────────────────────────────────
    clr_rows = qdb("""
        SELECT
            a.name                          AS assembly_name,
            a.clr_name,
            a.permission_set_desc,
            a.is_visible,
            a.create_date                   AS assembly_created,
            o.name                          AS object_name,
            o.type_desc                     AS object_type,
            s.name                          AS schema_name,
            am.assembly_class,
            am.assembly_method,
            am.null_on_null_input,
            am.execute_as_principal_id,
            -- how many places call this CLR object
            (SELECT COUNT(*) FROM sys.sql_expression_dependencies d
             WHERE d.referenced_id = o.object_id)  AS caller_count
        FROM sys.assemblies        a
        JOIN sys.assembly_modules  am ON a.assembly_id  = am.assembly_id
        JOIN sys.objects           o  ON am.object_id   = o.object_id
        JOIN sys.schemas           s  ON o.schema_id    = s.schema_id
        WHERE a.is_user_defined = 1
          AND s.name NOT IN ('sys', 'cdc')
        ORDER BY a.name, o.name;
    """)

    # ── 5. Pivot risk flags into one-row-per-risk format ──────────────────────
    risk_columns = [
        ("risk_xp_cmdshell",       "🔴 CRITICAL", "xp_cmdshell – OS command execution"),
        ("risk_external_data",     "🔴 HIGH",     "OPENROWSET / OPENQUERY / OPENDATASOURCE"),
        ("risk_four_part_name",    "🟠 HIGH",     "Four-part linked server name reference"),
        ("risk_dynamic_exec",      "🟠 HIGH",     "Dynamic SQL via EXEC with string concat"),
        ("risk_sp_executesql",     "🟡 MEDIUM",   "sp_executesql (dynamic SQL)"),
        ("risk_cursor",            "🟡 MEDIUM",   "CURSOR – potential row-by-row perf issue"),
        ("risk_global_temp_table", "🟡 MEDIUM",   "Global temp table (##) – session scope risk"),
        ("risk_no_try_catch",      "🟡 MEDIUM",   "@@ERROR without TRY/CATCH – old error handling"),
        ("risk_deprecated_join",   "🟠 HIGH",     "Deprecated *= / =* outer join syntax"),
        ("risk_set_rowcount",      "🟡 MEDIUM",   "SET ROWCOUNT – deprecated; use TOP"),
        ("risk_nolock_hint",       "🟡 MEDIUM",   "NOLOCK / READUNCOMMITTED hint"),
        ("risk_undocumented_proc", "🟠 HIGH",     "Undocumented SP (sp_MSforeachtable etc.)"),
        ("risk_goto",              "🔵 LOW",      "GOTO – old control flow"),
        ("risk_select_star",       "🔵 LOW",      "SELECT * – non-deterministic columns"),
        ("risk_waitfor",           "🟡 MEDIUM",   "WAITFOR – explicit blocking"),
        ("risk_recompile",         "🔵 LOW",      "WITH RECOMPILE / OPTION(RECOMPILE) hint"),
        ("risk_xp_other",          "🟠 HIGH",     "Other xp_ extended procedure call"),
        ("uses_raiserror",         "🔵 INFO",     "RAISERROR – consider replacing with THROW"),
        ("uses_execute_as",        "🔵 INFO",     "EXECUTE AS impersonation context"),
    ]

    risk_rows = []
    for obj in overview_rows:
        if "_error" in obj:
            continue
        for col, severity, description in risk_columns:
            if str(obj.get(col, "0")) == "1":
                risk_rows.append({
                    "schema_name":   obj.get("schema_name"),
                    "object_name":   obj.get("object_name"),
                    "object_type":   obj.get("object_type"),
                    "severity":      severity,
                    "risk_type":     col,
                    "description":   description,
                    "line_count":    obj.get("line_count"),
                    "param_count":   obj.get("param_count"),
                })

    # ── Write CSVs ────────────────────────────────────────────────────────────
    write_csv(f"{out_dir}/09_proc_overview_{slug}.csv",  overview_rows)
    write_csv(f"{out_dir}/09_proc_risks_{slug}.csv",     risk_rows)
    write_csv(f"{out_dir}/09_proc_params_{slug}.csv",    param_rows)
    write_csv(f"{out_dir}/09_clr_detail_{slug}.csv",     clr_rows)
    write_csv(f"{out_dir}/09_proc_deps_{slug}.csv",      dep_rows)

    # ── Risk summary for return value ─────────────────────────────────────────
    risk_summary: dict[str, int] = {}
    for r in risk_rows:
        k = r["risk_type"]
        risk_summary[k] = risk_summary.get(k, 0) + 1

    critical_objects = [r["object_name"] for r in risk_rows
                        if r["severity"].startswith("🔴")]

    return {
        "database": database,
        "total_procedures": sum(1 for r in overview_rows
                                if r.get("object_type") == "SQL_STORED_PROCEDURE"),
        "total_functions":  sum(1 for r in overview_rows
                                if r.get("object_type") not in
                                   ("SQL_STORED_PROCEDURE","CLR_STORED_PROCEDURE")),
        "total_clr_objects": len(clr_rows),
        "total_risk_findings": len(risk_rows),
        "critical_risk_objects": list(set(critical_objects)),
        "risk_summary": risk_summary,
        "overview_sample": overview_rows[:5],    # first 5 rows for agent context
        "clr_detail": clr_rows,
        "top_risks": risk_rows[:20],             # top 20 for agent report
    }



