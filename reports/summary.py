"""reports/summary.py – Build 00_server_summary.csv from task CSVs."""
import csv
import os
from collections import Counter
from datetime import datetime
from db.connection import ServerContext
from reports.csv_writer import write_csv, safe_slug, read_csv, isum, col_val

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def generate_server_summary_csv(ctx, out_dir: str, server: str, port: int) -> str:
    """
    Build a structured key-value summary CSV from per-task CSVs already written.
    Columns: category | metric | value | flag | detail
    Returns the path of the written file.
    """
    rows: list[dict] = []

    def add(category: str, metric: str, value, flag: str = "", detail: str = "") -> None:
        rows.append({"category": category, "metric": metric,
                     "value": str(value) if value is not None else "",
                     "flag": flag, "detail": detail})

    # ── Server profile ────────────────────────────────────────────────────────
    add("Server Profile", "Server",               f"{server}:{port}")
    add("Server Profile", "Version",              f"SQL Server {ctx.year} ({ctx.version_str})")
    add("Server Profile", "Edition",              ctx.edition)
    add("Server Profile", "SQL Agent available",  "Yes" if ctx.has_agent() else "No (Express)",
        "" if ctx.has_agent() else "⚠️")
    add("Server Profile", "Enterprise features",  "Yes" if ctx.is_enterprise else "No")
    add("Server Profile", "Report generated",     _now())

    # ── Database inventory ────────────────────────────────────────────────────
    dbs = read_csv(f"{out_dir}/01_databases.csv")
    total_size = sum(float(r.get("size_mb", 0) or 0) for r in dbs if "_error" not in r)
    add("Databases", "Total user databases", len(dbs))
    add("Databases", "Total size (MB)",      f"{total_size:,.0f}")
    for r in dbs:
        if "_error" not in r:
            add("Databases", f"  {r.get('name','')}",
                f"{r.get('size_mb','?')} MB",
                detail=f"recovery={r.get('recovery_model_desc','')}  "
                       f"compat={r.get('compatibility_level','')}")

    # ── Schema – aggregate across all databases ───────────────────────────────
    db_names    = [r["name"] for r in dbs if "name" in r and "_error" not in r]
    all_tables  = []
    all_flags   = []
    for db in db_names:
        slug = safe_slug(db)
        all_tables.extend(read_csv(f"{out_dir}/02_schema_{slug}_tables.csv"))
        all_flags.extend(read_csv(f"{out_dir}/02_schema_{slug}_flags.csv"))

    add("Schema", "Total tables (all DBs)", len([r for r in all_tables if "_error" not in r]))
    add("Schema", "Total rows (all DBs)",   f"{isum(all_tables, 'row_count'):,}")

    for fr in all_flags:
        if "_error" in fr: continue
        try:
            cnt = int(fr.get("count", 0) or 0)
        except (ValueError, TypeError):
            cnt = 0
        if cnt > 0:
            name = fr.get("flag", "")
            sev  = "⚠️" if any(w in name.lower() for w in ["deprecated","clr"]) else "ℹ️"
            add("Schema Flags", name, cnt, sev, f"database={fr.get('database','')}")

    # ── Native features – per database ───────────────────────────────────────
    for db in db_names:
        slug      = safe_slug(db)
        feat_rows = read_csv(f"{out_dir}/03_features_{slug}.csv")

        def rows_for(feat):
            return [r for r in feat_rows if r.get("_feature") == feat]

        def na(feat):
            rs = rows_for(feat)
            return rs and any("_not_applicable" in r for r in rs)

        def active_int(feat, col):
            return isum(rows_for(feat), col)

        def active_flag(feat, col, active_vals=("1","true","yes")):
            return any(str(r.get(col,"")).strip().lower() in active_vals
                       for r in rows_for(feat))

        # Service Broker
        broker_on = active_flag("service_broker", "broker_enabled")
        queues    = active_int("service_broker",  "queues")
        add("Features", f"[{db}] Service Broker",
            "ACTIVE" if broker_on else "NOT IN USE",
            "🔔 SPECIAL CUTOVER REQUIRED" if broker_on else "",
            f"queues={queues}" if broker_on else "")

        # CDC
        if na("cdc"):
            add("Features", f"[{db}] CDC", "N/A – requires Enterprise or 2016+")
        else:
            cdc_on = active_flag("cdc","db_cdc_enabled"); ct = active_int("cdc","cdc_tracked_tables")
            add("Features", f"[{db}] CDC", "ACTIVE" if cdc_on else "NOT IN USE",
                "🔔 SPECIAL CUTOVER REQUIRED" if cdc_on else "",
                f"tracked_tables={ct}" if cdc_on else "")

        # Change Tracking
        ct_on = active_flag("change_tracking","ct_enabled")
        ct_t  = active_int("change_tracking","ct_tracked_tables")
        add("Features", f"[{db}] Change Tracking", "ACTIVE" if ct_on else "NOT IN USE",
            "🔔 SPECIAL CUTOVER REQUIRED" if ct_on else "",
            f"tracked_tables={ct_t}" if ct_on else "")

        # Replication
        repl = active_int("replication","replicated_tables")
        add("Features", f"[{db}] Replication", "ACTIVE" if repl > 0 else "NOT IN USE",
            "🔔 SPECIAL CUTOVER REQUIRED" if repl > 0 else "",
            f"replicated_tables={repl}" if repl > 0 else "")

        # Always On
        if na("always_on"):
            add("Features", f"[{db}] Always On", "N/A – requires Enterprise or 2016+")
        else:
            hadr = active_flag("always_on","is_hadr_enabled")
            ag   = col_val(rows_for("always_on"),"ag_name")
            add("Features", f"[{db}] Always On",
                "ACTIVE" if hadr else "NOT IN USE",
                "🔔 SPECIAL CUTOVER REQUIRED" if hadr else "",
                f"ag={ag}" if hadr else "")

        # Mirroring
        mir_rs = rows_for("mirroring")
        mir_on = any(r.get("mirroring_state_desc","") not in ("","None","DISCONNECTED")
                     for r in mir_rs if "_error" not in r)
        add("Features", f"[{db}] Mirroring", "ACTIVE" if mir_on else "NOT IN USE",
            "🔔 SPECIAL CUTOVER REQUIRED" if mir_on else "")

        # Full-Text Search
        fts_on  = active_flag("full_text_search","fts_enabled")
        fts_idx = active_int("full_text_search","ft_indexes")
        add("Features", f"[{db}] Full-Text Search",
            "ACTIVE" if fts_on else "NOT IN USE",
            detail=f"indexes={fts_idx}" if fts_on else "")

        # In-Memory OLTP
        if na("in_memory_oltp"):
            add("Features", f"[{db}] In-Memory OLTP", "N/A – requires Enterprise or 2016+")
        else:
            im = active_int("in_memory_oltp","mem_optimized_tables")
            cp = active_int("in_memory_oltp","natively_compiled_procs")
            add("Features", f"[{db}] In-Memory OLTP",
                "ACTIVE" if im > 0 else "NOT IN USE",
                "🔍 MANUAL REVIEW REQUIRED" if im > 0 else "",
                f"tables={im}  compiled_procs={cp}")

        # FILESTREAM
        fsc = active_int("filestream","filestream_columns")
        fft = active_int("filestream","filetables")
        add("Features", f"[{db}] FILESTREAM / FileTable",
            "ACTIVE" if (fsc+fft)>0 else "NOT IN USE",
            "🔍 MANUAL REVIEW REQUIRED" if (fsc+fft)>0 else "",
            f"filestream_cols={fsc}  filetables={fft}")

        # Partitioning
        if na("partitioning"):
            add("Features", f"[{db}] Partitioning", "N/A – requires Enterprise or 2016+")
        else:
            pt = active_int("partitioning","partitioned_objects")
            add("Features", f"[{db}] Partitioning",
                "ACTIVE" if pt>0 else "NOT IN USE", detail=f"partitioned_objects={pt}")

        # Temporal
        if na("temporal_tables"):
            add("Features", f"[{db}] Temporal Tables", "N/A – requires SQL 2016+")
        else:
            tmp = len([r for r in rows_for("temporal_tables") if r.get("table_name")])
            add("Features", f"[{db}] Temporal Tables",
                "ACTIVE" if tmp>0 else "NOT IN USE", detail=f"count={tmp}")

        # Columnstore
        if na("columnstore"):
            add("Features", f"[{db}] Columnstore Indexes", "N/A – requires Enterprise or 2016+")
        else:
            cs = len([r for r in rows_for("columnstore") if r.get("table_name")])
            add("Features", f"[{db}] Columnstore Indexes",
                "ACTIVE" if cs>0 else "NOT IN USE", detail=f"count={cs}")

        # Graph / Ledger
        if na("graph_tables"):
            add("Features", f"[{db}] Graph Tables", "N/A – requires SQL 2017+")
        else:
            gr = len([r for r in rows_for("graph_tables") if r.get("name")])
            add("Features", f"[{db}] Graph Tables", "ACTIVE" if gr>0 else "NOT IN USE",
                detail=f"count={gr}")

        if na("ledger_tables"):
            add("Features", f"[{db}] Ledger Tables", "N/A – requires SQL 2022+")
        else:
            lg = len([r for r in rows_for("ledger_tables") if r.get("name")])
            add("Features", f"[{db}] Ledger Tables", "ACTIVE" if lg>0 else "NOT IN USE",
                detail=f"count={lg}")

        # Security
        tde_on = active_flag("tde","is_encrypted")
        add("Security", f"[{db}] TDE",
            "ENABLED" if tde_on else "NOT ENABLED",
            "🔑 KEY MIGRATION REQUIRED" if tde_on else "",
            f"state={col_val(rows_for('tde'),'encryption_state_desc')}" if tde_on else "")

        if na("always_encrypted"):
            add("Security", f"[{db}] Always Encrypted", "N/A – requires SQL 2016+")
        else:
            ae = active_int("always_encrypted","encrypted_columns")
            add("Security", f"[{db}] Always Encrypted",
                "ACTIVE" if ae>0 else "NOT IN USE",
                "🔑 KEY MIGRATION REQUIRED" if ae>0 else "",
                f"encrypted_columns={ae}")

        if na("rls"):
            add("Security", f"[{db}] Row-Level Security", "N/A – requires SQL 2016+")
        else:
            rls = len([r for r in rows_for("rls") if r.get("policy_name")])
            add("Security", f"[{db}] RLS Policies",
                "ACTIVE" if rls>0 else "NOT IN USE", detail=f"policies={rls}")

        if na("ddm"):
            add("Security", f"[{db}] Dynamic Data Masking", "N/A – requires SQL 2016+")
        else:
            ddm = len([r for r in rows_for("ddm") if r.get("column_name")])
            add("Security", f"[{db}] DDM Masked Columns",
                "ACTIVE" if ddm>0 else "NOT IN USE", detail=f"masked_columns={ddm}")

        # Programmability
        clr = len([r for r in rows_for("clr") if r.get("name") and "_error" not in r])
        add("Programmability", f"[{db}] CLR Assemblies", clr if clr>0 else "None",
            "🔍 MANUAL REVIEW REQUIRED" if clr>0 else "")

        udt = len([r for r in rows_for("udts") if r.get("type_name")])
        add("Programmability", f"[{db}] User-Defined Types", udt)

        add("Programmability", f"[{db}] XML Columns",     active_int("xml","xml_columns"))
        add("Programmability", f"[{db}] Spatial Columns", active_int("spatial","spatial_columns"))

        seqs = len([r for r in rows_for("sequences") if r.get("name")])
        add("Programmability", f"[{db}] Sequences", seqs)

        syns = len([r for r in rows_for("synonyms") if r.get("synonym_name")])
        add("Programmability", f"[{db}] Synonyms", syns)

        # Risks – BLOCKERS
        orph = [r for r in rows_for("orphaned_users")
                if r.get("database_user") and "_error" not in r]
        add("Risks", f"[{db}] Orphaned Users", len(orph),
            "❌ HIGH PRIORITY BLOCKER" if orph else "✓",
            ", ".join(r["database_user"] for r in orph))

        pkmiss = [r for r in rows_for("tables_missing_pk")
                  if r.get("table_name") and "_error" not in r]
        add("Risks", f"[{db}] Tables Missing PKs", len(pkmiss),
            "⚠️ RISK" if pkmiss else "✓",
            ", ".join(f"{r.get('schema_name','')}.{r.get('table_name','')} "
                      f"({r.get('row_count','?')} rows)"
                      for r in pkmiss)[:200])

    # ── Procedural code ───────────────────────────────────────────────────────
    for db in db_names:
        slug     = safe_slug(db)
        ov_rows  = read_csv(f"{out_dir}/09_proc_overview_{slug}.csv")
        rk_rows  = read_csv(f"{out_dir}/09_proc_risks_{slug}.csv")
        clr_rows = read_csv(f"{out_dir}/09_clr_detail_{slug}.csv")

        if not ov_rows or "_error" in ov_rows[0]:
            continue

        procs = [r for r in ov_rows if r.get("object_type") == "SQL_STORED_PROCEDURE"]
        fns   = [r for r in ov_rows
                 if r.get("object_type") not in ("SQL_STORED_PROCEDURE","CLR_STORED_PROCEDURE","")]

        add("Procedural Code", f"[{db}] Total stored procedures", len(procs))
        add("Procedural Code", f"[{db}] Total functions",         len(fns))
        add("Procedural Code", f"[{db}] Total CLR objects",       len(clr_rows))

        if ov_rows:
            def _safe_int(v):
                try: return int(v or 0)
                except (ValueError, TypeError): return 0
            biggest = max(ov_rows, key=lambda r: _safe_int(r.get("line_count")), default=None)
            if biggest:
                add("Procedural Code", f"[{db}] Largest object",
                    biggest.get("object_name",""),
                    detail=f"{biggest.get('line_count',0)} lines  "
                           f"type={biggest.get('object_type','')}")

        risk_counts = Counter(r.get("risk_type","") for r in rk_rows)
        critical    = [r for r in rk_rows if r.get("severity","").startswith("🔴")]
        high_risk   = [r for r in rk_rows if r.get("severity","").startswith("🟠")]

        add("Procedural Code", f"[{db}] Total risk findings", len(rk_rows),
            "⚠️" if rk_rows else "✓")
        add("Procedural Code", f"[{db}] Critical risk objects",
            len(set(r.get("object_name","") for r in critical)),
            "❌ CRITICAL" if critical else "✓",
            ", ".join(set(r.get("object_name","") for r in critical))[:150])
        add("Procedural Code", f"[{db}] High risk objects",
            len(set(r.get("object_name","") for r in high_risk)),
            "⚠️ HIGH" if high_risk else "✓")

        unsafe_clr = [r for r in clr_rows
                      if r.get("permission_set_desc","").upper() == "UNSAFE_ACCESS"]
        ext_clr    = [r for r in clr_rows
                      if r.get("permission_set_desc","").upper() == "EXTERNAL_ACCESS"]
        if unsafe_clr:
            add("Procedural Code", f"[{db}] CLR UNSAFE assemblies",
                len(unsafe_clr),
                "🔍 MANUAL REVIEW REQUIRED",
                ", ".join(r.get("assembly_name","") for r in unsafe_clr))
        if ext_clr:
            add("Procedural Code", f"[{db}] CLR EXTERNAL_ACCESS assemblies",
                len(ext_clr), "🔍 MANUAL REVIEW REQUIRED",
                ", ".join(r.get("assembly_name","") for r in ext_clr))

        for rtype, cnt in sorted(risk_counts.items()):
            if cnt > 0 and rtype:
                add("Procedural Risks", f"[{db}] {rtype}", cnt)

    # ── Tier-2 deep analysis (Claude-powered) ─────────────────────────────────
    for db in db_names:
        slug  = safe_slug(db)
        deep  = read_csv(f"{out_dir}/09_proc_deep_{slug}.csv")
        if not deep or "_error" in deep[0]:
            continue

        total_deep    = len(deep)
        needs_rewrite = [r for r in deep if str(r.get("migrate_as_is","")).lower() == "false"]
        very_complex  = [r for r in deep if r.get("migration_complexity","") == "Very Complex"]
        complex_      = [r for r in deep if r.get("migration_complexity","") == "Complex"]

        add("Deep Analysis", f"[{db}] Objects deep-analysed", total_deep)
        add("Deep Analysis", f"[{db}] Require rewrite",
            len(needs_rewrite),
            "⚠️ REWRITE REQUIRED" if needs_rewrite else "✓",
            ", ".join(r.get("object_name","") for r in needs_rewrite[:10]))
        add("Deep Analysis", f"[{db}] Very Complex objects",
            len(very_complex),
            "⚠️" if very_complex else "",
            ", ".join(r.get("object_name","") for r in very_complex[:8]))
        add("Deep Analysis", f"[{db}] Complex objects",
            len(complex_),
            "ℹ️" if complex_ else "")

        total_hours = sum(
            float(r.get("estimated_rewrite_hours") or 0)
            for r in deep
            if r.get("estimated_rewrite_hours") not in (None, "", "null")
        )
        if total_hours > 0:
            add("Deep Analysis", f"[{db}] Estimated rewrite hours (total)", f"{total_hours:.0f}h")

        for level in ("Very Complex", "Complex", "Moderate", "Simple"):
            cnt = sum(1 for r in deep if r.get("migration_complexity","") == level)
            if cnt > 0:
                add("Deep Analysis", f"[{db}] Complexity: {level}", cnt)

    # ── SQL Agent ─────────────────────────────────────────────────────────────
    job_rows  = read_csv(f"{out_dir}/04_jobs.csv")
    step_rows = read_csv(f"{out_dir}/04_job_steps.csv")
    if job_rows and "_not_applicable" not in job_rows[0] and "_error" not in job_rows[0]:
        enabled  = len([r for r in job_rows if str(r.get("enabled",""))=="1"])
        failed   = len([r for r in job_rows if str(r.get("last_run_status",""))=="0"])
        subs     = sorted({r.get("subsystem","") for r in step_rows if r.get("subsystem")})
        add("SQL Agent", "Total jobs",       len(job_rows))
        add("SQL Agent", "Enabled jobs",     enabled)
        add("SQL Agent", "Jobs last failed", failed, "⚠️" if failed>0 else "✓")
        add("SQL Agent", "Subsystems used",  ", ".join(subs))
    else:
        add("SQL Agent", "Status", "Not available (Express edition)", "ℹ️")

    # ── Linked servers ────────────────────────────────────────────────────────
    ls_rows = read_csv(f"{out_dir}/07_linked_servers.csv")
    lu_rows = read_csv(f"{out_dir}/07_linked_server_usage.csv")
    lsc = len([r for r in ls_rows if "_error" not in r])
    luc = len([r for r in lu_rows if "_error" not in r])
    add("Linked Servers", "Configured",               lsc, "⚠️" if lsc>0 else "✓")
    add("Linked Servers", "Objects referencing them", luc, "⚠️" if luc>0 else "✓")
    for r in ls_rows:
        if "_error" not in r:
            add("Linked Servers", f"  {r.get('linked_server_name','')}",
                r.get("data_source",""),
                detail=f"provider={r.get('provider','')}")

    # ── Backups ───────────────────────────────────────────────────────────────
    bk_rows = read_csv(f"{out_dir}/06_backups.csv")
    if bk_rows and "_not_applicable" not in bk_rows[0]:
        for r in bk_rows:
            if "_error" in r: continue
            db_n  = r.get("database_name","")
            full  = r.get("last_full","") or "Never"
            log_  = r.get("last_log","")  or "Never"
            add("Backups", f"[{db_n}] Last FULL", full,
                "⚠️ NO FULL BACKUP IN 30 DAYS" if full=="Never" else "✓")
            add("Backups", f"[{db_n}] Last LOG",  log_)
    else:
        add("Backups", "Status", "Not tracked in msdb on Express", "ℹ️")

    # ── Performance ───────────────────────────────────────────────────────────
    wr = read_csv(f"{out_dir}/05_perf_waits.csv")
    if wr:
        add("Performance", "Top wait type", col_val(wr,"wait_type"),
            detail=f"{col_val(wr,'pct_total')}% of total wait time")
    mi = read_csv(f"{out_dir}/05_perf_missing_idx.csv")
    add("Performance", "Missing index recommendations", len(mi),
        "ℹ️" if mi else "")
    mr = read_csv(f"{out_dir}/05_perf_memory.csv")
    if mr:
        add("Performance", "Memory used (MB)",        col_val(mr,"memory_used_mb"))
        add("Performance", "Memory utilisation %",    col_val(mr,"memory_utilization_percentage"))

    # ── Database Mail ─────────────────────────────────────────────────────────
    dm_rows = read_csv(f"{out_dir}/08_database_mail.csv")
    if dm_rows and "_not_applicable" not in dm_rows[0]:
        add("Database Mail", "Profiles configured", len(dm_rows))
    else:
        add("Database Mail", "Status", "Not available (Express edition)" if ctx.is_express
            else "Not configured")

    # ── write ─────────────────────────────────────────────────────────────────
    out_path = os.path.join(out_dir, "00_server_summary.csv")
    if not rows:
        rows = [{"category":"","metric":"No data","value":"","flag":"","detail":""}]
    keys = ["category","metric","value","flag","detail"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  [summary] {os.path.basename(out_path)}  ({len(rows)} rows)")
    return out_path
