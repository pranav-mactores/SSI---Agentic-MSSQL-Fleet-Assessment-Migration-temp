"""Auto-split from sql_migration_agent.py"""
from typing import Any
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

def tool_analyze_sql_features(ctx: ServerContext, database: str, out_dir: str) -> dict:
    slug = safe_slug(database)
    result: dict[str, Any] = {"database": database}
    csv_rows: list[dict] = []

    def record(feature: str, data: list[dict]) -> list[dict]:
        # Long/key-value format: 5 fixed columns regardless of feature.
        # Avoids a 150-column header when all features are merged into one CSV.
        for row_num, row in enumerate(data, 1):
            for key, value in row.items():
                if value is True:   value = "true"
                elif value is False: value = "false"
                elif value is None:  value = ""
                csv_rows.append({
                    "_feature":  feature,
                    "_database": database,
                    "_row":      row_num,
                    "key":       key,
                    "value":     str(value),
                })
        return data

    def qdb(sql: str) -> list[dict]:
        return rq(ctx, sql, database)

    def qm(sql: str) -> list[dict]:
        return rq(ctx, sql)

    # ── Service Broker ────────────────────────────────────────────────────────
    # is_ms_shipped exists on sys.service_queues in SQL <=2022; removed in 2025
    _sq_filter = "is_ms_shipped = 0" if ctx.version_int < 17 else "1=1"
    result["service_broker"] = record("service_broker", qdb(f"""
        SELECT
          (SELECT is_broker_enabled FROM sys.databases WHERE name=DB_NAME()) AS broker_enabled,
          (SELECT COUNT(*) FROM sys.service_queues    WHERE {_sq_filter}) AS queues,
          (SELECT COUNT(*) FROM sys.services)          AS services,
          (SELECT COUNT(*) FROM sys.service_contracts) AS contracts,
          (SELECT COUNT(*) FROM sys.routes)            AS routes;
    """))
    result["service_broker_queues"] = record("service_broker_queues", qdb(f"""
        SELECT q.name, q.is_activation_enabled, q.is_poison_message_handling_enabled,
               q.max_readers, s.name AS service_name
        FROM sys.service_queues q
        LEFT JOIN sys.services s ON q.object_id = s.service_queue_id
        WHERE {_sq_filter};
    """))

    # ── CDC  (Enterprise 2012+; Standard/Web 2016+; never Express) ──────────
    if not ctx.is_express and ctx.v(12) and (ctx.is_enterprise or ctx.v(13)):
        cdc_info = qdb("""
            SELECT
              (SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()) AS db_cdc_enabled,
              (SELECT COUNT(*) FROM sys.tables WHERE is_tracked_by_cdc=1)     AS cdc_tracked_tables;
        """)
        result["cdc"] = record("cdc", cdc_info)
        # cdc.change_tables only exists when CDC is enabled on this specific database
        cdc_on = (cdc_info and not cdc_info[0].get("_error")
                  and cdc_info[0].get("db_cdc_enabled"))
        if cdc_on:
            result["cdc_tables"] = record("cdc_tables", qdb("""
                SELECT capture_instance,
                       OBJECT_SCHEMA_NAME(source_object_id) AS source_schema,
                       OBJECT_NAME(source_object_id)        AS source_table,
                       supports_net_changes
                FROM cdc.change_tables;
            """))
        else:
            result["cdc_tables"] = record("cdc_tables",
                na_row("CDC not enabled on this database"))
    else:
        result["cdc"] = record("cdc", na_row(f"CDC not available on {ctx.edition}"))
        result["cdc_tables"] = record("cdc_tables", na_row(f"CDC not available on {ctx.edition}"))

    # ── Change Tracking  (2008+, all editions) ────────────────────────────────
    result["change_tracking"] = record("change_tracking", qdb("""
        SELECT
          CASE WHEN EXISTS (SELECT 1 FROM sys.change_tracking_databases
                            WHERE database_id=DB_ID()) THEN 1 ELSE 0 END AS ct_enabled,
          (SELECT retention_period FROM sys.change_tracking_databases
           WHERE database_id=DB_ID()) AS retention_period,
          (SELECT retention_period_units_desc FROM sys.change_tracking_databases
           WHERE database_id=DB_ID()) AS retention_units,
          (SELECT COUNT(*) FROM sys.change_tracking_tables) AS ct_tracked_tables;
    """))

    # ── Replication ───────────────────────────────────────────────────────────
    result["replication"] = record("replication", qdb("""
        SELECT (SELECT COUNT(*) FROM sys.tables WHERE is_replicated=1) AS replicated_tables,
               DB_NAME() AS database_name;
    """))

    # ── Always On  (Enterprise 2012+; Standard Basic AG 2016+; never Express) ─
    if not ctx.is_express and ctx.v(12) and (ctx.is_enterprise or ctx.v(13)):
        result["always_on"] = record("always_on", qdb("""
            SELECT ag.name          AS ag_name,
                   ar.replica_server_name,
                   ar.availability_mode_desc,
                   ar.failover_mode_desc,
                   agl.dns_name    AS listener_name,
                   1               AS is_hadr_enabled
            FROM sys.availability_databases_cluster   adc
            JOIN  sys.availability_groups          ag  ON adc.group_id = ag.group_id
            JOIN  sys.availability_replicas        ar  ON ag.group_id  = ar.group_id
            LEFT JOIN sys.availability_group_listeners agl ON ag.group_id = agl.group_id
            WHERE adc.database_name = DB_NAME()
              AND ar.replica_server_name = @@SERVERNAME;
        """))
    else:
        result["always_on"] = record("always_on", na_row(f"Always On not available on {ctx.edition}"))

    # ── Mirroring ─────────────────────────────────────────────────────────────
    result["mirroring"] = record("mirroring", qdb("""
        SELECT mirroring_state_desc, mirroring_role_desc,
               mirroring_partner_name, mirroring_safety_level_desc
        FROM sys.database_mirroring
        WHERE database_id=DB_ID() AND mirroring_guid IS NOT NULL;
    """))

    # ── Full-Text Search ──────────────────────────────────────────────────────
    result["full_text_search"] = record("full_text_search", qdb("""
        SELECT
          (SELECT is_fulltext_enabled FROM sys.databases WHERE name=DB_NAME()) AS fts_enabled,
          (SELECT COUNT(*) FROM sys.fulltext_catalogs)       AS ft_catalogs,
          (SELECT COUNT(*) FROM sys.fulltext_indexes)        AS ft_indexes;
    """))

    # ── In-Memory OLTP  (Enterprise 2014+; all editions 2016+) ───────────────
    if ctx.v(12) and (ctx.is_enterprise or ctx.v(13)):
        result["in_memory_oltp"] = record("in_memory_oltp", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.tables WHERE is_memory_optimized=1) AS mem_optimized_tables,
              (SELECT COUNT(*) FROM sys.procedures p
               JOIN sys.sql_modules m ON p.object_id=m.object_id
               WHERE m.uses_native_compilation=1) AS natively_compiled_procs;
        """))
        result["in_memory_tables"] = record("in_memory_tables", qdb("""
            SELECT t.name AS table_name, t.durability_desc, COUNT(c.column_id) AS column_count
            FROM sys.tables t JOIN sys.columns c ON t.object_id=c.object_id
            WHERE t.is_memory_optimized=1 GROUP BY t.name, t.durability_desc;
        """))
    else:
        result["in_memory_oltp"] = record("in_memory_oltp", na_row(f"In-Memory OLTP not available on {ctx.edition}"))

    # ── FILESTREAM / FileTable ────────────────────────────────────────────────
    result["filestream"] = record("filestream", qdb("""
        SELECT
          (SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
           WHERE t.name='varbinary' AND c.is_filestream=1) AS filestream_columns,
          (SELECT COUNT(*) FROM sys.tables WHERE is_filetable=1) AS filetables,
          (SELECT COUNT(*) FROM sys.data_spaces WHERE type='FD')  AS filestream_filegroups;
    """))

    # ── Partitioning  (Enterprise pre-2016; all editions 2016+) ──────────────
    if ctx.is_enterprise or ctx.v(13):
        result["partitioning"] = record("partitioning", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.partition_functions) AS partition_functions,
              (SELECT COUNT(*) FROM sys.partition_schemes)   AS partition_schemes,
              (SELECT COUNT(DISTINCT object_id) FROM sys.partitions
               WHERE partition_number > 1) AS partitioned_objects;
        """))
        result["partition_detail"] = record("partition_detail", qdb("""
            SELECT pf.name AS partition_function, pf.fanout AS partitions,
                   ps.name AS partition_scheme, t.name AS table_name, c.name AS partition_col
            FROM sys.partition_functions pf
            JOIN sys.partition_schemes ps ON pf.function_id   = ps.function_id
            JOIN sys.indexes           i  ON ps.data_space_id = i.data_space_id
            JOIN sys.tables            t  ON i.object_id      = t.object_id
            JOIN sys.index_columns     ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
                                        AND ic.partition_ordinal>0
            JOIN sys.columns           c  ON ic.object_id=c.object_id AND ic.column_id=c.column_id
            ORDER BY t.name;
        """))
    else:
        result["partitioning"] = record("partitioning", na_row("Partitioning requires Enterprise or 2016+"))

    # ── Temporal Tables  (2016+) ──────────────────────────────────────────────
    if ctx.v(13):
        result["temporal_tables"] = record("temporal_tables", qdb("""
            SELECT t.name AS table_name, t.temporal_type_desc, ht.name AS history_table,
                   cc.name AS period_start_col, cc2.name AS period_end_col
            FROM sys.tables t
            LEFT JOIN sys.tables  ht  ON t.history_table_id=ht.object_id
            LEFT JOIN sys.periods p   ON t.object_id=p.object_id
            LEFT JOIN sys.columns cc  ON p.start_column_id=cc.column_id  AND cc.object_id=t.object_id
            LEFT JOIN sys.columns cc2 ON p.end_column_id=cc2.column_id   AND cc2.object_id=t.object_id
            WHERE t.temporal_type IN (1,2) ORDER BY t.name;
        """))
    else:
        result["temporal_tables"] = record("temporal_tables", na_row("Temporal Tables require SQL 2016+"))

    # ── Columnstore  (Enterprise 2014+; all editions 2016+) ──────────────────
    if ctx.is_enterprise or ctx.v(13):
        result["columnstore"] = record("columnstore", qdb("""
            SELECT t.name AS table_name, i.name AS index_name, i.type_desc,
                   COUNT(ic.column_id) AS column_count
            FROM sys.indexes i
            JOIN sys.tables        t  ON i.object_id=t.object_id
            JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
            WHERE i.type IN (5,6) GROUP BY t.name, i.name, i.type_desc ORDER BY t.name;
        """))
    else:
        result["columnstore"] = record("columnstore", na_row("Columnstore requires Enterprise or 2016+"))

    # ── Graph Tables  (2017+) ─────────────────────────────────────────────────
    if ctx.v(14):
        result["graph_tables"] = record("graph_tables", qdb(
            "SELECT name, is_node, is_edge FROM sys.tables WHERE is_node=1 OR is_edge=1 ORDER BY name;"))
    else:
        result["graph_tables"] = record("graph_tables", na_row("Graph Tables require SQL 2017+"))

    # ── Ledger Tables  (2022+) ────────────────────────────────────────────────
    if ctx.v(16):
        result["ledger_tables"] = record("ledger_tables", qdb(
            "SELECT name, ledger_type_desc, is_dropped_ledger_table FROM sys.tables WHERE ledger_type IN (1,2);"))
    else:
        result["ledger_tables"] = record("ledger_tables", na_row("Ledger Tables require SQL 2022+"))

    # ── Row-Level Security  (2016+) ───────────────────────────────────────────
    if ctx.v(13):
        result["rls"] = record("rls", qdb("""
            SELECT sp.name AS policy_name, sp.is_enabled,
                   OBJECT_NAME(spf.target_object_id) AS target_table,
                   spf.predicate_type_desc
            FROM sys.security_policies   sp
            JOIN sys.security_predicates spf ON sp.object_id=spf.object_id ORDER BY sp.name;
        """))
    else:
        result["rls"] = record("rls", na_row("RLS requires SQL 2016+"))

    # ── Dynamic Data Masking  (2016+) ─────────────────────────────────────────
    if ctx.v(13):
        result["ddm"] = record("ddm", qdb("""
            SELECT t.name AS table_name, c.name AS column_name,
                   c.masking_function, ty.name AS data_type
            FROM sys.masked_columns c
            JOIN sys.tables t ON c.object_id=t.object_id
            JOIN sys.types  ty ON c.user_type_id=ty.user_type_id ORDER BY t.name;
        """))
    else:
        result["ddm"] = record("ddm", na_row("DDM requires SQL 2016+"))

    # ── TDE ───────────────────────────────────────────────────────────────────
    # encryption_state_desc added in SQL 2019; derive it for older versions
    _enc_desc = ("de.encryption_state_desc" if ctx.v(15) else
                 "CASE de.encryption_state "
                 "WHEN 0 THEN 'NO_DATABASE_ENCRYPTION_KEY' "
                 "WHEN 1 THEN 'UNENCRYPTED' "
                 "WHEN 2 THEN 'ENCRYPTION_IN_PROGRESS' "
                 "WHEN 3 THEN 'ENCRYPTED' "
                 "WHEN 4 THEN 'KEY_CHANGE_IN_PROGRESS' "
                 "WHEN 5 THEN 'DECRYPTION_IN_PROGRESS' "
                 "ELSE CAST(de.encryption_state AS NVARCHAR(50)) END")
    result["tde"] = record("tde", qdb(f"""
        SELECT d.name, d.is_encrypted, {_enc_desc} AS encryption_state_desc,
               de.encryptor_type
        FROM sys.databases d
        LEFT JOIN sys.dm_database_encryption_keys de ON d.database_id=de.database_id
        WHERE d.name=DB_NAME();
    """))

    # ── Always Encrypted  (2016+) ─────────────────────────────────────────────
    if ctx.v(13):
        result["always_encrypted"] = record("always_encrypted", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.column_master_keys)     AS column_master_keys,
              (SELECT COUNT(*) FROM sys.column_encryption_keys) AS column_encryption_keys,
              (SELECT COUNT(*) FROM sys.columns WHERE encryption_type IS NOT NULL) AS encrypted_columns;
        """))
    else:
        result["always_encrypted"] = record("always_encrypted", na_row("Always Encrypted requires SQL 2016+"))

    # ── Audit ─────────────────────────────────────────────────────────────────
    result["audit"] = record("audit", qdb("""
        SELECT sa.audit_action_name, sa.class_desc AS class_type_desc,
               ISNULL(OBJECT_SCHEMA_NAME(sa.major_id),'') AS schema_name,
               ISNULL(OBJECT_NAME(sa.major_id),'')        AS object_name
        FROM sys.database_audit_specifications das
        JOIN sys.database_audit_specification_details sa
          ON das.database_specification_id=sa.database_specification_id
        ORDER BY sa.audit_action_name;
    """))

    # ── CLR Assemblies ────────────────────────────────────────────────────────
    result["clr"] = record("clr", qdb("""
        SELECT a.name, a.clr_name, a.permission_set_desc, a.is_visible,
               COUNT(ao.object_id) AS dependent_objects
        FROM sys.assemblies a
        LEFT JOIN sys.assembly_modules ao ON a.assembly_id=ao.assembly_id
        WHERE a.is_user_defined=1
        GROUP BY a.name, a.clr_name, a.permission_set_desc, a.is_visible ORDER BY a.name;
    """))

    # ── UDTs ──────────────────────────────────────────────────────────────────
    result["udts"] = record("udts", qdb("""
        SELECT t.name AS type_name, s.name AS schema_name, t.is_assembly_type,
               bt.name AS base_type, t.max_length, t.precision, t.scale,
               (SELECT COUNT(*) FROM sys.columns c WHERE c.user_type_id=t.user_type_id) AS usage_count
        FROM sys.types t
        JOIN sys.schemas s ON t.schema_id=s.schema_id
        LEFT JOIN sys.types bt ON t.system_type_id=bt.user_type_id AND bt.is_user_defined=0
        WHERE t.is_user_defined=1 ORDER BY usage_count DESC;
    """))

    # ── Sequences  (2012+) ────────────────────────────────────────────────────
    result["sequences"] = record("sequences", qdb("""
        SELECT s.name, sc.name AS schema_name, t.name AS data_type,
               s.start_value, s.increment, s.is_cycling, s.current_value
        FROM sys.sequences s
        JOIN sys.schemas sc ON s.schema_id=sc.schema_id
        JOIN sys.types   t  ON s.user_type_id=t.user_type_id ORDER BY sc.name, s.name;
    """))

    # ── Synonyms ──────────────────────────────────────────────────────────────
    result["synonyms"] = record("synonyms", qdb("""
        SELECT s.name AS synonym_name, sc.name AS schema_name, s.base_object_name
        FROM sys.synonyms s JOIN sys.schemas sc ON s.schema_id=sc.schema_id ORDER BY sc.name, s.name;
    """))

    # ── Certificates & Keys ───────────────────────────────────────────────────
    result["certs_keys"] = record("certs_keys", qdb("""
        SELECT
          (SELECT COUNT(*) FROM sys.certificates    WHERE is_active_for_begin_dialog=1) AS certificates,
          (SELECT COUNT(*) FROM sys.asymmetric_keys  WHERE principal_id IS NOT NULL)    AS asymmetric_keys,
          (SELECT COUNT(*) FROM sys.symmetric_keys   WHERE symmetric_key_id > 2)        AS symmetric_keys;
    """))

    # ── XML ───────────────────────────────────────────────────────────────────
    result["xml"] = record("xml", qdb("""
        SELECT
          (SELECT COUNT(*) FROM sys.xml_schema_collections WHERE schema_id>4) AS xml_schema_collections,
          (SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
           WHERE t.name='xml') AS xml_columns,
          (SELECT COUNT(*) FROM sys.xml_indexes) AS xml_indexes;
    """))

    # ── Spatial ───────────────────────────────────────────────────────────────
    result["spatial"] = record("spatial", qdb("""
        SELECT
          (SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id
           WHERE t.name IN ('geometry','geography')) AS spatial_columns,
          (SELECT COUNT(*) FROM sys.spatial_indexes) AS spatial_indexes;
    """))

    # ── Orphaned Users ────────────────────────────────────────────────────────
    result["orphaned_users"] = record("orphaned_users", qdb("""
        SELECT dp.name AS database_user, dp.type_desc, dp.create_date
        FROM sys.database_principals dp
        WHERE dp.type IN ('S','U','G') AND dp.sid IS NOT NULL AND dp.sid<>0x01
          AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
          AND NOT EXISTS (SELECT 1 FROM sys.server_principals sp WHERE sp.sid=dp.sid)
        ORDER BY dp.name;
    """))

    # ── Tables Missing PKs ────────────────────────────────────────────────────
    result["tables_missing_pk"] = record("tables_missing_pk", qdb("""
        SELECT s.name AS schema_name, t.name AS table_name, SUM(p.rows) AS row_count
        FROM sys.tables t
        JOIN sys.schemas    s ON t.schema_id=s.schema_id
        JOIN sys.partitions p ON t.object_id=p.object_id AND p.index_id IN (0,1)
        WHERE t.object_id NOT IN (
            SELECT parent_object_id FROM sys.key_constraints WHERE type='PK'
        )
        GROUP BY s.name, t.name HAVING SUM(p.rows)>0 ORDER BY row_count DESC;
    """))

    # ── Resource Governor  (Enterprise only) ─────────────────────────────────
    if ctx.is_enterprise:
        result["resource_governor"] = record("resource_governor", qm("""
            SELECT (SELECT is_enabled FROM sys.resource_governor_configuration) AS rg_enabled,
                   (SELECT COUNT(*) FROM sys.resource_governor_resource_pools
                    WHERE name NOT IN ('internal','default')) AS custom_pools,
                   (SELECT COUNT(*) FROM sys.resource_governor_workload_groups
                    WHERE name NOT IN ('internal','default')) AS workload_groups;
        """))
    else:
        result["resource_governor"] = record("resource_governor",
            na_row("Resource Governor requires Enterprise edition"))

    # ── Extended Events ───────────────────────────────────────────────────────
    result["extended_events"] = record("extended_events", qm("""
        SELECT name, create_time, buffer_policy_desc
        FROM sys.dm_xe_sessions WHERE name NOT LIKE 'telemetry_%' ORDER BY name;
    """))

    # ── Server Triggers ───────────────────────────────────────────────────────
    result["server_triggers"] = record("server_triggers", qm(
        "SELECT name, type_desc, is_disabled, parent_class_desc FROM sys.server_triggers ORDER BY name;"))

    # ── DB Permissions ────────────────────────────────────────────────────────
    result["db_permissions"] = record("db_permissions", qdb("""
        SELECT dp.class_desc, dp.permission_name, dp.state_desc, pr.name AS grantee,
               COALESCE(OBJECT_NAME(dp.major_id),'') AS object_name
        FROM sys.database_permissions dp
        JOIN sys.database_principals pr ON dp.grantee_principal_id=pr.principal_id
        WHERE dp.class IN (0,1)
          AND pr.name NOT IN ('dbo','public','guest','INFORMATION_SCHEMA','sys')
        ORDER BY pr.name, dp.permission_name;
    """))


    # ── Log Shipping  (all editions) ─────────────────────────────────────────
    result["log_shipping"] = record("log_shipping", qm("""
        SELECT
          (SELECT COUNT(*) FROM msdb.dbo.log_shipping_primary_databases) AS primary_dbs,
          (SELECT COUNT(*) FROM msdb.dbo.log_shipping_secondary_databases) AS secondary_dbs,
          (SELECT COUNT(*) FROM msdb.dbo.log_shipping_monitor_primary) AS monitor_entries;
    """))
    result["log_shipping_detail"] = record("log_shipping_detail", qm("""
        SELECT p.primary_database, p.backup_directory, p.backup_retention_period,
               ps.secondary_server, ps.secondary_database,
               s.restore_delay
        FROM msdb.dbo.log_shipping_primary_databases p
        LEFT JOIN msdb.dbo.log_shipping_primary_secondaries ps
               ON p.primary_id = ps.primary_id
        LEFT JOIN msdb.dbo.log_shipping_secondary_databases s
               ON ps.secondary_server = s.secondary_server
              AND ps.secondary_database = s.secondary_database
        ORDER BY p.primary_database;
    """))

    # ── Database Snapshots  (Enterprise) ─────────────────────────────────────
    result["db_snapshots"] = record("db_snapshots", qm("""
        SELECT name AS snapshot_name, source_database_id,
               (SELECT name FROM sys.databases WHERE database_id = d.source_database_id)
                   AS source_database,
               create_date, state_desc
        FROM sys.databases d
        WHERE source_database_id IS NOT NULL
        ORDER BY name;
    """))

    # ── Query Store  (2016+) ──────────────────────────────────────────────────
    if ctx.v(13):
        result["query_store"] = record("query_store", qdb("""
            SELECT actual_state_desc, desired_state_desc,
                   current_storage_size_mb, max_storage_size_mb,
                   flush_interval_seconds,
                   interval_length_minutes, stale_query_threshold_days,
                   size_based_cleanup_mode_desc, query_capture_mode_desc
            FROM sys.database_query_store_options;
        """))
    else:
        result["query_store"] = record("query_store", na_row("Query Store requires SQL 2016+"))

    # ── Accelerated Database Recovery  (2019+) ────────────────────────────────
    if ctx.v(15):
        result["adr"] = record("adr", qdb("""
            SELECT is_accelerated_database_recovery_on
            FROM sys.databases WHERE name = DB_NAME();
        """))
    else:
        result["adr"] = record("adr", na_row("ADR requires SQL 2019+"))

    # ── Stretch Database  (2016–2022; removed in SQL 2025) ───────────────────
    if ctx.v(13) and not ctx.v(17):
        result["stretch_db"] = record("stretch_db", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.tables
               WHERE is_remote_data_archive_enabled = 1) AS stretch_tables,
              (SELECT is_remote_data_archive_enabled
               FROM sys.databases WHERE name = DB_NAME())  AS db_stretch_enabled;
        """))
    else:
        result["stretch_db"] = record("stretch_db", na_row(
            "Stretch DB requires SQL 2016+ and was removed in SQL 2025"))

    # ── PolyBase / External tables  (2016+) ───────────────────────────────────
    if ctx.v(13):
        result["polybase"] = record("polybase", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.external_tables)       AS external_tables,
              (SELECT COUNT(*) FROM sys.external_data_sources) AS external_data_sources,
              (SELECT COUNT(*) FROM sys.external_file_formats) AS external_file_formats;
        """))
        # pushdown column added in SQL 2019; omit for SQL 2016/2017/2018
        _pb_pushdown = ", pushdown" if ctx.v(15) else ""
        result["polybase_sources"] = record("polybase_sources", qdb(f"""
            SELECT name, type_desc, location, credential_id{_pb_pushdown}
            FROM sys.external_data_sources ORDER BY name;
        """))
    else:
        result["polybase"] = record("polybase", na_row("PolyBase requires SQL 2016+"))

    # ── Contained Databases  (2012+) ──────────────────────────────────────────
    result["contained_db"] = record("contained_db", qdb("""
        SELECT containment, containment_desc
        FROM sys.databases WHERE name = DB_NAME();
    """))

    # ── Azure Synapse Link  (2022 only) ───────────────────────────────────────
    if ctx.v(16):
        result["synapse_link"] = record("synapse_link", qdb("""
            SELECT is_change_feed_enabled
            FROM sys.databases WHERE name = DB_NAME();
        """))
    else:
        result["synapse_link"] = record("synapse_link", na_row("Azure Synapse Link requires SQL 2022+"))

    # ── Contained Availability Groups  (2022+) ────────────────────────────────
    if ctx.v(16):
        result["contained_ag"] = record("contained_ag", qm("""
            SELECT name, is_contained, is_distributed
            FROM sys.availability_groups ORDER BY name;
        """))
    else:
        result["contained_ag"] = record("contained_ag", na_row("Contained AGs require SQL 2022+"))

    # ── Row / Page Compression ────────────────────────────────────────────────
    result["compression"] = record("compression", qdb("""
        SELECT
            t.name AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            p.data_compression_desc,
            SUM(p.rows) AS rows,
            COUNT(*) AS partition_count
        FROM sys.partitions p
        JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
        JOIN sys.tables  t ON i.object_id = t.object_id
        WHERE p.data_compression > 0
          AND t.is_ms_shipped = 0
        GROUP BY t.name, i.name, i.type_desc, p.data_compression_desc
        ORDER BY t.name, i.name;
    """))

    # ── Sparse Columns ────────────────────────────────────────────────────────
    result["sparse_columns"] = record("sparse_columns", qdb("""
        SELECT t.name AS table_name, c.name AS column_name,
               ty.name AS data_type, c.is_column_set
        FROM sys.columns c
        JOIN sys.tables t  ON c.object_id     = t.object_id
        JOIN sys.types  ty ON c.user_type_id  = ty.user_type_id
        WHERE c.is_sparse = 1 AND t.is_ms_shipped = 0
        ORDER BY t.name, c.name;
    """))

    # ── Persisted Computed Columns ────────────────────────────────────────────
    result["computed_columns"] = record("computed_columns", qdb("""
        SELECT t.name AS table_name, c.name AS column_name,
               c.definition, c.is_persisted, c.is_nullable,
               ty.name AS type_name
        FROM sys.computed_columns c
        JOIN sys.tables t  ON c.object_id    = t.object_id
        JOIN sys.types  ty ON c.user_type_id = ty.user_type_id
        WHERE t.is_ms_shipped = 0
        ORDER BY t.name, c.name;
    """))

    # ── Indexed Views ─────────────────────────────────────────────────────────
    result["indexed_views"] = record("indexed_views", qdb("""
        SELECT v.name AS view_name, s.name AS schema_name,
               i.name AS index_name, i.type_desc,
               i.is_unique, i.is_padded
        FROM sys.views   v
        JOIN sys.schemas s ON v.schema_id   = s.schema_id
        JOIN sys.indexes i ON v.object_id   = i.object_id
        WHERE i.index_id > 0
        ORDER BY v.name, i.index_id;
    """))

    # ── Filtered Indexes ──────────────────────────────────────────────────────
    result["filtered_indexes"] = record("filtered_indexes", qdb("""
        SELECT t.name AS table_name, i.name AS index_name,
               i.filter_definition, i.is_unique, i.type_desc
        FROM sys.indexes i
        JOIN sys.tables  t ON i.object_id = t.object_id
        WHERE i.has_filter = 1 AND t.is_ms_shipped = 0
        ORDER BY t.name, i.name;
    """))

    # ── Cross-Database Foreign Keys ───────────────────────────────────────────
    result["cross_db_fk"] = record("cross_db_fk", qdb("""
        SELECT
            OBJECT_SCHEMA_NAME(fk.parent_object_id)  AS fk_schema,
            OBJECT_NAME(fk.parent_object_id)         AS fk_table,
            fk.name                                  AS fk_name,
            fk.is_disabled, fk.is_not_trusted,
            m.definition                             AS sp_definition_hint
        FROM sys.foreign_keys fk
        LEFT JOIN sys.sql_modules m
               ON fk.referenced_object_id = m.object_id
        WHERE fk.is_disabled = 0
          AND (
              m.definition LIKE '%.%.%' -- crude cross-db hint in check
              OR fk.name LIKE '%cross%'
          );
    """))

    # ── DDL Triggers (database-level, distinct from server triggers) ──────────
    result["ddl_triggers_db"] = record("ddl_triggers_db", qdb("""
        SELECT t.name, t.type_desc, t.is_disabled, t.is_instead_of_trigger,
               m.definition
        FROM sys.triggers t
        JOIN sys.sql_modules m ON t.object_id = m.object_id
        WHERE t.parent_class = 0   -- 0 = database scope
          AND t.is_ms_shipped = 0
        ORDER BY t.name;
    """))

    # ── Database Collation ────────────────────────────────────────────────────
    result["db_collation"] = record("db_collation", qdb("""
        SELECT
            name AS database_name,
            collation_name,
            is_read_only,
            is_auto_close_on,
            is_auto_shrink_on,
            is_ansi_nulls_on,
            is_ansi_warnings_on,
            is_quoted_identifier_on,
            is_recursive_triggers_on,
            is_trustworthy_on,
            compatibility_level
        FROM sys.databases WHERE name = DB_NAME();
    """))

    # ── Column collation overrides (non-default) ──────────────────────────────
    result["column_collations"] = record("column_collations", qdb("""
        SELECT TOP 50
            t.name AS table_name, c.name AS column_name,
            c.collation_name,
            ty.name AS data_type
        FROM sys.columns c
        JOIN sys.tables t  ON c.object_id    = t.object_id
        JOIN sys.types  ty ON c.user_type_id = ty.user_type_id
        WHERE c.collation_name IS NOT NULL
          AND c.collation_name <> CAST(DATABASEPROPERTYEX(DB_NAME(),'Collation') AS NVARCHAR(128))
          AND t.is_ms_shipped = 0
        ORDER BY t.name, c.name;
    """))

    # ── XTP / In-Memory filegroup ─────────────────────────────────────────────
    result["xtp_filegroup"] = record("xtp_filegroup", qdb("""
        SELECT fg.name AS filegroup_name, fg.type_desc,
               df.name AS file_name, df.physical_name,
               CAST(df.size * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb
        FROM sys.filegroups fg
        LEFT JOIN sys.database_files df ON fg.data_space_id = df.data_space_id
        WHERE fg.type = 'FX'
        ORDER BY fg.name;
    """))

    # ── Server-level Audit ────────────────────────────────────────────────────
    result["server_audit"] = record("server_audit", qm("""
        SELECT a.name AS audit_name, a.audit_guid, a.type_desc,
               a.on_failure_desc, a.is_state_enabled,
               aspec.name AS spec_name, aspec.is_state_enabled AS spec_enabled
        FROM sys.server_audits a
        LEFT JOIN sys.server_audit_specifications aspec
               ON a.audit_guid = aspec.audit_guid
        ORDER BY a.name;
    """))

    # ── Backup Encryption ─────────────────────────────────────────────────────
    if ctx.has_agent():
        result["backup_encryption"] = record("backup_encryption", qm("""
            SELECT database_name, encryptor_type, encryptor_thumbprint,
                   key_algorithm,
                   MAX(backup_finish_date) AS last_encrypted_backup
            FROM msdb.dbo.backupset
            WHERE encryptor_thumbprint IS NOT NULL
            GROUP BY database_name, encryptor_type, encryptor_thumbprint,
                     key_algorithm
            ORDER BY database_name;
        """))
    else:
        result["backup_encryption"] = record("backup_encryption",
            na_row("Backup history not available on Express"))

    # ── Logins & server role members ──────────────────────────────────────────
    result["logins"] = record("logins", qm("""
        SELECT sp.name AS login_name, sp.type_desc, sp.is_disabled,
               sp.default_database_name,
               sl.is_policy_checked, sl.is_expiration_checked,
               STUFF((
                   SELECT ', ' + sr2.name
                   FROM sys.server_role_members srm2
                   JOIN sys.server_principals   sr2 ON srm2.role_principal_id = sr2.principal_id
                   WHERE srm2.member_principal_id = sp.principal_id
                   FOR XML PATH(''), TYPE).value('.','NVARCHAR(MAX)'), 1, 2, '') AS server_roles
        FROM sys.server_principals sp
        LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
        WHERE sp.type IN ('S','U','G')
          AND sp.name NOT LIKE '##%'
          AND sp.name NOT IN ('sa','BUILTIN\\Administrators')
        ORDER BY sp.name;
    """))

    # ── Semantic Search ───────────────────────────────────────────────────────
    result["semantic_search"] = record("semantic_search", qdb("""
        SELECT
          (SELECT COUNT(*) FROM sys.fulltext_index_columns
           WHERE statistical_semantics = 1) AS semantic_indexed_columns;
    """))

    # ── Event Notifications ───────────────────────────────────────────────────
    result["event_notifications"] = record("event_notifications", qdb("""
        SELECT en.name, en.object_id, en.parent_class_desc,
               en.service_name, en.broker_instance
        FROM sys.event_notifications en
        ORDER BY en.name;
    """))

    # ── Custom Error Messages ─────────────────────────────────────────────────
    result["custom_errors"] = record("custom_errors", qm("""
        SELECT message_id, language_id, severity, is_event_logged, text
        FROM sys.messages
        WHERE message_id >= 50000
          AND language_id = 1033
        ORDER BY message_id;
    """))

    # ── SQL Agent Proxies ─────────────────────────────────────────────────────
    if ctx.has_agent():
        result["agent_proxies"] = record("agent_proxies", qm("""
            SELECT p.name AS proxy_name, p.enabled, p.description,
                   c.name AS credential_name,
                   STUFF((
                       SELECT ', ' + ss2.subsystem
                       FROM msdb.dbo.sysproxysubsystem ps2
                       JOIN msdb.dbo.syssubsystems ss2 ON ps2.subsystem_id = ss2.subsystem_id
                       WHERE ps2.proxy_id = p.proxy_id
                       FOR XML PATH(''), TYPE).value('.','NVARCHAR(MAX)'), 1, 2, '') AS subsystems
            FROM msdb.dbo.sysproxies p
            LEFT JOIN sys.credentials c ON p.credential_id = c.credential_id
            ORDER BY p.name;
        """))
        result["agent_alerts"] = record("agent_alerts", qm("""
            SELECT a.name, a.enabled, a.message_id, a.severity,
                   a.database_name, a.event_description_keyword,
                   a.notification_message, j.name AS job_name
            FROM msdb.dbo.sysalerts a
            LEFT JOIN msdb.dbo.sysjobs j ON a.job_id = j.job_id
            ORDER BY a.name;
        """))
    else:
        result["agent_proxies"] = record("agent_proxies", na_row("Express edition"))
        result["agent_alerts"]  = record("agent_alerts",  na_row("Express edition"))

    # ── Policy-Based Management ───────────────────────────────────────────────
    result["pbm"] = record("pbm", qm("""
        SELECT
          (SELECT COUNT(*) FROM msdb.dbo.syspolicy_policies  WHERE is_enabled=1) AS enabled_policies,
          (SELECT COUNT(*) FROM msdb.dbo.syspolicy_conditions)                   AS conditions,
          (SELECT COUNT(*) FROM msdb.dbo.syspolicy_policy_categories)            AS categories;
    """))

    # ── SSIS Packages in msdb ─────────────────────────────────────────────────
    if ctx.has_agent():
        result["ssis_packages"] = record("ssis_packages", qm("""
            SELECT name, folderid, description, createdate, packageformat, packagetype
            FROM msdb.dbo.sysssispackages
            ORDER BY name;
        """))
    else:
        result["ssis_packages"] = record("ssis_packages", na_row("Express edition"))

    # ── Data-Tier Applications (DAC) ──────────────────────────────────────────
    result["dac_instances"] = record("dac_instances", qm("""
        SELECT instance_name, type_name, type_version,
               description, date_created
        FROM msdb.dbo.sysdac_instances
        ORDER BY instance_name;
    """))

    # ── Extended Events: defined sessions (not just active) ───────────────────
    result["xe_defined_sessions"] = record("xe_defined_sessions", qm("""
        SELECT s.name, s.startup_state, s.event_retention_mode_desc,
               s.max_dispatch_latency, COUNT(e.name) AS event_count
        FROM sys.server_event_sessions s
        LEFT JOIN sys.server_event_session_events e ON s.event_session_id = e.event_session_id
        WHERE s.name NOT LIKE 'AlwaysOn%'
          AND s.name NOT LIKE 'system_%'
        GROUP BY s.name, s.startup_state, s.event_retention_mode_desc,
                 s.max_dispatch_latency
        ORDER BY s.name;
    """))

    # ── Server Configuration ──────────────────────────────────────────────────
    result["server_config"] = record("server_config", qm("""
        SELECT name, CAST(value_in_use AS VARCHAR(50)) AS value_in_use,
               description
        FROM sys.configurations
        WHERE name IN (
            'max server memory (MB)',
            'min server memory (MB)',
            'max degree of parallelism',
            'cost threshold for parallelism',
            'clr enabled',
            'clr strict security',
            'xp_cmdshell',
            'Database Mail XPs',
            'Ole Automation Procedures',
            'Ad Hoc Distributed Queries',
            'remote access',
            'remote query timeout (s)',
            'contained database authentication',
            'optimize for ad hoc workloads',
            'backup compression default',
            'tempdb metadata memory-optimized'
        )
        ORDER BY name;
    """))

    # ── Replication detail  (extend original count-only) ─────────────────────
    result["replication_detail"] = record("replication_detail", qdb("""
        SELECT t.name AS table_name,
               CASE WHEN t.is_replicated = 1 THEN 'YES' ELSE 'NO' END AS is_replicated,
               CASE WHEN EXISTS (
                   SELECT 1 FROM sys.columns c
                   JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                   WHERE c.object_id = t.object_id
                     AND ty.name = 'timestamp'
               ) THEN 1 ELSE 0 END AS has_timestamp_col
        FROM sys.tables t
        WHERE t.is_replicated = 1
        ORDER BY t.name;
    """))


    write_csv(f"{out_dir}/03_features_{slug}.csv", csv_rows)
    return result



