"""
SQL Server Migration Analyst Agent  –  Multi-Server Edition
=============================================================
Features
  - Reads a CSV of servers (server/host, port, username, password)
  - Auto-detects SQL Server version (2014–2022) and edition
  - Gates every query to the correct version + edition
  - Each tool writes its own CSV report immediately
  - Claude agent loop synthesises a markdown summary per server
  - Final multi-server summary CSV written at the end

Requirements
  pip install anthropic pyodbc

Input CSV format  (servers.csv)
  server,port,username,password
  SQLPROD01,1433,migration_analyst,P@ssword1
  SQLPROD02\\INST1,1433,migration_analyst,P@ssword1
  10.0.0.5,1433,,                         ← blank user/pass = Windows Auth

Usage
  python sql_migration_agent.py --servers servers.csv --output-dir ./reports
"""

import anthropic
import pyodbc
import csv
import json
import argparse
import textwrap
import os
import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# SERVER CONTEXT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ServerContext:
    """Holds live connection + version/edition facts for one server."""
    label: str                      # safe filename slug
    server: str
    port: int
    conn: pyodbc.Connection
    version_int: int  = 0           # 12=2014, 13=2016, 14=2017, 15=2019, 16=2022
    version_str: str  = ""          # e.g. "15.0.4312.2"
    year:        int  = 0           # e.g. 2019
    edition:     str  = ""          # Enterprise, Standard, Web, Express, Developer
    is_enterprise: bool = False
    is_express:    bool = False

    # convenience helpers
    def v(self, minimum: int) -> bool:
        """True if server version >= minimum (e.g. v(13) → 2016+)."""
        return self.version_int >= minimum

    def ed(self, *editions: str) -> bool:
        """True if edition matches any of the given edition substrings."""
        low = self.edition.lower()
        return any(e.lower() in low for e in editions)

    def has_agent(self) -> bool:
        return not self.is_express

    def has_feature(self, min_ver: int, *req_editions: str) -> bool:
        if not self.v(min_ver):
            return False
        if req_editions and not self.ed(*req_editions):
            return False
        return True


VERSION_YEAR = {12: 2014, 13: 2016, 14: 2017, 15: 2019, 16: 2022}

def detect_server(conn: pyodbc.Connection, label: str, server: str, port: int) -> ServerContext:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS maj,
            CAST(SERVERPROPERTY('ProductVersion')      AS NVARCHAR(50)) AS ver,
            CAST(SERVERPROPERTY('Edition')             AS NVARCHAR(100)) AS edition;
    """)
    row = cur.fetchone()
    maj     = int(row.maj or 12)
    ver     = str(row.ver or "12.0")
    edition = str(row.edition or "Unknown")
    ctx = ServerContext(
        label       = label,
        server      = server,
        port        = port,
        conn        = conn,
        version_int = maj,
        version_str = ver,
        year        = VERSION_YEAR.get(maj, maj * 100),  # fallback
        edition     = edition,
        is_enterprise = "enterprise" in edition.lower() or "developer" in edition.lower(),
        is_express    = "express"    in edition.lower(),
    )
    print(f"  [detect] SQL Server {ctx.year} ({ver}) – {edition}")
    return ctx


# ─────────────────────────────────────────────────────────────────────────────
# CSV WRITER
# ─────────────────────────────────────────────────────────────────────────────

def safe_slug(text: str) -> str:
    return re.sub(r'[^A-Za-z0-9_-]', '_', text).strip('_')[:60]

def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("(no data)\n")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"    [csv] {os.path.basename(path)}  ({len(rows)} rows)")

def flatten(obj: Any, prefix: str = "") -> list[dict]:
    """Recursively flatten nested dicts/lists into a list of flat dicts."""
    if isinstance(obj, list):
        return obj  # already a list of dicts
    if isinstance(obj, dict):
        # try to find the first list value
        for v in obj.values():
            if isinstance(v, list):
                return v
        # no list found — wrap the dict itself
        return [obj]
    return [{"value": str(obj)}]


# ─────────────────────────────────────────────────────────────────────────────
# QUERY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def rq(ctx: ServerContext, sql: str, db: str | None = None) -> list[dict]:
    """Run a query; return list of dicts. Never raises — returns error row."""
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
        return [{"_error": str(e)}]

def na_row(reason: str) -> list[dict]:
    return [{"_not_applicable": reason}]


# ─────────────────────────────────────────────────────────────────────────────
# TOOL IMPLEMENTATIONS
# ─────────────────────────────────────────────────────────────────────────────

def tool_list_databases(ctx: ServerContext, out_dir: str) -> dict:
    rows = rq(ctx, """
        SELECT d.name, d.state_desc, d.recovery_model_desc,
               d.compatibility_level,
               CAST(SERVERPROPERTY('ProductMajorVersion') AS INT) AS server_major_version,
               CAST(SERVERPROPERTY('Edition') AS NVARCHAR(100)) AS server_edition,
               CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(10,2)) AS size_mb
        FROM sys.databases d
        JOIN sys.master_files mf ON d.database_id = mf.database_id
        WHERE d.database_id > 4 AND d.state_desc = 'ONLINE'
        GROUP BY d.name, d.state_desc, d.recovery_model_desc, d.compatibility_level
        ORDER BY d.name;
    """)
    write_csv(f"{out_dir}/01_databases.csv", rows)
    return {"databases": rows, "count": len(rows),
            "server_version": ctx.version_str, "server_year": ctx.year,
            "server_edition": ctx.edition}


def tool_analyze_schema(ctx: ServerContext, database: str, out_dir: str) -> dict:
    slug = safe_slug(database)

    objects = rq(ctx, """
        SELECT o.type_desc AS object_type, COUNT(*) AS count
        FROM sys.objects o
        WHERE o.type IN ('U','V','P','FN','IF','TF','TR','SN','SO','TA')
          AND o.is_ms_shipped = 0
        GROUP BY o.type_desc ORDER BY count DESC;
    """, database)

    tables = rq(ctx, """
        SELECT t.name AS table_name, s.name AS schema_name,
               SUM(p.rows) AS row_count, COUNT(DISTINCT c.column_id) AS column_count
        FROM sys.tables t
        JOIN sys.schemas    s ON t.schema_id = s.schema_id
        JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
        JOIN sys.columns    c ON t.object_id = c.object_id
        GROUP BY t.name, s.name ORDER BY row_count DESC;
    """, database)

    flags_rows = []
    flag_queries = {
        "CLR objects":                     "SELECT COUNT(*) FROM sys.objects WHERE type IN ('FS','FT','PC','TA') AND is_ms_shipped=0",
        "OPENQUERY / OPENDATASOURCE refs": "SELECT COUNT(*) FROM sys.sql_modules WHERE definition LIKE '%OPENQUERY%' OR definition LIKE '%OPENDATASOURCE%'",
        "OPENROWSET usage":                "SELECT COUNT(*) FROM sys.sql_modules WHERE definition LIKE '%OPENROWSET%'",
        "Dynamic SQL (sp_executesql)":     "SELECT COUNT(*) FROM sys.sql_modules WHERE definition LIKE '%sp_executesql%'",
        "Deprecated TEXT/NTEXT/IMAGE":     "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE t.name IN ('text','ntext','image')",
        "Four-part linked server names":   "SELECT COUNT(*) FROM sys.sql_modules WHERE definition LIKE '%].%].%].%]%'",
    }
    for flag, q in flag_queries.items():
        res = rq(ctx, f"USE [{database}]; SELECT ({q}) AS count;")
        cnt = list(res[0].values())[0] if res and "_error" not in res[0] else "error"
        flags_rows.append({"flag": flag, "count": cnt, "database": database})

    write_csv(f"{out_dir}/02_schema_{slug}_objects.csv",   objects)
    write_csv(f"{out_dir}/02_schema_{slug}_tables.csv",    tables)
    write_csv(f"{out_dir}/02_schema_{slug}_flags.csv",     flags_rows)
    return {"database": database, "object_summary": objects,
            "all_tables": tables, "migration_flags": flags_rows}


def tool_analyze_sql_features(ctx: ServerContext, database: str, out_dir: str) -> dict:
    slug = safe_slug(database)
    result: dict[str, Any] = {"database": database}
    csv_rows: list[dict] = []

    def record(feature: str, data: list[dict]) -> list[dict]:
        for row in data:
            row["_feature"] = feature
            row["_database"] = database
            csv_rows.append(row)
        return data

    def qdb(sql: str) -> list[dict]:
        return rq(ctx, sql, database)

    def qm(sql: str) -> list[dict]:
        return rq(ctx, sql)

    # ── Service Broker ────────────────────────────────────────────────────────
    result["service_broker"] = record("service_broker", qdb("""
        SELECT
          (SELECT is_broker_enabled FROM sys.databases WHERE name=DB_NAME()) AS broker_enabled,
          (SELECT COUNT(*) FROM sys.service_queues    WHERE is_ms_shipped=0) AS queues,
          (SELECT COUNT(*) FROM sys.services          WHERE is_ms_shipped=0) AS services,
          (SELECT COUNT(*) FROM sys.service_contracts WHERE is_ms_shipped=0) AS contracts,
          (SELECT COUNT(*) FROM sys.routes) AS routes;
    """))
    result["service_broker_queues"] = record("service_broker_queues", qdb("""
        SELECT q.name, q.is_activation_enabled, q.is_poison_message_handling_enabled,
               q.max_readers, s.name AS service_name
        FROM sys.service_queues q
        LEFT JOIN sys.services s ON q.object_id = s.service_queue_id
        WHERE q.is_ms_shipped=0;
    """))

    # ── CDC  (2008+ Enterprise; Standard/Web 2016+) ───────────────────────────
    if ctx.v(12) and (ctx.is_enterprise or ctx.v(13)):
        result["cdc"] = record("cdc", qdb("""
            SELECT
              (SELECT is_cdc_enabled FROM sys.databases WHERE name=DB_NAME()) AS db_cdc_enabled,
              (SELECT COUNT(*) FROM sys.tables WHERE is_tracked_by_cdc=1)     AS cdc_tracked_tables;
        """))
        result["cdc_tables"] = record("cdc_tables", qdb("""
            SELECT capture_instance, source_schema, source_table, supports_net_changes
            FROM cdc.change_tables;
        """))
    else:
        result["cdc"] = record("cdc", na_row(f"CDC not available on {ctx.edition}"))

    # ── Change Tracking  (2008+, all editions) ────────────────────────────────
    result["change_tracking"] = record("change_tracking", qdb("""
        SELECT
          (SELECT is_change_tracking_on FROM sys.databases WHERE name=DB_NAME()) AS ct_enabled,
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

    # ── Always On  (Enterprise 2012+; Standard Basic AG 2016+) ───────────────
    if ctx.v(12) and (ctx.is_enterprise or ctx.v(13)):
        result["always_on"] = record("always_on", qdb("""
            SELECT d.is_hadr_enabled, ar.replica_server_name,
                   ar.availability_mode_desc, ar.failover_mode_desc,
                   ag.name AS ag_name, agl.dns_name AS listener_name
            FROM sys.databases d
            LEFT JOIN sys.availability_replicas        ar  ON d.replica_id = ar.replica_id
            LEFT JOIN sys.availability_groups          ag  ON ar.group_id  = ag.group_id
            LEFT JOIN sys.availability_group_listeners agl ON ag.group_id  = agl.group_id
            WHERE d.name=DB_NAME() AND d.is_hadr_enabled=1;
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
    result["tde"] = record("tde", qdb("""
        SELECT d.name, d.is_encrypted, de.encryption_state_desc, de.encryptor_type
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
        SELECT sa.audit_action_name, sa.class_type_desc, sa.schema_name, sa.object_name
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
          (SELECT COUNT(*) FROM sys.symmetric_keys   WHERE key_id > 2)                 AS symmetric_keys;
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
        SELECT name, create_time, event_retention_mode_desc
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
               p.backup_threshold, p.threshold_alert_enabled,
               s.secondary_server, s.secondary_database,
               s.restore_delay, s.restore_threshold
        FROM msdb.dbo.log_shipping_primary_databases p
        LEFT JOIN msdb.dbo.log_shipping_primary_secondaries ps
               ON p.primary_id = ps.primary_id
        LEFT JOIN msdb.dbo.log_shipping_secondary_databases s
               ON ps.secondary_id = s.secondary_id
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
                   flush_interval_seconds, data_flush_interval_seconds,
                   interval_length_minutes, stale_query_threshold_days,
                   size_based_cleanup_mode_desc, query_capture_mode_desc,
                   wait_stats_capture_mode_desc
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

    # ── Stretch Database  (2016, deprecated 2022) ─────────────────────────────
    if ctx.v(13):
        result["stretch_db"] = record("stretch_db", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.tables
               WHERE is_remote_data_archive_enabled = 1) AS stretch_tables,
              (SELECT is_remote_data_archive_enabled
               FROM sys.databases WHERE name = DB_NAME())  AS db_stretch_enabled;
        """))
    else:
        result["stretch_db"] = record("stretch_db", na_row("Stretch DB requires SQL 2016+"))

    # ── PolyBase / External tables  (2016+) ───────────────────────────────────
    if ctx.v(13):
        result["polybase"] = record("polybase", qdb("""
            SELECT
              (SELECT COUNT(*) FROM sys.external_tables)       AS external_tables,
              (SELECT COUNT(*) FROM sys.external_data_sources) AS external_data_sources,
              (SELECT COUNT(*) FROM sys.external_file_formats) AS external_file_formats;
        """))
        result["polybase_sources"] = record("polybase_sources", qdb("""
            SELECT name, type_desc, location, credential_id, pushdown
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
                   key_algorithm, key_length,
                   MAX(backup_finish_date) AS last_encrypted_backup
            FROM msdb.dbo.backupset
            WHERE encryptor_thumbprint IS NOT NULL
            GROUP BY database_name, encryptor_type, encryptor_thumbprint,
                     key_algorithm, key_length
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
               STRING_AGG(sr.name, ', ') AS server_roles
        FROM sys.server_principals sp
        LEFT JOIN sys.sql_logins sl ON sp.principal_id = sl.principal_id
        LEFT JOIN sys.server_role_members srm ON sp.principal_id = srm.member_principal_id
        LEFT JOIN sys.server_principals   sr  ON srm.role_principal_id = sr.principal_id
        WHERE sp.type IN ('S','U','G')
          AND sp.name NOT LIKE '##%'
          AND sp.name NOT IN ('sa','BUILTIN\\Administrators')
        GROUP BY sp.name, sp.type_desc, sp.is_disabled,
                 sp.default_database_name,
                 sl.is_policy_checked, sl.is_expiration_checked
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
               en.event_type, en.source_object_id,
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
                   STRING_AGG(ss.subsystem_name, ', ') AS subsystems
            FROM msdb.dbo.sysproxies p
            LEFT JOIN sys.credentials c ON p.credential_id = c.credential_id
            LEFT JOIN msdb.dbo.sysproxysubsystem ps ON p.proxy_id = ps.proxy_id
            LEFT JOIN msdb.dbo.syssubsystems      ss ON ps.subsystem_id = ss.subsystem_id
            GROUP BY p.name, p.enabled, p.description, c.name
            ORDER BY p.name;
        """))
        result["agent_alerts"] = record("agent_alerts", qm("""
            SELECT a.name, a.enabled, a.message_id, a.severity,
                   a.database_name, a.event_description_keyword,
                   a.notification_message, a.job_name
            FROM msdb.dbo.sysalerts a
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
               description, create_date
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
        WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'AF')
          AND o.is_ms_shipped = 0
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


def tool_analyze_jobs(ctx: ServerContext, out_dir: str) -> dict:
    if not ctx.has_agent():
        write_csv(f"{out_dir}/04_jobs.csv", na_row("SQL Agent not available on Express edition"))
        return {"_not_applicable": "SQL Agent not available on Express edition"}

    jobs = rq(ctx, """
        SELECT j.name AS job_name, j.enabled, j.description,
               c.name AS category, o.name AS owner,
               COUNT(DISTINCT s.step_id) AS step_count,
               MAX(jh.run_date) AS last_run_date, MAX(jh.run_status) AS last_run_status
        FROM msdb.dbo.sysjobs j
        LEFT JOIN msdb.dbo.syscategories c  ON j.category_id=c.category_id
        LEFT JOIN sys.server_principals  o  ON j.owner_sid=o.sid
        LEFT JOIN msdb.dbo.sysjobsteps   s  ON j.job_id=s.job_id
        LEFT JOIN msdb.dbo.sysjobhistory jh ON j.job_id=jh.job_id AND jh.step_id=0
        GROUP BY j.name, j.enabled, j.description, c.name, o.name ORDER BY j.name;
    """)
    steps = rq(ctx, """
        SELECT j.name AS job_name, s.step_id, s.step_name, s.subsystem,
               LEFT(s.command,120) AS command_preview
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobsteps s ON j.job_id=s.job_id ORDER BY j.name, s.step_id;
    """)
    schedules = rq(ctx, """
        SELECT j.name AS job_name, sc.name AS schedule_name, sc.enabled,
               CASE sc.freq_type WHEN 1 THEN 'Once' WHEN 4 THEN 'Daily'
                   WHEN 8 THEN 'Weekly' WHEN 16 THEN 'Monthly'
                   WHEN 64 THEN 'Agent start' WHEN 128 THEN 'Idle CPU' ELSE 'Other' END AS frequency
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobschedules js ON j.job_id=js.job_id
        JOIN msdb.dbo.sysschedules    sc ON js.schedule_id=sc.schedule_id ORDER BY j.name;
    """)
    write_csv(f"{out_dir}/04_jobs.csv",           jobs)
    write_csv(f"{out_dir}/04_job_steps.csv",      steps)
    write_csv(f"{out_dir}/04_job_schedules.csv",  schedules)
    return {"jobs": jobs, "steps": steps, "schedules": schedules}


def tool_analyze_performance(ctx: ServerContext, out_dir: str) -> dict:
    waits = rq(ctx, """
        SELECT TOP 15 wait_type, waiting_tasks_count, wait_time_ms,
               CAST(wait_time_ms*100.0/NULLIF(SUM(wait_time_ms) OVER(),0) AS DECIMAL(5,2)) AS pct_total,
               signal_wait_time_ms
        FROM sys.dm_os_wait_stats
        WHERE wait_type NOT IN (
            'SLEEP_TASK','BROKER_TO_FLUSH','BROKER_TASK_STOP','CLR_AUTO_EVENT',
            'DISPATCHER_QUEUE_SEMAPHORE','FT_IFTS_SCHEDULER_IDLE_WAIT',
            'HADR_WORK_QUEUE','HADR_FILESTREAM_IOMGR_IOCOMPLETION',
            'LOGMGR_QUEUE','ONDEMAND_TASK_QUEUE','REQUEST_FOR_DEADLOCK_SEARCH',
            'RESOURCE_QUEUE','SERVER_IDLE_CHECK','SLEEP_DBSTARTUP','SLEEP_DBRECOVER',
            'SLEEP_DBNULL','SLEEP_MASTERDBREADY','SLEEP_MASTERMDREADY',
            'SLEEP_MASTERUPGRADED','SLEEP_MSDBSTARTUP','SLEEP_SYSTEMTASK',
            'SLEEP_TEMPDBSTARTUP','SNI_HTTP_ACCEPT','SP_SERVER_DIAGNOSTICS_SLEEP',
            'SQLTRACE_BUFFER_FLUSH','SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
            'WAIT_XTP_OFFLINE_CKPT_NEW_LOG','WAITFOR','XE_DISPATCHER_WAIT',
            'XE_TIMER_EVENT','BROKER_EVENTHANDLER','CHECKPOINT_QUEUE',
            'DBMIRROR_EVENTS_QUEUE','SQLTRACE_WAIT_ENTRIES',
            'WAIT_XTP_CKPT_CLOSE','XE_DISPATCHER_JOIN'
        ) AND waiting_tasks_count > 0
        ORDER BY wait_time_ms DESC;
    """)
    missing_idx = rq(ctx, """
        SELECT TOP 20
            DB_NAME(mid.database_id) AS database_name,
            OBJECT_NAME(mid.object_id, mid.database_id) AS table_name,
            migs.avg_user_impact,
            migs.user_seeks + migs.user_scans AS total_uses,
            mid.equality_columns, mid.inequality_columns, mid.included_columns
        FROM sys.dm_db_missing_index_details      mid
        JOIN sys.dm_db_missing_index_groups       mig  ON mid.index_handle=mig.index_handle
        JOIN sys.dm_db_missing_index_group_stats migs  ON mig.index_group_handle=migs.group_handle
        WHERE migs.avg_user_impact > 10
        ORDER BY migs.avg_user_impact DESC;
    """)
    memory = rq(ctx, """
        SELECT physical_memory_in_use_kb/1024 AS memory_used_mb,
               page_fault_count, memory_utilization_percentage
        FROM sys.dm_os_process_memory;
    """)
    write_csv(f"{out_dir}/05_perf_waits.csv",        waits)
    write_csv(f"{out_dir}/05_perf_missing_idx.csv",  missing_idx)
    write_csv(f"{out_dir}/05_perf_memory.csv",        memory)
    return {"top_waits": waits, "missing_indexes": missing_idx, "memory": memory}


def tool_analyze_backups(ctx: ServerContext, out_dir: str) -> dict:
    if ctx.is_express:
        write_csv(f"{out_dir}/06_backups.csv", na_row("Backup history not tracked in msdb on Express"))
        return {"_not_applicable": "Express does not log backup history in msdb"}
    rows = rq(ctx, """
        SELECT bs.database_name,
               MAX(CASE WHEN bs.type='D' THEN bs.backup_start_date END) AS last_full,
               MAX(CASE WHEN bs.type='I' THEN bs.backup_start_date END) AS last_diff,
               MAX(CASE WHEN bs.type='L' THEN bs.backup_start_date END) AS last_log,
               CAST(MAX(bs.backup_size)/1024.0/1024.0 AS DECIMAL(10,2)) AS max_backup_mb,
               COUNT(DISTINCT bs.media_set_id) AS backup_count_30d
        FROM msdb.dbo.backupset bs
        WHERE bs.backup_start_date >= DATEADD(DAY,-30,GETDATE())
        GROUP BY bs.database_name ORDER BY bs.database_name;
    """)
    write_csv(f"{out_dir}/06_backups.csv", rows)
    return {"backup_history_30d": rows}


def tool_analyze_linked_servers(ctx: ServerContext, out_dir: str) -> dict:
    linked = rq(ctx, """
        SELECT s.name AS linked_server_name, s.product, s.provider, s.data_source,
               ls.is_remote_login_enabled, ls.is_rpc_out_enabled
        FROM sys.servers s
        LEFT JOIN sys.linked_logins ls ON s.server_id=ls.server_id
        WHERE s.is_linked=1 ORDER BY s.name;
    """)
    usage = rq(ctx, """
        SELECT OBJECT_NAME(object_id) AS object_name, type_desc AS object_type,
               LEFT(definition,200) AS usage_preview
        FROM sys.sql_modules
        WHERE (definition LIKE '%OPENQUERY%' OR definition LIKE '%OPENDATASOURCE%')
          AND object_id IS NOT NULL ORDER BY object_name;
    """)
    write_csv(f"{out_dir}/07_linked_servers.csv", linked)
    write_csv(f"{out_dir}/07_linked_server_usage.csv", usage)
    return {"linked_servers": linked, "cross_server_usage": usage}


def tool_analyze_database_mail(ctx: ServerContext, out_dir: str) -> dict:
    if ctx.is_express:
        write_csv(f"{out_dir}/08_database_mail.csv", na_row("Database Mail not available on Express"))
        return {"_not_applicable": "Express edition"}
    rows = rq(ctx, """
        SELECT p.name AS profile_name, a.name AS account_name,
               a.email_address, a.mailserver_name, a.mailserver_type
        FROM msdb.dbo.sysmail_profile       p
        JOIN msdb.dbo.sysmail_profileaccount pa ON p.profile_id=pa.profile_id
        JOIN msdb.dbo.sysmail_account        a  ON pa.account_id=a.account_id ORDER BY p.name;
    """)
    write_csv(f"{out_dir}/08_database_mail.csv", rows)
    return {"database_mail": rows}


# ─────────────────────────────────────────────────────────────────────────────
# TOOL REGISTRY  (version-aware wrappers stored at call time)
# ─────────────────────────────────────────────────────────────────────────────

def build_tools() -> list[dict]:
    return [
        {
            "name": "list_databases",
            "description": "List all online user databases with size, recovery model, and compatibility level. Call first.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_schema",
            "description": (
                "Analyse all schema objects in a database: every table with row counts, "
                "views, SPs, functions, triggers, and migration risk flags."
            ),
            "input_schema": {"type": "object",
                "properties": {"database": {"type": "string"}}, "required": ["database"]},
        },
        {
            "name": "analyze_sql_features",
            "description": (
                "Inventory ALL SQL Server native features in a database "
                "(version/edition-gated automatically): Service Broker, CDC, Change Tracking, "
                "Replication, Always On, Mirroring, FTS, In-Memory OLTP, FILESTREAM, "
                "Partitioning, Temporal, Columnstore, Graph, Ledger, XML, Spatial, "
                "RLS, DDM, TDE, Always Encrypted, Audit, CLR, UDTs, Sequences, "
                "Synonyms, Certs, Orphaned Users, Missing PKs, Permissions."
            ),
            "input_schema": {"type": "object",
                "properties": {"database": {"type": "string"}}, "required": ["database"]},
        },
        {
            "name": "analyze_jobs",
            "description": "All SQL Agent jobs, steps, schedules, and last run status (skipped on Express).",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_performance",
            "description": "Top wait stats, top missing index recommendations, and memory usage.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_backups",
            "description": "Backup history for all databases (last 30 days) from msdb.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_linked_servers",
            "description": "All configured linked servers and objects referencing them.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_database_mail",
            "description": "Database Mail profiles and accounts configured in msdb.",
            "input_schema": {"type": "object", "properties": {}, "required": []},
        },
        {
            "name": "analyze_procedural_code",
            "description": (
                "Deep analysis of every stored procedure, scalar/TVF/inline function, "
                "and CLR object in a database. Call for every user database. "
                "Scans all source code for 19 risk patterns: xp_cmdshell, "
                "OPENROWSET/OPENQUERY, dynamic EXEC string concat, four-part linked names, "
                "CURSORs, global temp tables, deprecated *=/=* joins, SET ROWCOUNT, "
                "NOLOCK hints, @@ERROR without TRY/CATCH, undocumented SPs, GOTO, "
                "SELECT *, WAITFOR, RECOMPILE hints, other xp_ calls, RAISERROR, "
                "EXECUTE AS, sp_executesql. Also provides per-object complexity metrics, "
                "full parameter inventory, dependency mapping, and CLR assembly details."
            ),
            "input_schema": {
                "type": "object",
                "properties": {"database": {"type": "string", "description": "Database name."}},
                "required": ["database"],
            },
        },
    ]


def build_dispatch(ctx: ServerContext, out_dir: str) -> dict[str, Any]:
    return {
        "list_databases":       lambda _: tool_list_databases(ctx, out_dir),
        "analyze_schema":       lambda i: tool_analyze_schema(ctx, i["database"], out_dir),
        "analyze_sql_features": lambda i: tool_analyze_sql_features(ctx, i["database"], out_dir),
        "analyze_jobs":         lambda _: tool_analyze_jobs(ctx, out_dir),
        "analyze_performance":  lambda _: tool_analyze_performance(ctx, out_dir),
        "analyze_backups":      lambda _: tool_analyze_backups(ctx, out_dir),
        "analyze_linked_servers": lambda _: tool_analyze_linked_servers(ctx, out_dir),
        "analyze_database_mail":  lambda _: tool_analyze_database_mail(ctx, out_dir),
        "analyze_procedural_code": lambda i: tool_analyze_procedural_code(ctx, i["database"], out_dir),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# ─────────────────────────────────────────────────────────────────────────────

def build_system_prompt(ctx: ServerContext) -> str:
    return textwrap.dedent(f"""
        You are a SQL Server migration analyst agent.
        Connected server  : {ctx.server}
        Detected version  : SQL Server {ctx.year}  ({ctx.version_str})
        Edition           : {ctx.edition}

        IMPORTANT NOTES FOR THIS SERVER:
        - SQL Agent available : {"YES" if ctx.has_agent() else "NO (Express edition)"}
        - Enterprise features : {"YES (full feature set)" if ctx.is_enterprise else "NO — some features gated"}
        - Temporal Tables     : {"YES (2016+)" if ctx.v(13) else "NO (requires SQL 2016+)"}
        - Graph Tables        : {"YES (2017+)" if ctx.v(14) else "NO (requires SQL 2017+)"}
        - Ledger Tables       : {"YES (2022+)" if ctx.v(16) else "NO (requires SQL 2022+)"}
        - RLS / DDM / AE      : {"YES (2016+)" if ctx.v(13) else "NO (requires SQL 2016+)"}

        PROCESS:
        1. Call list_databases first.
        2. For EACH user database call analyze_schema, analyze_sql_features,
       AND analyze_procedural_code.
        3. Call analyze_jobs, analyze_performance, analyze_backups,
           analyze_linked_servers, analyze_database_mail once each.
        4. Synthesise a detailed Markdown migration assessment report.
           Where a feature is not applicable due to version/edition, state that clearly.

        REPORT SECTIONS:
        ## Executive Summary
        ## Server Profile  (version, edition, feature availability matrix)
        ## Database Inventory
        ## Schema Analysis  (per database)
        ## Native SQL Server Features  (per database)
        ## SQL Agent Jobs
        ## Linked Servers & Cross-DB Dependencies
        ## Database Mail
        ## Backup Strategy & RPO Analysis
        ## Performance Observations
        ## Migration Risks & Recommendations
        ## Suggested Migration Order

        FLAGGING RULES:
        - Orphaned users         → ❌ HIGH PRIORITY BLOCKER
        - Tables missing PKs     → ⚠️  RISK
        - Active Service Broker  → 🔔 SPECIAL CUTOVER REQUIRED
        - Active CDC             → 🔔 SPECIAL CUTOVER REQUIRED
        - Active Replication     → 🔔 SPECIAL CUTOVER REQUIRED
        - Always On / Mirroring  → 🔔 SPECIAL CUTOVER REQUIRED
        - CLR assemblies         → 🔍 MANUAL REVIEW REQUIRED
        - FILESTREAM / FileTable → 🔍 MANUAL REVIEW REQUIRED
        - In-Memory OLTP         → 🔍 MANUAL REVIEW REQUIRED
        - TDE / Always Encrypted → 🔑 KEY MIGRATION REQUIRED

        Be specific. Reference exact object names and counts from tool results.
        For features not available on this version/edition, note clearly why.
    """).strip()


# ─────────────────────────────────────────────────────────────────────────────
# AGENT LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run_agent(ctx: ServerContext, out_dir: str, max_turns: int = 40) -> str:
    client   = anthropic.Anthropic()
    tools    = build_tools()
    dispatch = build_dispatch(ctx, out_dir)
    system   = build_system_prompt(ctx)

    messages: list[dict] = [{
        "role": "user",
        "content": "Analyse this SQL Server instance and produce a full migration assessment report.",
    }]

    for turn in range(1, max_turns + 1):
        print(f"  [agent] Turn {turn}...")
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8096,
            system=system,
            tools=tools,
            messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return "(No text output)"

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            inp = block.input or {}
            print(f"  [agent]   → {block.name}({json.dumps(inp) if inp else ''})")
            try:
                result = dispatch[block.name](inp)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
            except Exception as e:
                print(f"  [agent]   ✗ {block.name} error: {e}")
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps({"error": str(e)}),
                    "is_error": True,
                })
        messages.append({"role": "user", "content": tool_results})

    return "(Reached max_turns)"


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGER  – persists progress across runs
# ─────────────────────────────────────────────────────────────────────────────

class StateManager:
    """
    Persists per-server progress to <output_dir>/state.json.

    Server states:
      PENDING          – not yet started
      IN_PROGRESS      – started but not finished (crash / Ctrl-C)
      SUCCESS          – completed successfully  → skip on next run
      CONNECTION_FAILED – could not connect      → retry on next run by default
      AGENT_FAILED     – connected but agent error → retry on next run by default

    Flags:
      --force          reprocess ALL servers regardless of state
      --retry-failed   also reprocess CONNECTION_FAILED / AGENT_FAILED servers
                       (SUCCESS servers are still skipped unless --force is set)
    """

    SKIP_STATUSES = {"SUCCESS"}          # statuses skipped by default
    RETRY_STATUSES = {"CONNECTION_FAILED", "AGENT_FAILED", "IN_PROGRESS"}

    def __init__(self, state_path: str, force: bool = False,
                 retry_failed: bool = False) -> None:
        self.path         = state_path
        self.force        = force
        self.retry_failed = retry_failed
        self._data: dict  = self._load()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load(self) -> dict:
        if os.path.exists(self.path):
            try:
                with open(self.path, encoding="utf-8") as f:
                    data = json.load(f)
                print(f"[state] Loaded checkpoint: {self.path}")
                return data
            except Exception as e:
                print(f"[state] Warning – could not read state file ({e}); starting fresh.")
        return {"created": _now(), "last_updated": _now(), "servers": {}}

    def _save(self) -> None:
        self._data["last_updated"] = _now()
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, default=str)

    # ── per-server accessors ──────────────────────────────────────────────────

    def get(self, label: str) -> dict:
        return self._data["servers"].get(label, {})

    def should_skip(self, label: str) -> bool:
        """Return True if the server should be skipped on this run."""
        if self.force:
            return False
        status = self.get(label).get("status", "PENDING")
        if status in self.SKIP_STATUSES:
            return True
        if status in self.RETRY_STATUSES and not self.retry_failed:
            # IN_PROGRESS means a previous run crashed mid-server → always retry
            return status != "IN_PROGRESS"
        return False

    def mark_in_progress(self, label: str, server: str, port: int) -> None:
        self._data["servers"][label] = {
            "status":     "IN_PROGRESS",
            "server":     server,
            "port":       port,
            "started_at": _now(),
            "attempt":    self.get(label).get("attempt", 0) + 1,
        }
        self._save()

    def mark_success(self, label: str, version: str, edition: str,
                     db_count: int, report_path: str) -> None:
        self._data["servers"][label].update({
            "status":       "SUCCESS",
            "completed_at": _now(),
            "version":      version,
            "edition":      edition,
            "db_count":     db_count,
            "report":       report_path,
        })
        self._save()

    def mark_failed(self, label: str, status: str, error: str,
                    version: str = "", edition: str = "") -> None:
        self._data["servers"][label].update({
            "status":    status,
            "failed_at": _now(),
            "error":     error,
            "version":   version,
            "edition":   edition,
        })
        self._save()

    # ── summary helpers ───────────────────────────────────────────────────────

    def print_summary(self, total: int) -> None:
        servers = self._data["servers"]
        counts  = {}
        for v in servers.values():
            s = v.get("status", "UNKNOWN")
            counts[s] = counts.get(s, 0) + 1
        pending = total - len(servers)
        if pending > 0:
            counts["PENDING"] = counts.get("PENDING", 0) + pending
        print("\n[state] Run summary:")
        for status, n in sorted(counts.items()):
            icon = {"SUCCESS": "✓", "IN_PROGRESS": "~", "PENDING": "·",
                    "CONNECTION_FAILED": "✗", "AGENT_FAILED": "✗"}.get(status, "?")
            print(f"  {icon}  {status:<22} {n}")

    def all_server_records(self) -> list[dict]:
        return list(self._data["servers"].values())


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



# ─────────────────────────────────────────────────────────────────────────────
# PER-SERVER SUMMARY REPORT
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def _isum(rows: list[dict], col: str) -> int:
    total = 0
    for r in rows:
        try: total += int(r.get(col, 0) or 0)
        except (ValueError, TypeError): pass
    return total

def _col_val(rows: list[dict], col: str, default: str = "") -> str:
    for r in rows:
        v = str(r.get(col, "") or "").strip()
        if v: return v
    return default

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
    dbs = _read_csv(f"{out_dir}/01_databases.csv")
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
        all_tables.extend(_read_csv(f"{out_dir}/02_schema_{slug}_tables.csv"))
        all_flags.extend(_read_csv(f"{out_dir}/02_schema_{slug}_flags.csv"))

    add("Schema", "Total tables (all DBs)", len([r for r in all_tables if "_error" not in r]))
    add("Schema", "Total rows (all DBs)",   f"{_isum(all_tables, 'row_count'):,}")

    for fr in all_flags:
        if "_error" in fr: continue
        cnt = int(fr.get("count", 0) or 0)
        if cnt > 0:
            name = fr.get("flag", "")
            sev  = "⚠️" if any(w in name.lower() for w in ["deprecated","clr"]) else "ℹ️"
            add("Schema Flags", name, cnt, sev, f"database={fr.get('database','')}")

    # ── Native features – per database ───────────────────────────────────────
    for db in db_names:
        slug      = safe_slug(db)
        feat_rows = _read_csv(f"{out_dir}/03_features_{slug}.csv")

        def rows_for(feat):
            return [r for r in feat_rows if r.get("_feature") == feat]

        def na(feat):
            rs = rows_for(feat)
            return rs and any("_not_applicable" in r for r in rs)

        def active_int(feat, col):
            return _isum(rows_for(feat), col)

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
            ag   = _col_val(rows_for("always_on"),"ag_name")
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
            f"state={_col_val(rows_for('tde'),'encryption_state_desc')}" if tde_on else "")

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
            ov_rows  = _read_csv(f"{out_dir}/09_proc_overview_{slug}.csv")
            rk_rows  = _read_csv(f"{out_dir}/09_proc_risks_{slug}.csv")
            clr_rows = _read_csv(f"{out_dir}/09_clr_detail_{slug}.csv")

            if not ov_rows or "_error" in ov_rows[0]:
                continue

            procs = [r for r in ov_rows if r.get("object_type") == "SQL_STORED_PROCEDURE"]
            fns   = [r for r in ov_rows
                     if r.get("object_type") not in ("SQL_STORED_PROCEDURE","CLR_STORED_PROCEDURE","")]

            add("Procedural Code", f"[{db}] Total stored procedures", len(procs))
            add("Procedural Code", f"[{db}] Total functions",         len(fns))
            add("Procedural Code", f"[{db}] Total CLR objects",       len(clr_rows))

            # top riskiest object
            if ov_rows:
                biggest = max(ov_rows, key=lambda r: int(r.get("line_count") or 0), default=None)
                if biggest:
                    add("Procedural Code", f"[{db}] Largest object",
                        biggest.get("object_name",""),
                        detail=f"{biggest.get('line_count',0)} lines  "
                               f"type={biggest.get('object_type','')}")

            # risk summary
            from collections import Counter
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

            # CLR permission sets
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

            # per-risk-type breakdown (only non-zero)
            for rtype, cnt in sorted(risk_counts.items()):
                if cnt > 0 and rtype:
                    add("Procedural Risks", f"[{db}] {rtype}", cnt)

    # ── SQL Agent ─────────────────────────────────────────────────────────────
    job_rows  = _read_csv(f"{out_dir}/04_jobs.csv")
    step_rows = _read_csv(f"{out_dir}/04_job_steps.csv")
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
    ls_rows = _read_csv(f"{out_dir}/07_linked_servers.csv")
    lu_rows = _read_csv(f"{out_dir}/07_linked_server_usage.csv")
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
    bk_rows = _read_csv(f"{out_dir}/06_backups.csv")
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
    wr = _read_csv(f"{out_dir}/05_perf_waits.csv")
    if wr:
        add("Performance", "Top wait type", _col_val(wr,"wait_type"),
            detail=f"{_col_val(wr,'pct_total')}% of total wait time")
    mi = _read_csv(f"{out_dir}/05_perf_missing_idx.csv")
    add("Performance", "Missing index recommendations", len(mi),
        "ℹ️" if mi else "")
    mr = _read_csv(f"{out_dir}/05_perf_memory.csv")
    if mr:
        add("Performance", "Memory used (MB)",        _col_val(mr,"memory_used_mb"))
        add("Performance", "Memory utilisation %",    _col_val(mr,"memory_utilization_percentage"))

    # ── Database Mail ─────────────────────────────────────────────────────────
    dm_rows = _read_csv(f"{out_dir}/08_database_mail.csv")
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

# ─────────────────────────────────────────────────────────────────────────────
# MULTI-SERVER ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def connect(server: str, port: int, username: str, password: str) -> pyodbc.Connection:
    if username and password:
        conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server},{port};DATABASE=master;"
                    f"UID={username};PWD={password};")
    else:
        conn_str = (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server},{port};DATABASE=master;Trusted_Connection=yes;")
    return pyodbc.connect(conn_str, autocommit=True, timeout=30)


def detect_server_info(conn, label, server, port) -> ServerContext:
    return detect_server(conn, label, server, port)


def process_server(row: dict, out_root: str,
                   state: StateManager, summary_rows: list[dict]) -> None:
    server   = row.get("server", "").strip()
    port     = int(row.get("port", 1433) or 1433)
    username = row.get("username", "").strip()
    password = row.get("password", "").strip()
    label    = safe_slug(f"{server}_{port}")

    print(f"\n{'='*60}")
    print(f"[server] {server}:{port}  (label: {label})")

    # ── checkpoint check ──────────────────────────────────────────────────────
    if state.should_skip(label):
        prev = state.get(label)
        print(f"  [skip] Already {prev.get('status')} at {prev.get('completed_at','')} — skipping.")
        summary_rows.append({
            "server": server, "port": port,
            "status":  prev.get("status"),
            "version": prev.get("version", ""),
            "edition": prev.get("edition", ""),
            "databases": prev.get("db_count", 0),
            "report":  prev.get("report", ""),
            "error":   prev.get("error", ""),
            "skipped": True,
        })
        return

    out_dir = os.path.join(out_root, label)
    os.makedirs(out_dir, exist_ok=True)
    state.mark_in_progress(label, server, port)

    # ── connect ───────────────────────────────────────────────────────────────
    try:
        conn = connect(server, port, username, password)
        ctx  = detect_server_info(conn, label, server, port)
    except Exception as e:
        print(f"  [ERROR] Connection failed: {e}")
        state.mark_failed(label, "CONNECTION_FAILED", str(e))
        summary_rows.append({
            "server": server, "port": port, "status": "CONNECTION_FAILED",
            "version": "", "edition": "", "databases": 0, "report": "", "error": str(e),
            "skipped": False,
        })
        write_csv(os.path.join(out_dir, "CONNECTION_FAILED.csv"),
                  [{"server": server, "port": port, "error": str(e), "timestamp": _now()}])
        return

    # ── run agent ─────────────────────────────────────────────────────────────
    try:
        report_md = run_agent(ctx, out_dir)

        header = textwrap.dedent(f"""
            # SQL Server Migration Assessment
            Server    : {server}:{port}
            Version   : SQL Server {ctx.year} ({ctx.version_str})
            Edition   : {ctx.edition}
            Generated : {_now()}
            ---
        """).lstrip()

        md_path = os.path.join(out_dir, "migration_report.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(header + report_md)
        print(f"  [done] Report → {md_path}")

        # Per-server summary CSV (built from task CSVs, no extra SQL queries)
        summary_csv_path = generate_server_summary_csv(ctx, out_dir, server, port)

        db_csv   = os.path.join(out_dir, "01_databases.csv")
        db_count = 0
        if os.path.exists(db_csv):
            with open(db_csv) as f:
                db_count = max(0, sum(1 for _ in f) - 1)

        state.mark_success(label, f"SQL {ctx.year}", ctx.edition, db_count, md_path)
        summary_rows.append({
            "server": server, "port": port, "status": "SUCCESS",
            "version": f"SQL {ctx.year}", "edition": ctx.edition,
            "databases": db_count, "report": md_path,
            "summary_csv": summary_csv_path, "error": "", "skipped": False,
        })

    except Exception as e:
        print(f"  [ERROR] Agent failed: {e}")
        state.mark_failed(label, "AGENT_FAILED", str(e),
                          version=f"SQL {ctx.year}", edition=ctx.edition)
        summary_rows.append({
            "server": server, "port": port, "status": "AGENT_FAILED",
            "version": f"SQL {ctx.year}", "edition": ctx.edition,
            "databases": 0, "report": "", "error": str(e), "skipped": False,
        })
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SQL Server Migration Analyst Agent — Multi-Server Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            examples:
              # first run
              python sql_migration_agent.py --servers servers.csv --output-dir ./reports

              # resume after crash  (skips SUCCESS, retries IN_PROGRESS)
              python sql_migration_agent.py --servers servers.csv --output-dir ./reports

              # also retry connection/agent failures
              python sql_migration_agent.py --servers servers.csv --output-dir ./reports --retry-failed

              # reprocess everything from scratch
              python sql_migration_agent.py --servers servers.csv --output-dir ./reports --force
        """),
    )
    parser.add_argument("--servers",      required=True,
                        help="CSV: server,port,username,password")
    parser.add_argument("--output-dir",   default="./migration_reports",
                        help="Root output directory  (default: ./migration_reports)")
    parser.add_argument("--force",        action="store_true",
                        help="Reprocess ALL servers, ignoring saved state")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Also retry CONNECTION_FAILED and AGENT_FAILED servers")
    parser.add_argument("--status",       action="store_true",
                        help="Print current state and exit (no processing)")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    state_path = os.path.join(args.output_dir, "state.json")
    state = StateManager(state_path, force=args.force, retry_failed=args.retry_failed)

    # Read server list
    with open(args.servers, newline="", encoding="utf-8") as f:
        server_rows = list(csv.DictReader(f))

    # ── --status mode: just show current state and exit ───────────────────────
    if args.status:
        print(f"\n[state] Checkpoint: {state_path}")
        print(f"[state] Total servers in CSV: {len(server_rows)}\n")
        for row in server_rows:
            server = row.get("server", "").strip()
            port   = int(row.get("port", 1433) or 1433)
            label  = safe_slug(f"{server}_{port}")
            rec    = state.get(label)
            status = rec.get("status", "PENDING")
            extra  = ""
            if status == "SUCCESS":
                extra = f"  completed={rec.get('completed_at','')}  dbs={rec.get('db_count','?')}"
            elif status in ("CONNECTION_FAILED", "AGENT_FAILED"):
                extra = f"  failed={rec.get('failed_at','')}  err={rec.get('error','')[:60]}"
            elif status == "IN_PROGRESS":
                extra = f"  started={rec.get('started_at','')}  attempt={rec.get('attempt','?')}"
            icon = {"SUCCESS":"✓","IN_PROGRESS":"~","PENDING":"·",
                    "CONNECTION_FAILED":"✗","AGENT_FAILED":"✗"}.get(status,"?")
            print(f"  {icon}  {status:<22}  {server}:{port}{extra}")
        return

    # ── normal run ────────────────────────────────────────────────────────────
    print(f"[main] {len(server_rows)} server(s) in CSV  |  "
          f"force={args.force}  retry-failed={args.retry_failed}")

    if args.force:
        print("[main] --force: all servers will be reprocessed")
    elif args.retry_failed:
        print("[main] --retry-failed: failed servers will be retried")

    summary_rows: list[dict] = []
    for row in server_rows:
        process_server(row, args.output_dir, state, summary_rows)

    # Final summary CSV
    summary_path = os.path.join(args.output_dir, "00_summary.csv")
    write_csv(summary_path, summary_rows)
    state.print_summary(len(server_rows))
    print(f"\n[main] Done. Summary → {summary_path}  |  State → {state_path}")


if __name__ == "__main__":
    main()
