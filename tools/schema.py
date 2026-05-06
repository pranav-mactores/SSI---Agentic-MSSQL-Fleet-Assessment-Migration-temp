"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

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
        WHERE t.is_ms_shipped = 0
          AND s.name NOT IN ('sys', 'cdc', 'INFORMATION_SCHEMA')
        GROUP BY t.name, s.name ORDER BY row_count DESC;
    """, database)

    flags_rows = []
    flag_queries = {
        "CLR objects":                     "SELECT COUNT(*) FROM sys.objects WHERE type IN ('FS','FT','PC','TA') AND is_ms_shipped=0",
        "OPENQUERY / OPENDATASOURCE refs": "SELECT COUNT(*) FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.is_ms_shipped=0 AND (m.definition LIKE '%OPENQUERY%' OR m.definition LIKE '%OPENDATASOURCE%')",
        "OPENROWSET usage":                "SELECT COUNT(*) FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.is_ms_shipped=0 AND m.definition LIKE '%OPENROWSET%'",
        "Dynamic SQL (sp_executesql)":     "SELECT COUNT(*) FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.is_ms_shipped=0 AND m.definition LIKE '%sp_executesql%'",
        "Deprecated TEXT/NTEXT/IMAGE":     "SELECT COUNT(*) FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id JOIN sys.objects o ON c.object_id=o.object_id WHERE o.is_ms_shipped=0 AND t.name IN ('text','ntext','image')",
        "Four-part linked server names":   "SELECT COUNT(*) FROM sys.sql_modules m JOIN sys.objects o ON m.object_id=o.object_id WHERE o.is_ms_shipped=0 AND m.definition LIKE '%].%].%].%]%'",
    }
    for flag, q in flag_queries.items():
        res = rq(ctx, f"USE [{database}]; SELECT ({q}) AS count;")
        cnt = list(res[0].values())[0] if res and "_error" not in res[0] else 0
        flags_rows.append({"flag": flag, "count": cnt, "database": database})

    write_csv(f"{out_dir}/02_schema_{slug}_objects.csv",   objects)
    write_csv(f"{out_dir}/02_schema_{slug}_tables.csv",    tables)
    write_csv(f"{out_dir}/02_schema_{slug}_flags.csv",     flags_rows)
    return {"database": database, "object_summary": objects,
            "all_tables": tables, "migration_flags": flags_rows}



