"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

def tool_analyze_linked_servers(ctx: ServerContext, out_dir: str) -> dict:
    linked = rq(ctx, """
        SELECT s.name AS linked_server_name, s.product, s.provider, s.data_source,
               s.is_remote_login_enabled, s.is_rpc_out_enabled
        FROM sys.servers s
        WHERE s.is_linked=1 ORDER BY s.name;
    """)
    usage = rq(ctx, """
        SELECT OBJECT_NAME(m.object_id) AS object_name, o.type_desc AS object_type,
               LEFT(m.definition,200) AS usage_preview
        FROM sys.sql_modules m
        JOIN sys.objects o ON m.object_id = o.object_id
        WHERE (m.definition LIKE '%OPENQUERY%' OR m.definition LIKE '%OPENDATASOURCE%')
        ORDER BY object_name;
    """)
    write_csv(f"{out_dir}/07_linked_servers.csv", linked)
    write_csv(f"{out_dir}/07_linked_server_usage.csv", usage)
    return {"linked_servers": linked, "cross_server_usage": usage}



