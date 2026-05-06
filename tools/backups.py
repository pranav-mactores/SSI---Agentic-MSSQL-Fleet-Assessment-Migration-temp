"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

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



