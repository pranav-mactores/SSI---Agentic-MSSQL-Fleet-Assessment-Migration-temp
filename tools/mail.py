"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

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


