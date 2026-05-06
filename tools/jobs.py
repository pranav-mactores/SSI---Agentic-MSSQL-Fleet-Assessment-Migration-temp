"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

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



