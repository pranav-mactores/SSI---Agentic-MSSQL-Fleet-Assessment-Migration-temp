"""Auto-split from sql_migration_agent.py"""
from db.connection import ServerContext
from db.query import rq, na_row
from reports.csv_writer import write_csv, safe_slug

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



