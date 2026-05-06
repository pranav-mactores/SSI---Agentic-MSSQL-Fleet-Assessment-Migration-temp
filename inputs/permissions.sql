/*
================================================================================
  SQL SERVER MIGRATION ANALYST - LEAST PRIVILEGE SETUP SCRIPT
================================================================================
  Target Versions  : SQL Server 2016 / 2017 / 2019 / 2022
  Migration Target : Another SQL Server Instance
  Analysis Scope   : Schema & Objects, SQL Agent Jobs, Linked Servers,
                     Backup History, Performance & Wait Stats
  
  INSTRUCTIONS:
  1. Run SECTION 1 on the SOURCE instance (as sysadmin)
  2. Run SECTION 2 once per USER DATABASE you want to analyze
  3. Run SECTION 3 (cleanup) after migration analysis is complete

  NOTE: Replace 'StrongP@ssword123!' with a secure password before running.
================================================================================
*/

/*
================================================================================
  SECTION 1 — SERVER-LEVEL SETUP  (Run on master, once per instance)
================================================================================
*/
USE [master];
GO

-- ── Step 1: Create the login ─────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'migration_analyst')
BEGIN
    CREATE LOGIN [migration_analyst]
        WITH PASSWORD        = N'StrongP@ssword123!',
             CHECK_POLICY    = ON,
             CHECK_EXPIRATION = OFF,
             DEFAULT_DATABASE = [master];
    PRINT 'LOGIN [migration_analyst] created.';
END
ELSE
    PRINT 'LOGIN [migration_analyst] already exists — skipped creation.';
GO

-- ── Step 2: Server-level permissions ─────────────────────────────────────────

-- VIEW ANY DATABASE
--   Lets the analyst see all databases (including their metadata) via
--   sys.databases, sys.master_files, etc.
GRANT VIEW ANY DATABASE TO [migration_analyst];
GO

-- VIEW ANY DEFINITION
--   Allows reading the DDL/source of any server-scoped object:
--   logins, server roles, linked server definitions, endpoints, etc.
GRANT VIEW ANY DEFINITION TO [migration_analyst];
GO

-- VIEW SERVER STATE
--   Grants access to all server-level DMVs and DMFs:
--     sys.dm_exec_sessions       — active sessions
--     sys.dm_exec_requests       — running queries
--     sys.dm_os_wait_stats       — cumulative wait statistics
--     sys.dm_os_ring_buffers     — memory / scheduler diagnostics
--     sys.dm_io_virtual_file_stats — I/O per data/log file
--     sys.dm_os_performance_counters — PerfMon counters in T-SQL
--     sys.dm_db_index_usage_stats  — index seek/scan/lookup counters
--     sys.dm_exec_query_stats    — cached query execution statistics
GRANT VIEW SERVER STATE TO [migration_analyst];
GO

-- SELECT ALL USER SECURABLES
--   Allows SELECT on any user table/view without being db_owner.
--   Needed for row-count estimation and data profiling.
GRANT SELECT ALL USER SECURABLES TO [migration_analyst];
GO

-- ── Step 3: msdb — SQL Agent Jobs & Backup History ───────────────────────────
USE [msdb];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'migration_analyst' AND type = 'S'
)
BEGIN
    CREATE USER [migration_analyst] FOR LOGIN [migration_analyst];
    PRINT 'USER [migration_analyst] created in [msdb].';
END
GO

-- SQLAgentReaderRole
--   Allows reading: sysjobs, sysjobsteps, sysjobschedules, sysjobhistory,
--   syscategories, sysoperators, sysalerts — everything needed to document
--   all Agent jobs, their steps, schedules, and historical outcomes.
ALTER ROLE [SQLAgentReaderRole] ADD MEMBER [migration_analyst];
GO

-- db_datareader on msdb
--   Covers backup history tables not exposed via the Agent role:
--     backupset, backupmediafamily, backupmediaset, restorehistory
--   Also covers sysmail_profile, sysmail_account for mail config audit.
ALTER ROLE [db_datareader] ADD MEMBER [migration_analyst];
GO

-- Explicit grants for safety (some tables need explicit SELECT in older builds)
GRANT SELECT ON [dbo].[sysjobs]              TO [migration_analyst];
GRANT SELECT ON [dbo].[sysjobsteps]          TO [migration_analyst];
GRANT SELECT ON [dbo].[sysjobschedules]      TO [migration_analyst];
GRANT SELECT ON [dbo].[sysjobhistory]        TO [migration_analyst];
GRANT SELECT ON [dbo].[syscategories]        TO [migration_analyst];
GRANT SELECT ON [dbo].[sysoperators]         TO [migration_analyst];
GRANT SELECT ON [dbo].[sysalerts]            TO [migration_analyst];
GRANT SELECT ON [dbo].[backupset]            TO [migration_analyst];
GRANT SELECT ON [dbo].[backupmediafamily]    TO [migration_analyst];
GRANT SELECT ON [dbo].[backupmediaset]       TO [migration_analyst];
GRANT SELECT ON [dbo].[restorehistory]       TO [migration_analyst];
GRANT SELECT ON [dbo].[sysmail_profile]      TO [migration_analyst];
GRANT SELECT ON [dbo].[sysmail_account]      TO [migration_analyst];
GRANT SELECT ON [dbo].[sysmail_profileaccount] TO [migration_analyst];
GO

PRINT 'msdb permissions granted.';
GO

/*
================================================================================
  SECTION 2 — PER USER DATABASE SETUP
  !! Run this block once for EACH database you want to analyze !!
  Replace <<DATABASE_NAME>> with the actual database name.
================================================================================
*/

-- ─── AdventureWorks2022 ──────────────────────────────────────────────────────
USE [AdventureWorks2022];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'migration_analyst' AND type = 'S'
)
BEGIN
    CREATE USER [migration_analyst] FOR LOGIN [migration_analyst];
    PRINT 'USER created in [AdventureWorks2022].';
END
GO

ALTER ROLE [db_datareader] ADD MEMBER [migration_analyst];
GO

GRANT VIEW DEFINITION     TO [migration_analyst];
GO

GRANT VIEW DATABASE STATE TO [migration_analyst];
GO

GRANT SHOWPLAN            TO [migration_analyst];
GO

PRINT 'Database-level permissions granted on [AdventureWorks2022].';
GO

-- ─── AdventureWorks2016 (SQL Server 2016 instance) ──────────────────────────
-- NOTE: Before running this block, fix the orphaned dbo on AdventureWorks2016:
--   USE [AdventureWorks2016];
--   ALTER AUTHORIZATION ON DATABASE::AdventureWorks2016 TO [sa];
-- Then run the block below on the SQL2016 instance.
USE [AdventureWorks2016];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'migration_analyst' AND type = 'S'
)
BEGIN
    CREATE USER [migration_analyst] FOR LOGIN [migration_analyst];
    PRINT 'USER created in [AdventureWorks2016].';
END
GO

ALTER ROLE [db_datareader] ADD MEMBER [migration_analyst];
GO

GRANT VIEW DEFINITION     TO [migration_analyst];
GO

GRANT VIEW DATABASE STATE TO [migration_analyst];
GO

GRANT SHOWPLAN            TO [migration_analyst];
GO

PRINT 'Database-level permissions granted on [AdventureWorks2016].';
GO

-- ─── TEMPLATE: copy and run once per target database ────────────────────────
/*
USE [<<DATABASE_NAME>>];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.database_principals
    WHERE name = N'migration_analyst' AND type = 'S'
)
BEGIN
    CREATE USER [migration_analyst] FOR LOGIN [migration_analyst];
    PRINT 'USER created in [<<DATABASE_NAME>>].';
END
GO

-- db_datareader
--   SELECT on all user tables and views — needed for row counts,
--   data type profiling, NULL analysis, and FK validation.
ALTER ROLE [db_datareader] ADD MEMBER [migration_analyst];
GO

-- VIEW DEFINITION
--   Read the source code of stored procedures, functions, views,
--   triggers, and synonyms inside this database.
GRANT VIEW DEFINITION TO [migration_analyst];
GO

-- VIEW DATABASE STATE
--   Access to database-scoped DMVs:
--     sys.dm_db_index_physical_stats   — fragmentation per index
--     sys.dm_db_partition_stats        — row/page counts per partition
--     sys.dm_db_index_usage_stats      — which indexes are actually used
--     sys.dm_db_missing_index_details  — missing index recommendations
GRANT VIEW DATABASE STATE TO [migration_analyst];
GO

-- SHOWPLAN
--   Retrieve estimated and actual execution plans.
--   Required for query performance analysis and index gap assessment.
GRANT SHOWPLAN TO [migration_analyst];
GO

PRINT 'Database-level permissions granted on [<<DATABASE_NAME>>].';
GO
*/

-- ─── QUICK MULTI-DB HELPER ──────────────────────────────────────────────────
-- Uncomment and run this block to apply Section 2 to ALL user databases at once.
-- Review the exclusion list and add any databases you want to skip.
/*
DECLARE @sql   NVARCHAR(MAX) = N'';
DECLARE @db    NVARCHAR(128);
DECLARE @excl  TABLE (name NVARCHAR(128));

-- Add databases to skip (system DBs are already excluded below)
INSERT INTO @excl VALUES
    (N'ReportServerTempDB');  -- example: add any you want to skip

DECLARE db_cursor CURSOR FAST_FORWARD FOR
    SELECT name
    FROM   sys.databases
    WHERE  state_desc = 'ONLINE'
      AND  database_id > 4                      -- skip system databases
      AND  name NOT IN (SELECT name FROM @excl)
    ORDER  BY name;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'
USE [' + @db + N'];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''migration_analyst'' AND type = ''S'')
    CREATE USER [migration_analyst] FOR LOGIN [migration_analyst];
ALTER ROLE [db_datareader]    ADD MEMBER [migration_analyst];
GRANT VIEW DEFINITION         TO [migration_analyst];
GRANT VIEW DATABASE STATE     TO [migration_analyst];
GRANT SHOWPLAN                TO [migration_analyst];
PRINT ''Permissions granted on [' + @db + N'].'';
';
    EXEC sp_executesql @sql;
    FETCH NEXT FROM db_cursor INTO @db;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;
*/

/*
================================================================================
  SECTION 3 — VERIFICATION QUERIES
  Run these as [migration_analyst] to confirm all permissions work correctly.
================================================================================
*/

-- ✅ 1. List all databases (VIEW ANY DATABASE)
-- SELECT name, state_desc, recovery_model_desc FROM sys.databases ORDER BY name;

-- ✅ 2. List all logins (VIEW ANY DEFINITION)
-- SELECT name, type_desc, is_disabled FROM sys.server_principals ORDER BY type_desc, name;

-- ✅ 3. List all linked servers (VIEW ANY DEFINITION)
-- SELECT name, product, provider, data_source, is_linked FROM sys.servers WHERE is_linked = 1;

-- ✅ 4. Wait statistics (VIEW SERVER STATE)
-- SELECT TOP 20 wait_type, waiting_tasks_count, wait_time_ms
-- FROM sys.dm_os_wait_stats
-- ORDER BY wait_time_ms DESC;

-- ✅ 5. Active sessions (VIEW SERVER STATE)
-- SELECT session_id, login_name, status, host_name, program_name, database_id
-- FROM sys.dm_exec_sessions WHERE is_user_process = 1;

-- ✅ 6. SQL Agent jobs (SQLAgentReaderRole on msdb)
-- USE msdb;
-- SELECT j.name, j.enabled, s.step_name, s.subsystem, s.command
-- FROM dbo.sysjobs j JOIN dbo.sysjobsteps s ON j.job_id = s.job_id
-- ORDER BY j.name, s.step_id;

-- ✅ 7. Backup history (db_datareader on msdb)
-- USE msdb;
-- SELECT database_name, backup_start_date, backup_finish_date, type, backup_size
-- FROM dbo.backupset ORDER BY backup_start_date DESC;

-- ✅ 8. Index fragmentation — per user DB (VIEW DATABASE STATE)
-- SELECT OBJECT_NAME(ips.object_id) AS table_name, i.name AS index_name,
--        ips.avg_fragmentation_in_percent, ips.page_count
-- FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
-- JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
-- WHERE ips.page_count > 100
-- ORDER BY ips.avg_fragmentation_in_percent DESC;

-- ✅ 9. Missing index recommendations — per user DB (VIEW DATABASE STATE)
-- SELECT mid.statement AS table_name,
--        migs.avg_user_impact, migs.user_seeks, migs.user_scans,
--        mid.equality_columns, mid.inequality_columns, mid.included_columns
-- FROM sys.dm_db_missing_index_details    mid
-- JOIN sys.dm_db_missing_index_groups     mig  ON mid.index_handle = mig.index_handle
-- JOIN sys.dm_db_missing_index_group_stats migs ON mig.index_group_handle = migs.group_handle
-- ORDER BY migs.avg_user_impact DESC;

/*
================================================================================
  SECTION 4 — CLEANUP
  Run AFTER migration analysis is complete to remove all access.
================================================================================
*/

-- Drop the login (also removes all orphaned database users automatically in SQL 2016+)
-- Uncomment when ready:
/*
USE [master];

-- Optional: explicitly drop db users first if needed
DECLARE @drop_sql NVARCHAR(MAX) = N'';
SELECT @drop_sql += N'USE [' + d.name + N']; IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N''migration_analyst'') DROP USER [migration_analyst]; '
FROM sys.databases d
WHERE state_desc = 'ONLINE' AND database_id > 4;
EXEC sp_executesql @drop_sql;

-- Drop from msdb
USE [msdb];
IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'migration_analyst')
    DROP USER [migration_analyst];

-- Finally drop the login
USE [master];
IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'migration_analyst')
    DROP LOGIN [migration_analyst];

PRINT 'migration_analyst login and all associated users have been removed.';
*/

/*
================================================================================
  PERMISSION SUMMARY
================================================================================

  SERVER LEVEL (master)
  ──────────────────────────────────────────────────────────────────────────────
  VIEW ANY DATABASE          → sys.databases, sys.master_files
  VIEW ANY DEFINITION        → Logins, linked servers, server-role members
  VIEW SERVER STATE          → All server DMVs (waits, sessions, I/O, memory,
                               query stats, execution plans cache)
  SELECT ALL USER SECURABLES → Row counts, data profiling across all user tables

  MSDB
  ──────────────────────────────────────────────────────────────────────────────
  SQLAgentReaderRole         → All SQL Agent objects (jobs, steps, schedules,
                               history, operators, alerts)
  db_datareader              → Backup/restore history, mail config

  PER USER DATABASE
  ──────────────────────────────────────────────────────────────────────────────
  db_datareader              → SELECT on all tables and views
  VIEW DEFINITION            → SP, function, view, trigger source code
  VIEW DATABASE STATE        → Index fragmentation, missing indexes,
                               partition stats, per-DB DMVs
  SHOWPLAN                   → Estimated & actual execution plans

================================================================================
*/
