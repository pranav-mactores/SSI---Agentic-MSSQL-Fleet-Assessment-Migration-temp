/*
================================================================================
  SQL SERVER FEATURE ENABLEMENT SCRIPT — AdventureWorks2022
================================================================================
  Target Version : SQL Server 2022 (16.0.1000.6) — Developer Edition
  Target Instance: EC2AMAZ-R98HQD0\SQL2022
  Target Database: AdventureWorks2022

  Enables the following inactive features identified in the migration report:
    Service Broker, CDC, Change Tracking, Query Store, TDE, DDM, RLS,
    In-Memory OLTP, Temporal Tables, Columnstore Indexes, Partitioning,
    Graph Tables, Ledger Tables, FILESTREAM/FileTable, Always Encrypted,
    Replication, Always On AG

  SECTIONS:
    1. Instance-Level Features   (run as sysadmin on master)
    2. Database-Level Features   (run on AdventureWorks2022)
    3. Schema-Level Features     (filegroups, tables, indexes)
    4. Security Features         (TDE, DDM, RLS, Always Encrypted)
    5. Infrastructure Features   (Replication, Always On, Mirroring notes)
    6. Verification Queries
    7. Cleanup

  NOTE: Each block is idempotent — safe to re-run.
        Run sections in order; some have cross-section dependencies.
================================================================================
*/

/*
================================================================================
  SECTION 1 — INSTANCE-LEVEL FEATURES  (run on master as sysadmin)
================================================================================
*/
USE [master];
GO

-- ── 1a: FILESTREAM ────────────────────────────────────────────────────────────
--   Stores BLOB data (documents, images) in the NTFS file system while
--   maintaining transactional consistency with the database.
--
--   PREREQUISITES (must be done before running sp_configure):
--     Open SQL Server Configuration Manager → SQL Server Services →
--     right-click your instance → Properties → FILESTREAM tab →
--     check "Enable FILESTREAM for Transact-SQL access".
--
--   Level values:
--     0 = disabled
--     1 = T-SQL access only
--     2 = T-SQL + Win32 streaming access
--     3 = T-SQL + Win32 + remote client access

-- Verify current state before enabling
SELECT SERVERPROPERTY('FilestreamConfiguredLevel')  AS configured_level,
       SERVERPROPERTY('FilestreamEffectiveLevel')   AS effective_level;
GO

-- Enable FILESTREAM for T-SQL access (level 1).
-- Uncomment ONLY after enabling in SQL Server Configuration Manager:
/*
EXEC sp_configure 'filestream access level', 1;
RECONFIGURE;
PRINT 'FILESTREAM enabled at instance level (T-SQL access).';
*/
GO

-- ── 1b: Always On Availability Groups — instance prep ────────────────────────
--   Always On AG provides high availability and read-scale replicas.
--
--   FULL PREREQUISITES (cannot be scripted here):
--     • Windows Server Failover Cluster (WSFC) with 2+ nodes
--     • SQL Server installed on each node with same edition/version
--     • Shared or mirrored storage / log-shipping style sync
--
--   The two sp_configure calls below prepare this instance for AG use and
--   require a SQL Server service restart to take effect.

/*
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
EXEC sp_configure 'hadr enabled', 1;
RECONFIGURE WITH OVERRIDE;
PRINT 'Always On AG enabled at instance level — restart SQL Server service to apply.';
*/
-- After restart, full AG creation:
--   CREATE AVAILABILITY GROUP [...] WITH (AUTOMATED_BACKUP_PREFERENCE = SECONDARY, ...)
--   ALTER  AVAILABILITY GROUP [...] ADD DATABASE [AdventureWorks2022];
-- Use the SSMS Availability Group Wizard for guided setup.
GO

-- ── 1c: Database Mirroring ────────────────────────────────────────────────────
--   ⚠️  REMOVED in SQL Server 2022.
--       Deprecated since SQL 2008 R2 SP1; not available in SQL 2016+.
--       Replacement: Always On Availability Groups (see Section 1b / Section 5b).
PRINT 'Database Mirroring is not available in SQL Server 2022. Use Always On AG.';
GO

/*
================================================================================
  SECTION 2 — DATABASE-LEVEL FEATURES  (run on AdventureWorks2022)
================================================================================
*/
USE [AdventureWorks2022];
GO

-- ── 2a: Service Broker ────────────────────────────────────────────────────────
--   Provides native async messaging and queuing within SQL Server.
--   Required for Query Notifications and event-driven architectures.
--   ROLLBACK IMMEDIATE ends any open transactions to acquire the exclusive lock.

IF (SELECT is_broker_enabled FROM sys.databases WHERE name = N'AdventureWorks2022') = 0
BEGIN
    ALTER DATABASE [AdventureWorks2022] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
    PRINT 'Service Broker enabled on [AdventureWorks2022].';
END
ELSE
    PRINT 'Service Broker already enabled — skipped.';
GO

-- ── 2b: Change Data Capture (CDC) ─────────────────────────────────────────────
--   CDC captures INSERT / UPDATE / DELETE changes to tracked tables and stores
--   them in cdc.* system tables for ETL pipelines, auditing, and replication.
--   SQL Server Agent must be running for the capture and cleanup jobs.

-- Enable CDC at the database level
IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = N'AdventureWorks2022') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on [AdventureWorks2022].';
END
ELSE
    PRINT 'CDC already enabled on [AdventureWorks2022] — skipped.';
GO

-- Track Sales.SalesOrderHeader (all columns; net changes enabled)
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('Sales.SalesOrderHeader')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'Sales',
        @source_name          = N'SalesOrderHeader',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on [Sales].[SalesOrderHeader].';
END
GO

-- Track Sales.SalesOrderDetail
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('Sales.SalesOrderDetail')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'Sales',
        @source_name          = N'SalesOrderDetail',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on [Sales].[SalesOrderDetail].';
END
GO

-- Track HumanResources.Employee
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('HumanResources.Employee')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'HumanResources',
        @source_name          = N'Employee',
        @role_name            = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on [HumanResources].[Employee].';
END
GO

-- ── 2c: Change Tracking ───────────────────────────────────────────────────────
--   Lightweight alternative to CDC: records which rows changed (not the
--   before/after values). Ideal for sync scenarios (mobile, offline clients).

IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID(N'AdventureWorks2022'))
BEGIN
    ALTER DATABASE [AdventureWorks2022]
        SET CHANGE_TRACKING = ON
        (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);
    PRINT 'Change Tracking enabled on [AdventureWorks2022] (3-day retention, auto-cleanup).';
END
ELSE
    PRINT 'Change Tracking already enabled — skipped.';
GO

-- Enable per-table: Production.Product
IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_tables
    WHERE object_id = OBJECT_ID('Production.Product')
)
BEGIN
    ALTER TABLE [Production].[Product]
        ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
    PRINT 'Change Tracking enabled on [Production].[Product].';
END
GO

-- Enable per-table: Person.Person
IF NOT EXISTS (
    SELECT 1 FROM sys.change_tracking_tables
    WHERE object_id = OBJECT_ID('Person.Person')
)
BEGIN
    ALTER TABLE [Person].[Person]
        ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
    PRINT 'Change Tracking enabled on [Person].[Person].';
END
GO

-- ── 2d: Query Store ───────────────────────────────────────────────────────────
--   Captures query execution plans and runtime statistics over time.
--   Enables plan regression detection, forced plans, and wait stats per query.

IF (SELECT actual_state FROM sys.database_query_store_options) <> 1
BEGIN
    ALTER DATABASE [AdventureWorks2022]
        SET QUERY_STORE = ON (
            OPERATION_MODE              = READ_WRITE,
            CLEANUP_POLICY              = (STALE_QUERY_THRESHOLD_DAYS = 30),
            DATA_FLUSH_INTERVAL_SECONDS = 900,
            MAX_STORAGE_SIZE_MB         = 1000,
            INTERVAL_LENGTH_MINUTES     = 60,
            SIZE_BASED_CLEANUP_MODE     = AUTO,
            QUERY_CAPTURE_MODE          = AUTO,
            MAX_PLANS_PER_QUERY         = 200,
            WAIT_STATS_CAPTURE_MODE     = ON
        );
    PRINT 'Query Store enabled on [AdventureWorks2022] (1 GB, 30-day retention, AUTO capture).';
END
ELSE
    PRINT 'Query Store already enabled — skipped.';
GO

/*
================================================================================
  SECTION 3 — SCHEMA-LEVEL FEATURES  (filegroups, tables, indexes)
================================================================================
*/
USE [AdventureWorks2022];
GO

-- ── 3a: In-Memory OLTP (Hekaton) ─────────────────────────────────────────────
--   Lock-free / latch-free memory-optimized tables for high-throughput OLTP.
--   Requires a MEMORY_OPTIMIZED_DATA filegroup with a container directory.

-- Add the filegroup
IF NOT EXISTS (
    SELECT 1 FROM sys.filegroups WHERE type = 'FX'
)
BEGIN
    ALTER DATABASE [AdventureWorks2022]
        ADD FILEGROUP [AW_InMemory_FG] CONTAINS MEMORY_OPTIMIZED_DATA;
    PRINT 'In-Memory OLTP filegroup [AW_InMemory_FG] created.';
END
GO

-- Add the container file (directory path derived from existing data file)
-- NOTE: SQL Server 2022 RTM requires HADR (Always On) enabled at the instance level
--       even for standalone In-Memory OLTP. Run Section 1b + restart first if needed.
IF SERVERPROPERTY('IsHadrEnabled') = 0
BEGIN
    PRINT 'SKIPPED: In-Memory OLTP container requires HADR enabled at the instance level.';
    PRINT '  1. Uncomment the sp_configure block in Section 1b.';
    PRINT '  2. Restart the SQL Server service.';
    PRINT '  3. Re-run this section.';
END
ELSE IF NOT EXISTS (
    SELECT 1 FROM sys.database_files
    WHERE data_space_id = (
        SELECT data_space_id FROM sys.filegroups WHERE type = 'FX'
    )
)
BEGIN
    DECLARE @base_path NVARCHAR(260);
    SELECT @base_path = physical_name
    FROM   sys.master_files
    WHERE  database_id = DB_ID('AdventureWorks2022') AND file_id = 1;

    SET @base_path = LEFT(@base_path, LEN(@base_path) - CHARINDEX('\', REVERSE(@base_path)))
                     + N'\AW_InMemory_Container';

    DECLARE @sql NVARCHAR(1000);
    SET @sql = N'ALTER DATABASE [AdventureWorks2022] ADD FILE (
              NAME     = N''AW_InMemory_Container'',
              FILENAME = N''' + @base_path + N'''
          ) TO FILEGROUP [AW_InMemory_FG];';
    EXEC sp_executesql @sql;
    PRINT 'In-Memory OLTP container added.';
END
ELSE
    PRINT 'In-Memory OLTP container already exists — skipped.';
GO

-- Example memory-optimized table: high-throughput shopping cart
IF SERVERPROPERTY('IsHadrEnabled') = 0
    PRINT 'SKIPPED: [Sales].[ShoppingCart_InMemory] — enable HADR first (see container step above).';
ELSE IF OBJECT_ID('Sales.ShoppingCart_InMemory', 'U') IS NULL
BEGIN
    CREATE TABLE [Sales].[ShoppingCart_InMemory]
    (
        CartID      INT          NOT NULL,
        ProductID   INT          NOT NULL,
        Quantity    SMALLINT     NOT NULL CONSTRAINT DF_ShoppingCart_Qty DEFAULT 1,
        AddedDate   DATETIME2(3) NOT NULL CONSTRAINT DF_ShoppingCart_Date DEFAULT SYSDATETIME(),
        CONSTRAINT PK_ShoppingCart_InMemory PRIMARY KEY NONCLUSTERED (CartID, ProductID)
    )
    WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
    PRINT 'Memory-optimized table [Sales].[ShoppingCart_InMemory] created.';
END
GO

-- ── 3b: Temporal Tables ──────────────────────────────────────────────────────
--   System-time versioned tables automatically track full row history.
--   Enables point-in-time queries: SELECT ... FOR SYSTEM_TIME AS OF '2024-01-01'

IF OBJECT_ID('Production.ProductPriceHistory', 'U') IS NULL
BEGIN
    CREATE TABLE [Production].[ProductPriceHistory]
    (
        ProductID    INT          NOT NULL,
        ListPrice    MONEY        NOT NULL,
        ModifiedBy   NVARCHAR(50) NOT NULL CONSTRAINT DF_PPH_ModifiedBy DEFAULT SYSTEM_USER,
        SysStartTime DATETIME2(7) GENERATED ALWAYS AS ROW START NOT NULL,
        SysEndTime   DATETIME2(7) GENERATED ALWAYS AS ROW END   NOT NULL,
        PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime),
        CONSTRAINT PK_ProductPriceHistory PRIMARY KEY (ProductID, SysStartTime)
    )
    WITH (
        SYSTEM_VERSIONING = ON (
            HISTORY_TABLE = [Production].[ProductPriceHistory_Archive]
        )
    );
    PRINT 'Temporal table [Production].[ProductPriceHistory] created with auto-history table.';
END
GO

-- ── 3c: Columnstore Indexes ──────────────────────────────────────────────────
--   Column-oriented storage + batch execution mode.
--   10-100x faster for analytical aggregations over large row sets.
--   Nonclustered variant preserves the existing clustered (row-store) index.

-- Covers common BI aggregations on Sales.SalesOrderHeader
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('Sales.SalesOrderHeader') AND type = 6
)
BEGIN
    CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_SalesOrderHeader_Analytics]
        ON [Sales].[SalesOrderHeader]
        (
            OrderDate, DueDate, ShipDate,
            CustomerID, SalesPersonID, TerritoryID,
            SubTotal, TaxAmt, Freight,
            Status, OnlineOrderFlag
        );
    PRINT 'Nonclustered columnstore index created on [Sales].[SalesOrderHeader].';
END
GO

-- Covers line-item analytics on Sales.SalesOrderDetail
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('Sales.SalesOrderDetail') AND type = 6
)
BEGIN
    CREATE NONCLUSTERED COLUMNSTORE INDEX [NCCI_SalesOrderDetail_Analytics]
        ON [Sales].[SalesOrderDetail]
        (
            SalesOrderID, ProductID, SpecialOfferID,
            OrderQty, UnitPrice, UnitPriceDiscount
        );
    PRINT 'Nonclustered columnstore index created on [Sales].[SalesOrderDetail].';
END
GO

-- ── 3d: Partitioning ─────────────────────────────────────────────────────────
--   Splits large tables by a key range into independent partitions.
--   Enables partition elimination, faster archiving, and parallel scans.
--   Example: partition SalesOrderHeader by OrderDate year.

IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'PF_SalesOrderByYear')
BEGIN
    CREATE PARTITION FUNCTION [PF_SalesOrderByYear] (DATETIME)
        AS RANGE RIGHT FOR VALUES
        (
            '2011-01-01',
            '2012-01-01',
            '2013-01-01',
            '2014-01-01',
            '2015-01-01'
        );
    PRINT 'Partition function [PF_SalesOrderByYear] created (6 partitions: <2011, 2011..2015, >2015).';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'PS_SalesOrderByYear')
BEGIN
    CREATE PARTITION SCHEME [PS_SalesOrderByYear]
        AS PARTITION [PF_SalesOrderByYear]
        ALL TO ([PRIMARY]);
    PRINT 'Partition scheme [PS_SalesOrderByYear] created — all partitions on PRIMARY.';
END
GO

-- Demonstration: partitioned staging table mirroring SalesOrderHeader structure
IF OBJECT_ID('Sales.SalesOrderHeader_Partitioned', 'U') IS NULL
BEGIN
    CREATE TABLE [Sales].[SalesOrderHeader_Partitioned]
    (
        SalesOrderID INT      NOT NULL,
        OrderDate    DATETIME NOT NULL,
        CustomerID   INT      NOT NULL,
        TotalDue     MONEY    NOT NULL,
        Status       TINYINT  NOT NULL
    )
    ON [PS_SalesOrderByYear] (OrderDate);
    PRINT 'Partitioned demo table [Sales].[SalesOrderHeader_Partitioned] created.';
END
GO

-- ── 3e: Graph Tables ─────────────────────────────────────────────────────────
--   Node/edge tables with MATCH syntax for relationship traversal.
--   Replaces complex self-joins for social graphs, org charts, recommendations.

IF OBJECT_ID('dbo.PersonNode', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[PersonNode]
    (
        PersonID  INT           NOT NULL,
        FullName  NVARCHAR(150) NOT NULL,
        Email     NVARCHAR(256) NULL
    ) AS NODE;
    PRINT 'Graph node table [dbo].[PersonNode] created.';
END
GO

IF OBJECT_ID('dbo.ProductNode', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[ProductNode]
    (
        ProductID   INT           NOT NULL,
        ProductName NVARCHAR(256) NOT NULL,
        Category    NVARCHAR(100) NULL
    ) AS NODE;
    PRINT 'Graph node table [dbo].[ProductNode] created.';
END
GO

IF OBJECT_ID('dbo.Purchased', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[Purchased]
    (
        PurchaseDate DATE NOT NULL,
        Quantity     INT  NOT NULL CONSTRAINT DF_Purchased_Qty DEFAULT 1
    ) AS EDGE;
    PRINT 'Graph edge table [dbo].[Purchased] (Person → Product) created.';
END
GO

-- Sample MATCH query (run after inserting nodes/edges):
-- SELECT p.FullName, pr.ProductName
-- FROM   dbo.PersonNode p, dbo.Purchased purch, dbo.ProductNode pr
-- WHERE  MATCH(p-(purch)->pr) AND p.PersonID = 1;

-- ── 3f: Ledger Tables ────────────────────────────────────────────────────────
--   SQL Server 2022 feature: cryptographically tamper-evident audit trail.
--   Both tables use APPEND_ONLY = ON: rows can only be INSERTed, never changed.
--   SQL Server generates a cryptographic digest chain verifiable by auditors.

IF OBJECT_ID('dbo.AuditLedger', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[AuditLedger]
    (
        EventID   INT           IDENTITY(1,1) NOT NULL,
        EventTime DATETIME2(3)  NOT NULL CONSTRAINT DF_AuditLedger_Time DEFAULT SYSUTCDATETIME(),
        UserName  NVARCHAR(128) NOT NULL CONSTRAINT DF_AuditLedger_User DEFAULT SYSTEM_USER,
        Action    NVARCHAR(50)  NOT NULL,
        TableName NVARCHAR(256) NOT NULL,
        RowID     INT           NULL,
        Notes     NVARCHAR(MAX) NULL
    )
    WITH (LEDGER = ON (APPEND_ONLY = ON));
    PRINT 'Append-only ledger table [dbo].[AuditLedger] created.';
END
GO

IF OBJECT_ID('dbo.PriceChangeLog', 'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[PriceChangeLog]
    (
        ChangeID  INT           IDENTITY(1,1) NOT NULL,
        ProductID INT           NOT NULL,
        OldPrice  MONEY         NOT NULL,
        NewPrice  MONEY         NOT NULL,
        ChangedBy NVARCHAR(128) NOT NULL CONSTRAINT DF_PCL_ChangedBy DEFAULT SYSTEM_USER,
        ChangedAt DATETIME2(3)  NOT NULL CONSTRAINT DF_PCL_ChangedAt DEFAULT SYSUTCDATETIME()
    )
    WITH (LEDGER = ON (APPEND_ONLY = ON));
    PRINT 'Append-only ledger table [dbo].[PriceChangeLog] created.';
END
GO

-- ── 3g: FILESTREAM / FileTable ───────────────────────────────────────────────
--   FILESTREAM stores BLOBs in NTFS with transactional integrity.
--   FileTable extends this with Windows folder/file semantics (UNC path access).
--   REQUIRES: Instance-level FILESTREAM enabled first (see Section 1a).

/*
-- Add FILESTREAM filegroup — adjust path to an existing folder on this server
IF NOT EXISTS (SELECT 1 FROM sys.filegroups WHERE type = 'FD')
BEGIN
    ALTER DATABASE [AdventureWorks2022]
        ADD FILEGROUP [AW_FileStream_FG] CONTAINS FILESTREAM;

    ALTER DATABASE [AdventureWorks2022]
        ADD FILE (
            NAME     = N'AW_FileStream_Container',
            FILENAME = N'C:\SQLData\AW_FileStream'   -- folder must exist
        )
        TO FILEGROUP [AW_FileStream_FG];
    PRINT 'FILESTREAM filegroup and container added.';
END
GO

-- Enable NON_TRANSACTED_ACCESS for FileTable Windows access
ALTER DATABASE [AdventureWorks2022]
    SET FILESTREAM (
        NON_TRANSACTED_ACCESS = FULL,
        DIRECTORY_NAME        = N'AdventureWorks2022'
    );
GO

-- Example FileTable for product documentation (PDFs, images, specs)
IF OBJECT_ID('Production.ProductDocuments', 'U') IS NULL
BEGIN
    CREATE TABLE [Production].[ProductDocuments] AS FileTable
    WITH (
        FileTable_Directory         = 'ProductDocuments',
        FileTable_Collate_Filename  = database_default
    );
    PRINT 'FileTable [Production].[ProductDocuments] created.';
END
GO
*/
PRINT 'FILESTREAM/FileTable: enable FILESTREAM at instance level first (Section 1a), then uncomment Section 3g.';
GO

/*
================================================================================
  SECTION 4 — SECURITY FEATURES
================================================================================
*/

-- ── 4a: Transparent Data Encryption (TDE) ────────────────────────────────────
--   Encrypts the entire database at rest: data files, log file, and backups.
--   Transparent to the application — no connection string or query changes needed.
--
--   ⚠️  BACK UP the certificate + private key immediately after Step 2.
--       Without this backup, the database cannot be restored to another instance.

USE [master];
GO

-- Step 1: Server master key (required to protect the certificate)
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = N'##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'MasterKeyP@ss2024!';
    PRINT 'Database master key created in [master].';
END
GO

-- Step 2: Certificate to protect the Database Encryption Key
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = N'TDE_AdventureWorks_Cert')
BEGIN
    CREATE CERTIFICATE [TDE_AdventureWorks_Cert]
        WITH SUBJECT     = 'AdventureWorks2022 TDE Certificate',
             EXPIRY_DATE = '2030-01-01';
    PRINT 'TDE certificate [TDE_AdventureWorks_Cert] created in [master].';
END
GO

-- ⚠️  Back up the certificate — update paths and password before running:
/*
BACKUP CERTIFICATE [TDE_AdventureWorks_Cert]
    TO FILE = N'C:\Backups\TDE_AdventureWorks_Cert.cer'
    WITH PRIVATE KEY (
        FILE               = N'C:\Backups\TDE_AdventureWorks_Cert.pvk',
        ENCRYPTION BY PASSWORD = N'CertBackupP@ss2024!'
    );
PRINT 'TDE certificate backed up — store private key file off this server.';
*/

USE [AdventureWorks2022];
GO

-- Step 3: Database Encryption Key (DEK), protected by the certificate
IF NOT EXISTS (
    SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID()
)
BEGIN
    CREATE DATABASE ENCRYPTION KEY
        WITH ALGORITHM = AES_256
        ENCRYPTION BY SERVER CERTIFICATE [TDE_AdventureWorks_Cert];
    PRINT 'Database Encryption Key (AES_256) created on [AdventureWorks2022].';
END
GO

-- Step 4: Enable encryption (background scan will run — monitor with verification query)
IF NOT EXISTS (
    SELECT 1 FROM sys.dm_database_encryption_keys
    WHERE database_id = DB_ID() AND encryption_state = 3
)
BEGIN
    ALTER DATABASE [AdventureWorks2022] SET ENCRYPTION ON;
    PRINT 'TDE encryption enabled — background scan in progress (check verification query 5).';
END
ELSE
    PRINT 'TDE already fully encrypted — skipped.';
GO

-- ── 4b: Dynamic Data Masking (DDM) ───────────────────────────────────────────
--   Masks sensitive column values in query results for non-privileged users.
--   Data is stored and indexed unmasked; sysadmin / db_owner always see plaintext.

USE [AdventureWorks2022];
GO

-- Mask email addresses: show first char + domain shape only
IF NOT EXISTS (
    SELECT 1 FROM sys.masked_columns mc
    JOIN   sys.tables t ON mc.object_id = t.object_id
    WHERE  t.name = 'EmailAddress' AND SCHEMA_NAME(t.schema_id) = 'Person'
      AND  mc.name = 'EmailAddress'
)
BEGIN
    ALTER TABLE [Person].[EmailAddress]
        ALTER COLUMN [EmailAddress] ADD MASKED WITH (FUNCTION = 'email()');
    PRINT 'DDM email mask applied to [Person].[EmailAddress].[EmailAddress].';
END
GO

-- Mask phone numbers: show only last 4 digits
IF NOT EXISTS (
    SELECT 1 FROM sys.masked_columns mc
    JOIN   sys.tables t ON mc.object_id = t.object_id
    WHERE  t.name = 'PersonPhone' AND SCHEMA_NAME(t.schema_id) = 'Person'
      AND  mc.name = 'PhoneNumber'
)
BEGIN
    ALTER TABLE [Person].[PersonPhone]
        ALTER COLUMN [PhoneNumber] ADD MASKED WITH (FUNCTION = 'partial(2,"XXX-XXX-",4)');
    PRINT 'DDM partial mask applied to [Person].[PersonPhone].[PhoneNumber].';
END
GO

-- Mask credit card numbers: show only last 4 digits
IF NOT EXISTS (
    SELECT 1 FROM sys.masked_columns mc
    JOIN   sys.tables t ON mc.object_id = t.object_id
    WHERE  t.name = 'CreditCard' AND SCHEMA_NAME(t.schema_id) = 'Sales'
      AND  mc.name = 'CardNumber'
)
BEGIN
    ALTER TABLE [Sales].[CreditCard]
        ALTER COLUMN [CardNumber] ADD MASKED WITH (FUNCTION = 'partial(0,"XXXX-XXXX-XXXX-",4)');
    PRINT 'DDM partial mask applied to [Sales].[CreditCard].[CardNumber].';
END
GO

-- Create an unprivileged test user to verify masking
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'ddm_test_user')
BEGIN
    CREATE USER [ddm_test_user] WITHOUT LOGIN;
    GRANT SELECT ON [Person].[EmailAddress] TO [ddm_test_user];
    GRANT SELECT ON [Person].[PersonPhone]  TO [ddm_test_user];
    GRANT SELECT ON [Sales].[CreditCard]    TO [ddm_test_user];
    PRINT 'DDM test user created — run verification query 6 to confirm masks.';
END
GO
-- Verify mask behavior:
-- EXECUTE AS USER = 'ddm_test_user';
-- SELECT TOP 5 EmailAddress FROM Person.EmailAddress;
-- REVERT;

-- ── 4c: Row-Level Security (RLS) ─────────────────────────────────────────────
--   Filters rows automatically based on the executing user's identity.
--   Applied via an inline TVF predicate + security policy — transparent to apps.
--   Example: sales reps see only their own orders; db_owners see all.

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'Security')
    EXEC('CREATE SCHEMA [Security]');
GO

IF OBJECT_ID('Security.fn_SalesOrderFilter', 'IF') IS NULL
    EXEC('
    CREATE FUNCTION [Security].[fn_SalesOrderFilter](@SalesPersonID INT)
    RETURNS TABLE
    WITH SCHEMABINDING
    AS
    RETURN
        SELECT 1 AS fn_result
        WHERE  IS_MEMBER(''db_owner'') = 1          -- db owners see all rows
            OR USER_NAME() = ''migration_analyst''   -- analyst account sees all
            OR @SalesPersonID IS NULL;               -- online orders (no rep) visible to all
    ');
GO
PRINT 'RLS predicate function [Security].[fn_SalesOrderFilter] created.';
GO

IF NOT EXISTS (SELECT 1 FROM sys.security_policies WHERE name = N'SalesOrderPolicy')
BEGIN
    CREATE SECURITY POLICY [Security].[SalesOrderPolicy]
        ADD FILTER PREDICATE [Security].[fn_SalesOrderFilter](SalesPersonID)
            ON [Sales].[SalesOrderHeader]
    WITH (STATE = ON, SCHEMABINDING = ON);
    PRINT 'RLS security policy [Security].[SalesOrderPolicy] created and enabled.';
END
GO

-- ── 4d: Always Encrypted ─────────────────────────────────────────────────────
--   Client-side encryption: the SQL Server engine never sees plaintext.
--   Column Master Key (CMK) lives in the client key store (Windows Cert Store
--   or Azure Key Vault). The database stores only encrypted ciphertext.
--
--   This CANNOT be fully scripted in T-SQL alone — the CMK must be generated
--   through a tool that has access to the client key store:
--     • SSMS: right-click database → Tasks → Encrypt Columns (wizard)
--     • PowerShell: SqlServer module → New-SqlColumnMasterKeySettings /
--                   New-SqlColumnEncryptionKey
--
--   After generating keys via SSMS/PowerShell, the metadata T-SQL looks like:
/*
-- Column Master Key (points to a certificate in Windows Cert Store)
CREATE COLUMN MASTER KEY [CMK_AW_WinStore]
    WITH (
        KEY_STORE_PROVIDER_NAME = N'MSSQL_CERTIFICATE_STORE',
        KEY_PATH                = N'CurrentUser/My/<thumbprint>'  -- replace with actual
    );

-- Column Encryption Key (value generated and encrypted by SSMS/PowerShell)
CREATE COLUMN ENCRYPTION KEY [CEK_AW_Sensitive]
    WITH VALUES (
        COLUMN_MASTER_KEY  = [CMK_AW_WinStore],
        ALGORITHM          = 'RSA_OAEP',
        ENCRYPTED_VALUE    = 0x...  -- generated value from SSMS/PowerShell
    );

-- Encrypt a column (SSMS wizard handles the offline migration automatically)
ALTER TABLE [Sales].[CreditCard]
    ALTER COLUMN [CardNumber]
        NVARCHAR(25) COLLATE Latin1_General_BIN2
        ENCRYPTED WITH (
            COLUMN_ENCRYPTION_KEY = [CEK_AW_Sensitive],
            ENCRYPTION_TYPE       = DETERMINISTIC,  -- allows equality filter
            ALGORITHM             = 'AEAD_AES_256_CBC_HMAC_SHA_256'
        ) NOT NULL;
*/
PRINT 'Always Encrypted: use SSMS Encrypt Columns wizard or PowerShell SqlServer module. See Section 4d.';
GO

/*
================================================================================
  SECTION 5 — INFRASTRUCTURE FEATURES  (require external setup)
================================================================================
*/

-- ── 5a: Replication ───────────────────────────────────────────────────────────
--   Transactional replication streams committed changes from a publisher to
--   one or more subscribers in near-real-time via the Log Reader Agent.
--
--   PREREQUISITES:
--     • SQL Server Agent running on publisher and distributor
--     • AdventureWorks2022 must be on FULL recovery model
--     • A full backup must exist before the first snapshot
--
--   Run the following as sysadmin after completing the prerequisites:
/*
USE [master];
GO

-- Step 1: Configure this instance as its own distributor
EXEC sp_adddistributor
    @distributor = @@SERVERNAME,
    @password    = N'DistributorP@ss1!';

EXEC sp_adddistributiondb
    @database          = N'distribution',
    @data_folder       = N'C:\Program Files\Microsoft SQL Server\MSSQL16.SQL2022\MSSQL\Data',
    @log_folder        = N'C:\Program Files\Microsoft SQL Server\MSSQL16.SQL2022\MSSQL\Data',
    @log_file_size     = 2,
    @min_distretention = 0,
    @max_distretention = 72,
    @history_retention = 48;
GO

-- Step 2: Switch to FULL recovery (required for log-based replication)
ALTER DATABASE [AdventureWorks2022] SET RECOVERY FULL;
GO

-- Step 3: Take a full backup (establishes the log chain)
BACKUP DATABASE [AdventureWorks2022]
    TO DISK = N'C:\Backups\AdventureWorks2022_RepBaseline.bak'
    WITH INIT, COMPRESSION, STATS = 10;
GO

-- Step 4: Register AdventureWorks2022 as a publication database
EXEC sp_replicationdboption
    @dbname  = N'AdventureWorks2022',
    @optname = N'publish',
    @value   = N'true';
GO

-- Step 5: Create a transactional publication
USE [AdventureWorks2022];
GO

EXEC sp_addpublication
    @publication          = N'AW_TransactionalPub',
    @status               = N'active',
    @sync_method          = N'concurrent',
    @repl_freq            = N'continuous',
    @description          = N'AdventureWorks2022 transactional publication',
    @independent_agent    = N'true',
    @immediate_sync       = N'true',
    @allow_push           = N'true',
    @allow_pull           = N'true';
GO

-- Step 6: Add articles (tables) to publish
EXEC sp_addarticle
    @publication   = N'AW_TransactionalPub',
    @article       = N'SalesOrderHeader',
    @source_owner  = N'Sales',
    @source_object = N'SalesOrderHeader',
    @type          = N'logbased';

EXEC sp_addarticle
    @publication   = N'AW_TransactionalPub',
    @article       = N'SalesOrderDetail',
    @source_owner  = N'Sales',
    @source_object = N'SalesOrderDetail',
    @type          = N'logbased';
GO
*/
PRINT 'Replication: see Section 5a comments — requires FULL recovery, distributor setup, and full backup.';
GO

-- ── 5b: Always On Availability Groups — full setup note ──────────────────────
--   After completing the WSFC/infrastructure prerequisites and enabling the
--   feature at the instance level (Section 1b + service restart), use:
--
--     SSMS → Object Explorer → Always On High Availability →
--       New Availability Group Wizard
--
--   Or use the CREATE AVAILABILITY GROUP statement documented at:
--     https://docs.microsoft.com/en-us/sql/t-sql/statements/create-availability-group-transact-sql
PRINT 'Always On AG: complete WSFC setup first, then enable at instance level (Section 1b).';
GO

-- ── 5c: Database Mirroring ────────────────────────────────────────────────────
PRINT 'Database Mirroring: REMOVED in SQL Server 2022. Migrate to Always On AG (Section 5b).';
GO

/*
================================================================================
  SECTION 6 — VERIFICATION QUERIES
================================================================================
*/
USE [AdventureWorks2022];
GO

-- ✅ 1. Service Broker state
-- SELECT name, is_broker_enabled FROM sys.databases WHERE name = 'AdventureWorks2022';

-- ✅ 2. CDC — database and tracked tables
-- SELECT is_cdc_enabled FROM sys.databases WHERE name = 'AdventureWorks2022';
-- SELECT source_schema, source_table, capture_instance, supports_net_changes
-- FROM   cdc.change_tables;

-- ✅ 3. Change Tracking — database and tables
-- SELECT is_change_tracking_enabled, change_tracking_retention_period,
--        change_tracking_retention_period_units_desc
-- FROM   sys.databases WHERE name = 'AdventureWorks2022';
-- SELECT OBJECT_SCHEMA_NAME(object_id) + '.' + OBJECT_NAME(object_id) AS tbl,
--        is_track_columns_updated_on
-- FROM   sys.change_tracking_tables;

-- ✅ 4. Query Store — state and storage
-- SELECT actual_state_desc, desired_state_desc, query_capture_mode_desc,
--        current_storage_size_mb, max_storage_size_mb
-- FROM   sys.database_query_store_options;

-- ✅ 5. TDE — encryption state (3 = encrypted, 2 = in progress)
-- SELECT db.name, dek.encryption_state_desc, dek.percent_complete, dek.encryptor_type
-- FROM   sys.dm_database_encryption_keys dek
-- JOIN   sys.databases db ON dek.database_id = db.database_id
-- WHERE  db.name = 'AdventureWorks2022';

-- ✅ 6. Dynamic Data Masking — verify masks (run as ddm_test_user)
-- SELECT t.name AS table_name, c.name AS column_name, c.masking_function
-- FROM   sys.masked_columns c JOIN sys.tables t ON c.object_id = t.object_id;
-- EXECUTE AS USER = 'ddm_test_user';
-- SELECT TOP 3 EmailAddress FROM Person.EmailAddress;
-- SELECT TOP 3 PhoneNumber  FROM Person.PersonPhone;
-- SELECT TOP 3 CardNumber   FROM Sales.CreditCard;
-- REVERT;

-- ✅ 7. Row-Level Security — policies and predicates
-- SELECT p.name AS policy, p.is_enabled, pred.predicate_type_desc,
--        pred.target_schema_name + '.' + pred.target_object_name AS target
-- FROM   sys.security_policies p
-- JOIN   sys.security_predicates pred ON p.object_id = pred.object_id;

-- ✅ 8. In-Memory OLTP — filegroup and tables
-- SELECT fg.name, fg.type_desc FROM sys.filegroups fg WHERE fg.type = 'FX';
-- SELECT OBJECT_SCHEMA_NAME(object_id) + '.' + name AS tbl, durability_desc
-- FROM   sys.tables WHERE is_memory_optimized = 1;

-- ✅ 9. Temporal Tables
-- SELECT name, temporal_type_desc, OBJECT_NAME(history_table_id) AS history_table
-- FROM   sys.tables WHERE temporal_type > 0;

-- ✅ 10. Columnstore Indexes
-- SELECT t.name AS table_name, i.name AS index_name, i.type_desc
-- FROM   sys.indexes i JOIN sys.tables t ON i.object_id = t.object_id
-- WHERE  i.type IN (5, 6);  -- 5=CLUSTERED COLUMNSTORE, 6=NONCLUSTERED COLUMNSTORE

-- ✅ 11. Partitioning — functions and schemes
-- SELECT pf.name AS fn, ps.name AS scheme, prv.value AS boundary
-- FROM   sys.partition_functions pf
-- JOIN   sys.partition_schemes    ps  ON pf.function_id = ps.function_id
-- JOIN   sys.partition_range_values prv ON pf.function_id = prv.function_id
-- ORDER  BY prv.value;

-- ✅ 12. Graph Tables
-- SELECT name, CASE WHEN is_node = 1 THEN 'NODE' ELSE 'EDGE' END AS graph_type
-- FROM   sys.tables WHERE is_node = 1 OR is_edge = 1;

-- ✅ 13. Ledger Tables
-- SELECT name, ledger_type_desc FROM sys.tables WHERE ledger_type > 0;

/*
================================================================================
  SECTION 7 — CLEANUP
  Removes all objects and settings created by this script.
  Run AFTER testing is complete.
================================================================================
*/
/*
USE [AdventureWorks2022];
GO

-- RLS
DROP SECURITY POLICY IF EXISTS [Security].[SalesOrderPolicy];
IF OBJECT_ID('Security.fn_SalesOrderFilter', 'IF') IS NOT NULL
    DROP FUNCTION [Security].[fn_SalesOrderFilter];

-- DDM: remove column masks
ALTER TABLE [Person].[EmailAddress] ALTER COLUMN [EmailAddress] DROP MASKED;
ALTER TABLE [Person].[PersonPhone]  ALTER COLUMN [PhoneNumber]  DROP MASKED;
ALTER TABLE [Sales].[CreditCard]    ALTER COLUMN [CardNumber]   DROP MASKED;
DROP USER IF EXISTS [ddm_test_user];

-- Columnstore indexes
DROP INDEX IF EXISTS [NCCI_SalesOrderHeader_Analytics] ON [Sales].[SalesOrderHeader];
DROP INDEX IF EXISTS [NCCI_SalesOrderDetail_Analytics] ON [Sales].[SalesOrderDetail];

-- Temporal table (must turn off versioning before dropping)
IF OBJECT_ID('Production.ProductPriceHistory', 'U') IS NOT NULL
BEGIN
    ALTER TABLE [Production].[ProductPriceHistory] SET (SYSTEM_VERSIONING = OFF);
    DROP TABLE [Production].[ProductPriceHistory];
    DROP TABLE IF EXISTS [Production].[ProductPriceHistory_Archive];
END

-- Graph tables (edge must be dropped before nodes)
DROP TABLE IF EXISTS [dbo].[Purchased];
DROP TABLE IF EXISTS [dbo].[PersonNode];
DROP TABLE IF EXISTS [dbo].[ProductNode];

-- Ledger tables (append-only — drop directly)
DROP TABLE IF EXISTS [dbo].[AuditLedger];
DROP TABLE IF EXISTS [dbo].[PriceChangeLog];

-- Partitioning
DROP TABLE            IF EXISTS [Sales].[SalesOrderHeader_Partitioned];
DROP PARTITION SCHEME  IF EXISTS [PS_SalesOrderByYear];
DROP PARTITION FUNCTION IF EXISTS [PF_SalesOrderByYear];

-- In-Memory OLTP table (filegroup removal requires no objects in it)
DROP TABLE IF EXISTS [Sales].[ShoppingCart_InMemory];

-- Change Tracking
ALTER TABLE [Production].[Product] DISABLE CHANGE_TRACKING;
ALTER TABLE [Person].[Person]      DISABLE CHANGE_TRACKING;
ALTER DATABASE [AdventureWorks2022] SET CHANGE_TRACKING = OFF;

-- CDC
EXEC sys.sp_cdc_disable_table @source_schema = 'Sales',          @source_name = 'SalesOrderHeader', @capture_instance = 'all';
EXEC sys.sp_cdc_disable_table @source_schema = 'Sales',          @source_name = 'SalesOrderDetail',  @capture_instance = 'all';
EXEC sys.sp_cdc_disable_table @source_schema = 'HumanResources', @source_name = 'Employee',          @capture_instance = 'all';
EXEC sys.sp_cdc_disable_db;

-- TDE: disable encryption (background scan runs to decrypt)
ALTER DATABASE [AdventureWorks2022] SET ENCRYPTION OFF;
-- Wait for encryption_state = 1 (unencrypted) before dropping DEK:
-- SELECT encryption_state_desc FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID();
DROP DATABASE ENCRYPTION KEY;

-- Query Store
ALTER DATABASE [AdventureWorks2022] SET QUERY_STORE = OFF;
ALTER DATABASE [AdventureWorks2022] SET QUERY_STORE CLEAR;

-- Service Broker
ALTER DATABASE [AdventureWorks2022] SET DISABLE_BROKER;

-- TDE certificate cleanup (run in master after DEK is dropped above)
USE [master];
GO
DROP CERTIFICATE IF EXISTS [TDE_AdventureWorks_Cert];
-- DROP MASTER KEY;  -- only if no other databases on this instance use TDE

PRINT 'Cleanup complete — all features disabled and demo objects removed.';
*/

/*
================================================================================
  FEATURE SUMMARY
================================================================================

  DATABASE LEVEL (AdventureWorks2022)
  ──────────────────────────────────────────────────────────────────────────────
  Service Broker          → ALTER DATABASE SET ENABLE_BROKER
  Change Data Capture     → sp_cdc_enable_db + sp_cdc_enable_table (3 tables)
  Change Tracking         → ALTER DATABASE + ALTER TABLE ENABLE CHANGE_TRACKING
  Query Store             → ALTER DATABASE SET QUERY_STORE = ON

  SECURITY
  ──────────────────────────────────────────────────────────────────────────────
  TDE                     → Master key + Certificate + DEK + ALTER DATABASE SET ENCRYPTION ON
  Dynamic Data Masking    → ADD MASKED on EmailAddress, PhoneNumber, CardNumber
  Row-Level Security      → Inline TVF predicate + SECURITY POLICY on SalesOrderHeader
  Always Encrypted        → Requires SSMS wizard / PowerShell for CMK generation

  SCHEMA / OBJECTS
  ──────────────────────────────────────────────────────────────────────────────
  In-Memory OLTP          → MEMORY_OPTIMIZED_DATA filegroup + ShoppingCart_InMemory table
  Temporal Tables         → ProductPriceHistory with auto-history table
  Columnstore Indexes     → NCCI on SalesOrderHeader and SalesOrderDetail
  Partitioning            → PF + PS by OrderDate year + demo partitioned table
  Graph Tables            → PersonNode, ProductNode nodes + Purchased edge
  Ledger Tables           → Updatable AuditLedger + append-only PriceChangeLog

  INFRASTRUCTURE (require external setup — see section comments)
  ──────────────────────────────────────────────────────────────────────────────
  FILESTREAM/FileTable    → OS-level config + SQL Server Configuration Manager first
  Replication             → Distributor + FULL recovery + full backup first
  Always On AG            → WSFC infrastructure + instance restart first
  Database Mirroring      → REMOVED in SQL Server 2022; use Always On AG

================================================================================
*/
