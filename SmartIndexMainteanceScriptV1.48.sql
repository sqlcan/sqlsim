/*
   Developed by: Mohit K. Gupta
   Avaliable at: http://smartindex.codeplex.com
                 http://sqlcan.wordpress.com

 - COPYRIGHT NOTICE -

Microsoft Public License (Ms-PL)

This license governs use of the accompanying software. If you use the software,
you accept this license. If you do not accept the license, do not use the software.

1. Definitions

The terms "reproduce," "reproduction," "derivative works," and "distribution" have
the same meaning here as under U.S. copyright law.

A "contribution" is the original software, or any additions or changes to the software.

A "contributor" is any person that distributes its contribution under this license.

"Licensed patents" are a contributor's patent claims that read directly on its contribution.

2. Grant of Rights

(A) Copyright Grant- Subject to the terms of this license, including the license conditions
    and limitations in section 3, each contributor grants you a non-exclusive, worldwide,
    royalty-free copyright license to reproduce its contribution, prepare derivative works
    of its contribution, and distribute its contribution or any derivative works that you
    create.

(B) Patent Grant- Subject to the terms of this license, including the license conditions
    and limitations in section 3, each contributor grants you a non-exclusive, worldwide,
    royalty-free license under its licensed patents to make, have made, use, sell, offer
    for sale, import, and/or otherwise dispose of its contribution in the software or
    derivative works of the contribution in the software.

3. Conditions and Limitations

(A) No Trademark License- This license does not grant you rights to use any contributors'
    name, logo, or trademarks.

(B) If you bring a patent claim against any contributor over patents that you claim are
    infringed by the software, your patent license from such contributor to the software
    ends automatically.

(C) If you distribute any portion of the software, you must retain all copyright, patent,
    trademark, and attribution notices that are present in the software.

(D) If you distribute any portion of the software in source code form, you may do so only
    under this license by including a complete copy of this license with your distribution.
    If you distribute any portion of the software in compiled or object code form, you may
    only do so under a license that complies with this license.

(E) The software is licensed "as-is." You bear the risk of using it. The contributors give
    no express warranties, guarantees or conditions. You may have additional consumer rights
    under your local laws which this license cannot change. To the extent permitted under
    your local laws, the contributors exclude the implied warranties of merchantability,
    fitness for a particular purpose and non-infringement.

- COPYRIGHT NOTICE - */


--------------------------------------------------------------------------------------
-- Database Index Maintenance Script
--
-- Developed by: Mohit K. Gupta
--               mogupta@microsoft.com
--
-- Last Updated: February 24, 2015
--
-- Version: 1.48
--
-- 1.01 Updated all table definitions in this script to required columns, types, etc.
-- 1.02 Removed the taking the database off-line.  Because taking it off-line does not
--      remove the .mdf and .ldf. Which causes the problem with create statement
--      afterwards.
-- 1.03 Removed NOLOCK hints, as I do not think getting meta data should cause any
--      conflicts with the system.
-- 1.04 Added Index Name back into the Data Collection Procedure
--      [dbo].[upUpdateMasterIndexCatalog].
-- 1.05 Fixed the bugs in [dbo].[upUpdateMasterIndexCatalog] script; which was
--      deleting all the indexes.
-- 1.06 Fixed the bugs in dynamic code identified by dba last time (missing type
--      and missing and statement)
-- 1.07 Fixed bug where the list of columns being fetched was not same at start off
--      the loop and end of the loop for index lookups.
-- 1.08 Simplified the Create Database Statement; removing growth settings, initial
--      size, etc.
-- 1.09 Fixed table definition mistakes in MasterIndexCatalog table.
-- 1.10 Added a missing comma after i.name in upUpdateMasterIndexCatalog.
-- 1.11 Removed the object qualification in 2nd delete statement, as the parent object
--      does not have aliases.
-- 1.12 Added check for making sure index id is at least 1 or greater.
-- 1.13 Fixed the date stamps being recorded when operation completes.
-- 1.14 Fixed bug where an index potentially will be scanned in every maintenance
--      window if the result of Fragmentation Analysis is NOOP.
-- 1.15 Changed the index operation logic for indexes that were fragmented
--      between 10 - 30%; ideally in this range we should reorganize indexes.
--      However if the Allow Page locks is turned off we cannot, therefore
--      script was defaulting to rebuild online/offline.  However this is problem
--      as for indexes with even 11% fragmentation it was triggering rebuild.
--      In some large systems these indexes are in 100GB+.  Thus not desired operation, thus
--      if index is identified in this range now; desired operation is NOOP.
-- 1.16 Modified the MasterIndexCatalog Table, added fields to track the index
--      usage details, namely Index Singleton Lookups vs Index Range Scans.
-- 1.17 Created new table to track the last time the index usage stats were collected.
--      This table will be used in conjunction with the job that will capture all
--      servers index usage stats every 15 minutes and update the MasterIndexCatalog.
-- 1.18 Modified the code to maintain which have range scan, all other indexes
--      will be ignored.  In-addition indexes now will be maintained by
--      last scanned, last maintained, and largest range scans first.
-- 1.19 Added functionality to track transaction log file size.  If the transaction
--      log usage reaches capacity (defined value, defaulted at 80%); it will stop
--      maintaining all indexes for the database where it has reached capacity.
-- 1.20 Added notification under NOOP, to report back index size and fragmentation.
-- 1.21 Adjusted now the MAXDOP value is used.  If the index does not allow page locks
--      then the index rebuild operation is online however no parallelism is used.
--      For additional reference, please read blog article ...
--      http://blogs.msdn.com/b/psssql/archive/2012/09/05/
--      how-it-works-online-index-rebuild-can-cause-increased-fragmentation.aspx 
-- 1.22 Fixed minor bugs where table names and column names were miss-spelled.
-- 1.23 Fixed bug where the table to check tlog space needs to be re-created.
-- 1.24 Added functionality to handle weekdays in the maintenance windows calculations.
-- 1.25 Added functionality to skip indexes based on how frequently they become 
--      fragmented.
-- 1.26 Fixed the bug with how column types were detected to analyze if online index
--      operations are possible.
-- 1.27 Fixed object cleanup check in overall script; where wrong pros were being
--      dropped after EXSITS check.
-- 1.28 Added additional details to History Log when an index scan finishes.
-- 1.29 Fixed bug in upUpdaetIndexUsageStats; where meta data table if it did not
--      have any rows it would not collect last restart date.  Which would cause
--      the historical data to wipe and clean up.
-- 1.30 Updated the code for calculating maintenance windows based on weekdays.
-- 1.31 Fixed bug with if clause, in upMaintainIndexes; was missing a bracket and
--      was using the wrong comparatives >= vs => (incorrect).
-- 1.32 Fixed bug in LOB column check.
-- 1.33 Fixed the bug in LOB check; the OBJECTPROPERTY function can only be used with
--      local database and not target database.  Therefore changed it to use 
--      is_ms_shipped field in sys.tables catalog view instead.
-- 1.34 Fixed second bug in LOB check; where the database context was not added
--      when checking for LOB fields, therefore the check was failing.
-- 1.35 Added functionality to provide reasoning for why NOOP was chosen in the
--      history table.
-- 1.36 Adjusted the calculation for MaxSkip count; if the default date was being used
--      then the MaxSkip count would be set to 30, which is incorrect.  If the
--      index has never been maintained then max skip count should only adjust by value
--      of 1 day.
-- 1.37 If the index was skipped due to maintenance window constraints, the skip count
--      is reset to Max Skip count to so index does not get skipped next cycle.
-- 1.38 If index is disabled we do not need to do internal SmartIndex maintenance for
--      the index therefore needed to add additional checks under NOOP phase.
-- 1.39 Code added in 1.37 was added in wrong branch of the code, therefore the skip
--      count was being reset to maxskip count every time. Moved the code to correct
--      branch.
-- 1.40 Fixed the MaxSkip count, where if the date was default (1900-01-01) then it
--      would increment the skip count by 1 day, however only if MaxSkipCount was > 0
--      which it never be if its not initiated. Therefore check should be >= 0.
-- 1.41 Fixed the IsDisabled check in noop branch of the code.
-- 1.42 Updated how index skip counter works. 
-- 1.43 Updated how the default operation time is calculated.  It defaulted to
--      1 hour which was too much for small maintenance windows which are only 
--      30 minute long.
-- 1.44 Added new mechanism to handle index operation time; if index is failed to
--      maintain due to previous operation time.  It will continue to 
--      decrement previous operation time by 5% to allow for index to be maintained
--      in future maintenance windows.
-- 1.45 Fixed branch of code to allow for offline index maintenance if reorg is not
--      possible.
-- 1.46 Fixing spelling mistakes.
-- 1.47 Changed how the indexes are picked up for management.
--      Changed from MIC.LastScanned ASC, MIC.LastManaged ASC, RangeScanCount DESC to
--      LastManaged, SkipCount, RangeCount.
-- 1.48 Bug fix.  Added mainteance window constraint check at start of of index
--      management loop to prevent even an index scan operation from starting
--      in event script has reached end of mainteance cycle.
--------------------------------------------------------------------------------------

USE [master]
GO

-- If database MSIMD already exists, drop it.
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'MSIMD')
BEGIN
	DROP DATABASE MSIMD
END

-- Create a new database setting the recovery model to SIMPLE.
CREATE DATABASE [MSIMD]
GO

ALTER DATABASE [MSIMD] SET RECOVERY SIMPLE 
GO

USE [MSIMD]
GO

-- Setup up the MSIMD Meta-data tables used to maintain the indexes

CREATE TABLE dbo.MaintenanceWindow (
	MaintenanceWindowID			int							NOT NULL	IDENTITY(1,1) 
	   CONSTRAINT pkMaintenanceWindow_MaintenanceWindowID	PRIMARY KEY,
	MaintenanceWindowName		nvarchar(255)				NOT NULL,
	MaintenanceWindowStartTime	time						NOT NULL
	   CONSTRAINT dfSTNullValue								DEFAULT ('0:00'),
	MaintenanceWindowEndTime	time						NOT NULL
	   CONSTRAINT dfETNullValue								DEFAULT ('0:00'),
    MainteanceWindowWeekdays    varchar(56)                 NOT NULL,
       CONSTRAINT MainteanceWindowWeekdays                  DEFAULT ('None'),
	[MaintenanceWindowDateModifer]  AS (CASE WHEN [MaintenanceWindowStartTime]>[MaintenanceWindowEndTime] THEN (-1) ELSE (0) END) PERSISTED NOT NULL
);

INSERT INTO MaintenanceWindow (MaintenanceWindowName,MaintenanceWindowStartTime,MaintenanceWindowEndTime,MainteanceWindowWeekdays)
     VALUES ('No Maintenance','0:00','0:00','None'),
			('HOT Tables','23:00','1:00','None'),
			('Maintenance Window #1','1:00','1:45','None'),
			('Maintenance Window #2','2:30','6:30','None')

CREATE TABLE dbo.DatabaseStatus (
    DatabaseID				 int				NOT NULL,
    IsLogFileFull            bit                NOT NULL
)

CREATE TABLE dbo.MetaData (
    LastIndexUsageScanDate   datetime           NOT NULL
        CONSTRAINT dfLastIndexUsageScanDate     DEFAULT ('1900-01-01')
)

CREATE TABLE dbo.MasterIndexCatalog (
	ID						 bigint				NOT NULL	IDENTITY(1,1)
		CONSTRAINT pkMasterIndexCatalog_ID		PRIMARY KEY,
	DatabaseID				 int				NOT NULL,
	DatabaseName			 nvarchar(255)		NOT NULL,
	SchemaID				 int				NOT NULL,
	SchemaName				 nvarchar(255)		NOT NULL,
	TableID					 bigint				NOT NULL,
	TableName				 nvarchar(255)		NOT NULL,
	IndexID					 int				NOT NULL,
	IndexName				 nvarchar(255)		NOT NULL,
	IndexFillFactor			 tinyint 			NULL
		CONSTRAINT dfIndexFillFactor			DEFAULT(95),
	IsDisabled				 bit				NOT NULL
	    CONSTRAINT dfIsDisabled					DEFAULT(0),	
	IndexPageLockAllowed	 bit				NULL
		CONSTRAINT dfIsPageLockAllowed			DEFAULT(1),	
	OfflineOpsAllowed		 bit				NULL
		CONSTRAINT dfOfflineOpsAllowed			DEFAULT(0),
    RangeScanCount           bigint             NOT NULL
        CONSTRAINT dfRangeScanCount             DEFAULT(0),
    SingletonLookupCount     bigint             NOT NULL
        CONSTRAINT dfSingletonLookupCount       DEFAULT(0),
    LastRangeScanCount       bigint             NOT NULL
        CONSTRAINT dfLastRangeScanCount         DEFAULT(0),
    LastSingletonLookupCount bigint             NOT NULL
        CONSTRAINT dfLastSingletonLookupCount   DEFAULT(0),
    OnlineOpsSupported		 bit				NULL
        CONSTRAINT dfOnlineOpsSupported			DEFAULT(1),
    SkipCount                int                NOT NULL
        CONSTRAINT dfSkipCount                  DEFAULT(0),
    MaxSkipCount             int                NOT NULL
        CONSTRAINT dfMaxSkipCount               DEFAULT(0),
	MaintenanceWindowID		 int				NULL
		CONSTRAINT dfMaintenanceWindowID		DEFAULT(1)
		CONSTRAINT fkMaintenanceWindowID_MasterIndexCatalog_MaintenanceWindowID FOREIGN KEY REFERENCES MaintenanceWindow(MaintenanceWindowID),
	LastScanned				 datetime			NOT NULL
		CONSTRAINT dfLastScanned				DEFAULT('1900-01-01'),
	LastScannedTime			 int				NOT NULL
		CONSTRAINT dfLastScannedTime			DEFAULT(0),
	LastManaged				 datetime		    NULL
		CONSTRAINT dfLastManaged				DEFAULT('1900-01-01'),
	LastRebuildTime			 int				NULL
		CONSTRAINT dfLastRebuildTime			DEFAULT(0),
	LastReorgTime			 int				NULL
		CONSTRAINT dfLastReorgTime				DEFAULT(0),
    LastEvaluated            datetime           NOT NULL,
        CONSTRAINT dfLastEvaluated              DEFAULT(GetDate())
)

CREATE TABLE dbo.MaintenanceHistory (
	HistoryID				bigint					NOT NULL	IDENTITY (1,1)
		CONSTRAINT pkMaintenanceHistory_HistoryID	PRIMARY KEY,
	MasterIndexCatalogID	bigint					NOT NULL
		CONSTRAINT fkMasterIndexCatalogID_MasterIndexCatalog_ID FOREIGN KEY REFERENCES MasterIndexCatalog(ID),
	Page_Count				bigint					NOT NULL,
	Fragmentation			float					NOT NULL,
	OperationType			varchar(25)				NOT NULL,
	OperationStartTime		datetime				NOT NULL,
	OperationEndTime		datetime				NOT NULL,
	ErrorDetails			varchar(8000)			NOT NULL,
)

GO

IF EXISTS (SELECT * FROM sys.objects WHERE name = 'upUpdateMasterIndexCatalog')
    DROP PROCEDURE upUpdateMasterIndexCatalog
GO

CREATE PROCEDURE dbo.upUpdateMasterIndexCatalog
AS
BEGIN

	DECLARE @DatabaseID		int
	DECLARE @DatabaseName	nvarchar(255)
	DECLARE @SQL			varchar(8000)

	CREATE TABLE #DatabaseToManage
	(DatabaseID		int,
     DatabaseName	nvarchar(255));

	INSERT INTO #DatabaseToManage
		 SELECT database_id, name
	       FROM sys.databases
	      WHERE database_id > 4
	        AND state = 0
	        AND is_read_only = 0

    DELETE FROM dbo.DatabaseStatus
    INSERT INTO dbo.DatabaseStatus
    SELECT DatabaseID, 0
      FROM #DatabaseToManage

	DECLARE cuDatabaeScan
	 CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY
	    FOR SELECT DatabaseID, DatabaseName
	          FROM #DatabaseToManage
	
	OPEN cuDatabaeScan
	
		FETCH NEXT FROM cuDatabaeScan
		INTO @DatabaseID, @DatabaseName
	
		WHILE @@FETCH_STATUS = 0
		BEGIN
			
			SET @SQL = 'INSERT INTO dbo.MasterIndexCatalog (DatabaseID, DatabaseName, SchemaID, SchemaName, TableID, TableName, IndexID, IndexName, IndexFillFactor)
			            SELECT ' + CAST(@DatabaseID AS varchar) + ', ''' + @DatabaseName + ''', s.schema_id, s.name, t.object_id, t.name, i.index_id, i.name, i.fill_factor
						  FROM [' + @DatabaseName + '].sys.schemas s
                          JOIN [' + @DatabaseName + '].sys.tables t ON s.schema_id = t.schema_id
                          JOIN [' + @DatabaseName + '].sys.indexes i on t.object_id = i.object_id
                         WHERE i.is_hypothetical = 0
						   AND i.index_id >= 1
                           AND NOT EXISTS (SELECT *
                                             FROM dbo.MasterIndexCatalog MIC
                                            WHERE MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
                                              AND MIC.SchemaID = s.schema_id
                                              AND MIC.TableID = t.object_id
                                              AND MIC.IndexID = i.index_id)'
			
			EXEC(@SQL)
			
			SET @SQL = 'DELETE FROM MaintenanceHistory
			                  WHERE MasterIndexCatalogID
			                     IN ( SELECT ID
			                            FROM dbo.MasterIndexCatalog MIC
			                           WHERE NOT EXISTS (SELECT *
			                                               FROM [' + @DatabaseName + '].sys.schemas s
														   JOIN [' + @DatabaseName + '].sys.tables t ON s.schema_id = t.schema_id
														   JOIN [' + @DatabaseName + '].sys.indexes i on t.object_id = i.object_id
														  WHERE MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
                                                            AND MIC.SchemaID = s.schema_id
                                                            AND MIC.TableID = t.object_id
                                                            AND MIC.IndexID = i.index_id)
							             AND MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + ')'
                                              
			
			EXEC(@SQL)
			
			SET @SQL = 'DELETE FROM MasterIndexCatalog
			                  WHERE NOT EXISTS (SELECT *
			                                      FROM [' + @DatabaseName + '].sys.schemas s
							  				      JOIN [' + @DatabaseName + '].sys.tables t ON s.schema_id = t.schema_id
												  JOIN [' + @DatabaseName + '].sys.indexes i on t.object_id = i.object_id
											     WHERE DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
                                                   AND SchemaID = s.schema_id
                                                   AND TableID = t.object_id
                                                   AND IndexID = i.index_id)
	                            AND DatabaseID = ' + CAST(@DatabaseID AS varchar)
                                              
			
			EXEC(@SQL)
			
			FETCH NEXT FROM cuDatabaeScan
			INTO @DatabaseID, @DatabaseName
			
		END
		
	CLOSE cuDatabaeScan
	
	DEALLOCATE cuDatabaeScan
	
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE name = 'upUpdateIndexUsageStats')
    DROP PROCEDURE upUpdateIndexUsageStats
GO

CREATE PROCEDURE upUpdateIndexUsageStats
AS
BEGIN

    DECLARE @LastRestartDate DATETIME

    SELECT @LastRestartDate = create_date
      FROM sys.databases
     WHERE database_id = 2

    IF EXISTS (SELECT * FROM dbo.MetaData WHERE LastIndexUsageScanDate > @LastRestartDate)
    BEGIN
        -- Server has restarted since last data collection.
        UPDATE dbo.MasterIndexCatalog
           SET LastRangeScanCount = range_scan_count,
               LastSingletonLookupCount = singleton_lookup_count,
               RangeScanCount = RangeScanCount + range_scan_count,
               SingletonLookupCount = SingletonLookupCount + singleton_lookup_count
          FROM sys.dm_db_index_operational_stats(null,null,null,null) IOS
          JOIN dbo.MasterIndexCatalog MIC ON IOS.database_id = MIC.DatabaseID
                                         AND IOS.object_id = MIC.TableID
                                         AND IOS.index_id = MIC.IndexID
    END
    ELSE
    BEGIN
        -- Server did not restart since last collection.
        UPDATE dbo.MasterIndexCatalog
           SET LastRangeScanCount = LastRangeScanCount + (range_scan_count - LastRangeScanCount),
               LastSingletonLookupCount = LastSingletonLookupCount + (singleton_lookup_count - LastSingletonLookupCount),
               RangeScanCount = RangeScanCount + (range_scan_count - LastRangeScanCount),
               SingletonLookupCount = SingletonLookupCount + (singleton_lookup_count - LastSingletonLookupCount)
          FROM sys.dm_db_index_operational_stats(null,null,null,null) IOS
          JOIN dbo.MasterIndexCatalog MIC ON IOS.database_id = MIC.DatabaseID
                                         AND IOS.object_id = MIC.TableID
                                         AND IOS.index_id = MIC.IndexID

    END

    IF ((SELECT COUNT(*) FROM dbo.MetaData) = 1)
    BEGIN
        UPDATE dbo.MetaData
            SET LastIndexUsageScanDate = GetDate()
    END
    ELSE
    BEGIN
        INSERT INTO dbo.MetaData (LastIndexUsageScanDate) VALUES (GetDate())
    END

END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE name = 'upMaintainIndexes')
	DROP PROCEDURE upMaintainIndexes
GO

CREATE PROCEDURE [dbo].[upMaintainIndexes]
AS
BEGIN

    -- Start of Stored Procedure
	DECLARE @MaintenanceWindowName	    varchar(255)
	DECLARE @SQL					    varchar(8000)
	DECLARE @DatabaseID				    int
	DECLARE @DatabaseName			    nvarchar(255)
	DECLARE @SchemaName				    nvarchar(255)
	DECLARE @TableID				    bigint
	DECLARE @TableName				    nvarchar(255)
	DECLARE @IndexID				    int
	DECLARE @IndexName				    nvarchar(255)
	DECLARE @IndexFillFactor		    tinyint 
	DECLARE @IndexOperation			    varchar(25)
	DECLARE @OfflineOpsAllowed		    bit
	DECLARE @OnlineOpsSupported		    bit
	DECLARE @RebuildOnline			    bit
	DECLARE @ServerEdition			    int
	DECLARE @MWStartTime			    datetime
	DECLARE @MWEndTime				    datetime
	DECLARE @OpStartTime			    datetime
	DECLARE @OpEndTime				    datetime
	DECLARE @LastManaged			    datetime
	DECLARE @LastScanned			    datetime
    DECLARE @LastEvaluated              datetime
    DECLARE @SkipCount                  int
    DECLARE @MaxSkipCount               int
	DECLARE @MAXDOP					    int
	DECLARE @DefaultOpTime			    int
	DECLARE @FiveMinuteCheck		    int
	DECLARE @FFA					    int --Fill Factor Adjustment
    DECLARE @LogSpacePercentage         float
    DECLARE @MaxLogSpaceUsageBeforeStop float
    DECLARE @ReasonForNOOP				varchar(255)
	
	SET NOCOUNT ON

	SET @MAXDOP = 8		             -- Degree of Parallelism to use for Index Rebuilds

	SET @FiveMinuteCheck = 5*60*1000 -- When the script is with in 5 minutes of maintenance window; it will not try to run any more
	                                 --  operations.

    SET @MaxLogSpaceUsageBeforeStop = 80.0 -- This value controls when the maintenance script stops executing because the maximum log usage size has been reached.
                                           -- This value is in percentage.

    SELECT MaintenanceWindowID,
           MaintenanceWindowName,
           CASE WHEN GETDATE() > CAST(DATEADD(Day,1,CONVERT(CHAR(10),GETDATE(),111)) + ' 00:00:00.000' AS DateTime) THEN  -- If the current time is after midnight; then we need to decrement the 
              DATEADD(DAY,MaintenanceWindowDateModifer,CAST(CONVERT(CHAR(10),GETDATE(),111) + ' ' + CONVERT(CHAR(10),MaintenanceWindowStartTime,114) AS DATETIME))
           ELSE
              CAST(CONVERT(CHAR(10),GETDATE(),111) + ' ' + CONVERT(CHAR(10),MaintenanceWindowStartTime,114) AS DATETIME)
           END AS MaintenanceWindowStartTime,
           CASE WHEN MaintenanceWindowDateModifer = -1 THEN
              DATEADD(DAY,MaintenanceWindowDateModifer*-1,CAST(CONVERT(CHAR(10),GETDATE(),111) + ' ' + CONVERT(CHAR(10),MaintenanceWindowEndTime,114) AS DATETIME))
           ELSE
              CAST(CONVERT(CHAR(10),GETDATE(),111) + ' ' + CONVERT(CHAR(10),MaintenanceWindowEndTime,114) AS DATETIME)
           END AS MaintenanceWindowEndTime
      INTO #RelativeMaintenanceWindows
      FROM MaintenanceWindow
     WHERE MainteanceWindowWeekdays LIKE '%' + DATENAME(DW,GETDATE()) + '%'
 
      SELECT TOP 1 @MaintenanceWindowName = MaintenanceWindowName,
             @MWStartTime = MaintenanceWindowStartTime,
             @MWEndTime = MaintenanceWindowEndTime
        FROM #RelativeMaintenanceWindows
       WHERE MaintenanceWindowStartTime <= GETDATE() AND MaintenanceWindowEndTime >= GETDATE()
    ORDER BY MaintenanceWindowStartTime ASC

	IF (@MaintenanceWindowName IS NULL)
	BEGIN
		RETURN	
	END

    -- We need to calculate the Default Op Time, the default value in V1 was 1 HOUR (60*60*1000)
    -- However this doesn't work for small maintenance windows.  Small maintenance windows
    -- are ideal for small tables, therefore the default option on these should also be recalculated
    -- to match the small maintenance window.
    --
    -- Default Op will now assume that it will take approx 1/10 of time allocated to a maintenance
    -- window.

    SET @DefaultOpTime = DATEDIFF(MILLISECOND,@MWStartTime,@MWEndTime) / 10

    -- We are starting maintenance schedule all over therefore
    -- We'll assume the database is in health state, (i.e. transaction log is not at capacity).
    --
    -- However after the first index gets maintained we will re-check to make sure this is still valid state.
    UPDATE dbo.DatabaseStatus
       SET IsLogFileFull = 0

	SELECT @ServerEdition = CAST(SERVERPROPERTY('EngineEdition') AS int) -- 3 = Enterprise, Developer, Enterprise Eval
	
	DECLARE cuIndexList
	 CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY
	    FOR SELECT DatabaseID, DatabaseName, SchemaName, TableID, TableName, IndexID, IndexName, IndexFillFactor, OfflineOpsAllowed, LastManaged, LastScanned, LastEvaluated, SkipCount, MaxSkipCount
	          FROM dbo.MasterIndexCatalog MIC
	          JOIN dbo.MaintenanceWindow  MW   ON MIC.MaintenanceWindowID = MW.MaintenanceWindowID
	         WHERE MW.MaintenanceWindowName = @MaintenanceWindowName
               AND MIC.RangeScanCount > 0
          ORDER BY MIC.LastManaged ASC, MIC.SkipCount ASC, RangeScanCount DESC
	
	OPEN cuIndexList
	
		FETCH NEXT FROM cuIndexList
		INTO @DatabaseID, @DatabaseName, @SchemaName, @TableID, @TableName, @IndexID, @IndexName, @IndexFillFactor, @OfflineOpsAllowed, @LastManaged, @LastScanned, @LastEvaluated, @SkipCount, @MaxSkipCount
		
		WHILE @@FETCH_STATUS = 0
		BEGIN  -- START -- CURSOR

            -- Only manage the current index if current database's tlog is not full, index skip count has been reached
            -- and there is still time in maintenance window.
            IF (((NOT EXISTS (SELECT * FROM dbo.DatabaseStatus WHERE DatabaseID = @DatabaseID AND IsLogFileFull = 1)) AND
                             (@SkipCount >= @MaxSkipCount)) AND
                             ((DATEADD(MILLISECOND,@FiveMinuteCheck,GETDATE())) < @MWEndTime))
            BEGIN -- START -- Maintain Indexes for Databases where TLog is not Full.

			    SET @IndexOperation = 'NOOP'      --No Operation
				SET @ReasonForNOOP = 'No Reason.' --Default value.
			    SET @RebuildOnline = 1		      --If rebuild is going to execute it should be online.

			    SET @SQL = 'UPDATE dbo.MasterIndexCatalog
			                   SET IsDisabled = i.is_disabled,
			                       IndexPageLockAllowed = i.allow_page_locks
			                  FROM dbo.MasterIndexCatalog MIC
			                  JOIN [' + @DatabaseName + '].sys.indexes i
			                    ON MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
			                   AND MIC.TableID = i.object_id 
			                   AND MIC.IndexID = i.index_id
                             WHERE i.object_id = ' + CAST(@TableID AS varchar) + '
                               AND i.index_id = ' + CAST(@IndexID AS varchar) 
                               
			    EXEC (@SQL)

			    DECLARE @IsDisabled				BIT
			    DECLARE @IndexPageLockAllowed	BIT

			    SELECT @IsDisabled = IsDisabled, @IndexPageLockAllowed = IndexPageLockAllowed
			      FROM dbo.MasterIndexCatalog MIC
			     WHERE MIC.DatabaseID = @DatabaseID
			       AND MIC.TableID = @TableID
			       AND MIC.IndexID = @IndexID

                -- Since it is not skipped; the skip counter is reinitialized to 0.
				UPDATE dbo.MasterIndexCatalog
				   SET SkipCount = 0,
                       LastEvaluated = GetDate()
				 WHERE DatabaseID = @DatabaseID
				   AND TableID = @TableID
				   AND IndexID = @IndexID 
			 
			    IF (@IsDisabled = 0)
			    BEGIN -- START -- Decide on Index Operation
	
				    DECLARE @FragmentationLevel float
				    DECLARE @PageCount			bigint
				
				    SET @OpStartTime = GETDATE()
				
				    SELECT @FragmentationLevel = avg_fragmentation_in_percent, @PageCount = page_count
				      FROM sys.dm_db_index_physical_stats(@DatabaseID,@TableID,@IndexID,null,'LIMITED')

				    SET @OpEndTime = GETDATE()
				
                    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
                    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'FragScan', @OpStartTime, @OpEndTime, 'Index fragmentation scan completed; fragmentation level.'
                     FROM dbo.MasterIndexCatalog MIC
                    WHERE MIC.DatabaseID = @DatabaseID
                        AND MIC.TableID = @TableID
                        AND MIC.IndexID = @IndexID

				    UPDATE dbo.MasterIndexCatalog
				       SET LastScanned = @OpEndTime,
				           LastScannedTime = DATEDIFF(MILLISECOND,@OpStartTime,@OpEndTime)
				     WHERE DatabaseID = @DatabaseID
				       AND TableID = @TableID
				       AND IndexID = @IndexID 
				
				    -- If fragmentation level is less then 10 we do not need to look at the index
				    -- does not matter if it is hot or other.
				    IF ((@FragmentationLevel >= 10.0) AND (@PageCount > 64))
				    BEGIN
				
					    -- Evaluate if the index supports online operations or not.

                        -- Lob Column Types
                        -- image, ntext, text, varchar(max), nvarchar(max), varbinary(max), and xml. 

                        IF (@IndexID > 1)
                        BEGIN

                            -- A non-clustered index can be built online as long as there are no
                            -- lob column times in definition or include type.

                            SET @SQL = 'DECLARE @RowsFound int
                             
                                        SELECT @RowsFound = COUNT(*)
									      FROM [' + @DatabaseName + '].sys.indexes i
                                          JOIN [' + @DatabaseName + '].sys.index_columns ic
                                            ON i.object_id = ic.object_id
                                           AND i.index_id = ic.index_id
                                          JOIN [' + @DatabaseName + '].sys.columns c
                                            ON ic.column_id = c.column_id
                                           AND ic.object_id = c.object_id
                                         WHERE (   (     c.max_length = -1
                                                     AND c.system_type_id IN (167,231,165,241))
                                                OR ( c.system_type_id IN (34,35,99)))
                                           AND i.object_id = ' + CAST(@TableID AS varchar) + '
                                           AND i.index_id = ' + CAST(@IndexID AS varchar) 
                        END
                        ELSE
                        BEGIN

                            -- A cluster index can only be online if there are no lob column types
                            -- in underline table definition.

                            SET @SQL = 'DECLARE @RowsFound int
                                       
                                       SELECT @RowsFound = COUNT(*)
									     FROM [' + @DatabaseName + '].sys.indexes i
                                         JOIN [' + @DatabaseName + '].sys.tables t
                                           ON i.object_id = t.object_id
                                         JOIN [' + @DatabaseName + '].sys.columns c
                                          ON t.object_id = c.object_id
                                       WHERE i.index_id = 1
                                         AND (   (     c.max_length = -1
                                                   AND c.system_type_id IN (167,231,165,241))
                                              OR ( c.system_type_id IN (34,35,99)))
                                          AND i.object_id = ' + CAST(@TableID AS varchar)
                        END

					    SET @SQL = @SQL + '
									   
								    IF (@RowsFound > 0)
								    BEGIN
									
									    UPDATE dbo.MasterIndexCatalog
										   SET OnlineOpsSupported = 0
										  FROM dbo.MasterIndexCatalog MIC
	                                      JOIN [' + @DatabaseName + '].sys.indexes i (NOLOCK)
										    ON MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
										   AND MIC.TableID = i.object_id 
										   AND MIC.IndexID = i.index_id
										 WHERE i.object_id = ' + CAST(@TableID AS varchar) + '
										   AND i.index_id = ' + CAST(@IndexID AS varchar) + '
											   
								    END
								    ELSE
								    BEGIN
									
									    UPDATE dbo.MasterIndexCatalog
										   SET OnlineOpsSupported = 1
										  FROM dbo.MasterIndexCatalog MIC
	                                      JOIN [' + @DatabaseName + '].sys.indexes i (NOLOCK)
										    ON MIC.DatabaseID = ' + CAST(@DatabaseID AS varchar) + '
										   AND MIC.TableID = i.object_id 
										   AND MIC.IndexID = i.index_id
										 WHERE i.object_id = ' + CAST(@TableID AS varchar) + '
										   AND i.index_id = ' + CAST(@IndexID AS varchar) + '
											   
								    END
								    '

					    EXEC (@SQL)
					
					    SELECT @OnlineOpsSupported = OnlineOpsSupported
					      FROM dbo.MasterIndexCatalog MIC
					     WHERE MIC.DatabaseID = @DatabaseID
					       AND MIC.TableID = @TableID
					       AND MIC.IndexID = @IndexID					
							
					    -- Index has some fragmentation and is at least 64 pages.  So we want to evaluate
					    -- if it should be maintained or not.  If it is HOT Table, it should be
					    -- maintained; however if it is not HOT Table, then it will only be maintained
					    -- if it has at least 1000 pages.
					    IF ((@MaintenanceWindowName = 'HOT Tables') OR
				            ((@MaintenanceWindowName <> 'HOT Tables') AND (@PageCount >= 1000)))
					    BEGIN
					
						    -- Either it is a hot index with 64 pages or index has at least 1000
						    -- pages and the fragmentation needs to be addressed.
						    IF ((@FragmentationLevel < 30.0) AND (@IndexPageLockAllowed = 1))
						    BEGIN
						
							    SET @IndexOperation = 'REORGANIZE'
						
						    END
						    ELSE
						    BEGIN
							
							    IF ((@FragmentationLevel < 30.0) AND (@IndexPageLockAllowed = 0))
							    BEGIN
							
								    -- Index Organization is not allowed because page lock is not allowed for the index.
								    -- Therefore only option is to rebuild the index, however to rebuild index online
								    -- online operations must be supported.
							
								    IF (((@OnlineOpsSupported = 0) OR (@ServerEdition <> 3)) AND (@OfflineOpsAllowed = 0))
								    BEGIN
									    -- Online operation not supported by table or edition.
									    -- However offline operations are not allowed and table cannot be
									    -- Reorganized because Page Locks are not allowed.
									
									    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
									    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'ERROR', GETDATE(), GETDATE(),
									           'Failed to maintain index because Online not supported, offline not allowed, and page locks not allowed to do reorganize online.'
                                          FROM dbo.MasterIndexCatalog MIC
								         WHERE MIC.DatabaseID = @DatabaseID
								           AND MIC.TableID = @TableID
								           AND MIC.IndexID = @IndexID 
								       
										SET @ReasonForNOOP = 'Error in index maintenance, please reference additional details in history log.'
										
								    END
								    ELSE
								    BEGIN

									    IF (((@OnlineOpsSupported = 0) OR (@ServerEdition <> 3)) AND (@OfflineOpsAllowed = 1))
									    BEGIN
										    SET @IndexOperation = 'REBUILD'
										    SET @RebuildOnline = 0
									    END
                                        ELSE
                                        BEGIN
                                            SET @IndexOperation = 'NOOP'
                                            SET @ReasonForNOOP = 'Index does not support index reorganization or offline index rebuild however fragmentation has not reached critical point (30%+) to rebuild.'
                                        END

                                        -- Apr. 25, 2014 - this functionality is being removed.  Systems which do not allow reorganize
                                        --                 can be rebuild to manage fragmentation, however we do not want to manage
                                        --                 the fragmentation at a low value.  Having low fragmentation
                                        --                 this functionality was still triggering a rebuild.  Which is costly
                                        --                 operation for large indexes. Replaced with code below, i.e.
                                        --                 Index Operation = NOOP.

                                        /*
									    ELSE
									    BEGIN
									
										    IF ((@OnlineOpsSupported = 1) AND (@ServerEdition = 3) AND (@OfflineOpsAllowed = 0))
										    BEGIN
											    SET @IndexOperation = 'REBUILD'
											    SET @RebuildOnline = 1
										    END
										
									    END
                                        */
								
								    END
								
							    END
							    ELSE
							    BEGIN
							
								    -- If script came to this phase; then it must mean the fragmentation is
								    -- higher then 30%.  Therefore index must be rebuilt.
								       
								    IF ((@OnlineOpsSupported = 1) AND (@ServerEdition = 3))
								    BEGIN
									    SET @IndexOperation = 'REBUILD'
									    SET @RebuildOnline = 1
								    END
								    ELSE
								    BEGIN
									    -- Online operations are not supported by the table or edition.
									
									    IF (@OfflineOpsAllowed = 1)
									    BEGIN
										    SET @IndexOperation = 'REBUILD'
										    SET @RebuildOnline = 0
									    END
									    ELSE
									    BEGIN
									
										    IF (@IndexPageLockAllowed = 1)
										    BEGIN
											    SET @IndexOperation = 'REORGANIZE'
										    END
										    ELSE
										    BEGIN
											    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
											    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'ERROR', GETDATE(), GETDATE(),
												       'Failed to maintain index because Online not supported, offline not allowed, and page locks not allowed to do reorganize online.'
											      FROM dbo.MasterIndexCatalog MIC
											     WHERE MIC.DatabaseID = @DatabaseID
											       AND MIC.TableID = @TableID
											       AND MIC.IndexID = @IndexID 
											       
												SET @ReasonForNOOP = 'Error in index maintenance, please reference additional details in history log.'
										    END
										
									    END
									
								    END
								
							    END
							
						    END

					    END
					    ELSE
					    BEGIN
							SET @ReasonForNOOP = 'Small table not part of HOT maintenance window.'
						END 

				    END
				    ELSE
				    BEGIN
						SET @ReasonForNOOP = 'Small table (less then 64KB) or low fragmentation (less then 10%).'
					END
								
			    END -- END -- Decide on Index Operation
				ELSE
				BEGIN -- START -- Index is disabled just record reason for NOOP
					SET @ReasonForNOOP = 'Index disabled.'
				END -- END -- Index is disabled just record reason for NOOP
				
			    IF (@IndexOperation <> 'NOOP')
			    BEGIN -- START -- Calculate and Execute Index Operation
				
				    -- Decisions around Index Operation has been made; therefore its time to do the actual work.
				    -- However before we can execute we must evaluate the maintenance window requirements.
				
				    DECLARE @IndexReorgTime		int
				    DECLARE @IndexRebuildTime	int
				    DECLARE @OpTime				int
				    DECLARE @EstOpEndTime		datetime
				 
				    SELECT @IndexReorgTime = LastReorgTime, @IndexRebuildTime = LastRebuildTime
				      FROM dbo.MasterIndexCatalog MIC
				     WHERE MIC.DatabaseID = @DatabaseID
				       AND MIC.TableID = @TableID
				       AND MIC.IndexID = @IndexID
			
				    -- If the index has never been maintained the last reorg / rebuild time will
				    -- be zero.  In those cases we will assume the index operation will take
				    -- approximately 1 hour to complete.
				
				    IF (@IndexReorgTime = 0)
					    SET @IndexReorgTime = @DefaultOpTime
					
				    IF (@IndexRebuildTime = 0)
					    SET @IndexRebuildTime = @DefaultOpTime
					
				    IF (@IndexOperation = 'REORGANIZE')
					    SET @OpTime = @IndexReorgTime
				    ELSE
					    SET @OpTime = @IndexRebuildTime
				
				    SET @EstOpEndTime = DATEADD(MILLISECOND,@OpTime,GETDATE())
				
				    -- Confirm operation will complete before the Maintenance Window End Time.
				    IF (@EstOpEndTime < @MWEndTime)
				    BEGIN
				    
						-- Index is being maintained so we will decrement the MaxSkipCount by 1; minimum value is 0.
						UPDATE dbo.MasterIndexCatalog
						   SET MaxSkipCount = CASE WHEN (@LastManaged = '1900-01-01 00:00:00.000') AND @MaxSkipCount > 0 THEN @MaxSkipCount - 1
												   WHEN (@LastManaged = '1900-01-01 00:00:00.000') AND @MaxSkipCount < 0 THEN 0
												   WHEN (@MaxSkipCount - DATEDIFF(DAY,@LastManaged,GETDATE()) < 1) THEN 0
												   ELSE @MaxSkipCount - DATEDIFF(DAY,@LastManaged,GETDATE()) END
						 WHERE DatabaseID = @DatabaseID
						   AND TableID = @TableID
						   AND IndexID = @IndexID 
					
					    SET @SQL = 'USE [' + @DatabaseName + ']
								    ALTER INDEX [' + @IndexName + ']
								    ON [' + @SchemaName + '].[' + @TableName + '] '
					            
					    IF (@IndexOperation = 'REORGANIZE')
					    BEGIN
						    SET @SQL = @SQL + 
								       ' REORGANIZE'
					    END
					    ELSE
					    BEGIN

						    IF (@IndexFillFactor = 0)
						    BEGIN
							    SET @IndexFillFactor = 95
							    SET @FFA = 0
						    END
						    ELSE
						    BEGIN

                                -- Adjust fill factor by 0 to 15%; for each day it didn't get maintained
                                -- it will adjust fill factor by smaller number.
                                --
                                -- e.g. If index was maintained just yesterday; it'll adjust it by 15%
                                --      If index was maintained 8 days ago; it will adjust it by 1%
                                --      If index was maintained 9+ days ago; it will adjust it by 0%

							    SET @FFA = ((8-DATEDIFF(DAY,@LastManaged,GETDATE()))*2)+1

							    IF (@FFA < 1)
							       SET @FFA = 0

							    IF (@FFA > 15)
							       SET @FFA = 15
						    END
								
						    SET @IndexFillFactor = @IndexFillFactor - @FFA
								
						    IF (@IndexFillFactor < 70)
						    BEGIN
							    SET @IndexFillFactor = 70
							    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
							    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'WARNING', GETDATE(), GETDATE(),
									    'Index fill factor is dropping below 70%.  Please evaluate if the index is using a wide key, which might be causing excessive fragmentation.'
								    FROM dbo.MasterIndexCatalog MIC
								    WHERE MIC.DatabaseID = @DatabaseID
								    AND MIC.TableID = @TableID
								    AND MIC.IndexID = @IndexID 
						    END
								
						    UPDATE dbo.MasterIndexCatalog
							   SET IndexFillFactor = @IndexFillFactor
							 WHERE DatabaseID = @DatabaseID
							   AND TableID = @TableID
							   AND IndexID = @IndexID 

						    SET @SQL = @SQL + 
								       ' REBUILD 
									     WITH (FILLFACTOR = ' + CAST(@IndexFillFactor AS VARCHAR) + ', 
									     SORT_IN_TEMPDB = ON,'


						    IF (@RebuildOnline = 1)
						    BEGIN
							    SET @SQL = @SQL + 
                                       ' MAXDOP = ' + CASE WHEN @IndexPageLockAllowed = 0 THEN '1' ELSE CAST(@MAXDOP AS VARCHAR) END + ', ' +
								       ' ONLINE = ON'
						    END
                            ELSE
                            BEGIN
							    SET @SQL = @SQL + 
                                       ' MAXDOP = ' + CAST(@MAXDOP AS VARCHAR)
                            END

						    SET @SQL = @SQL + ');'
					
					    END
					
					    SET @OpStartTime = GETDATE()
					
					    EXEC (@SQL)
					
					    SET @OpEndTime = GETDATE()
					
					    UPDATE dbo.MasterIndexCatalog
					       SET LastManaged = @OpEndTime
					     WHERE DatabaseID = @DatabaseID
					       AND TableID = @TableID
					       AND IndexID = @IndexID  
				
				
					    IF (@IndexOperation = 'REORGANIZE')
					    BEGIN
						    UPDATE dbo.MasterIndexCatalog
						       SET LastReorgTime = DATEDIFF(MILLISECOND,@OpStartTime,@OpEndTime)
						     WHERE DatabaseID = @DatabaseID
						       AND TableID = @TableID
						       AND IndexID = @IndexID  
					    END
					    ELSE
					    BEGIN
						    UPDATE dbo.MasterIndexCatalog
						       SET LastRebuildTime = DATEDIFF(MILLISECOND,@OpStartTime,@OpEndTime)
						     WHERE DatabaseID = @DatabaseID
						       AND TableID = @TableID
						       AND IndexID = @IndexID  
					    END
					
					    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
					    SELECT MIC.ID,
					           @PageCount,
						       @FragmentationLevel,
						       CASE WHEN @RebuildOnline = 1 THEN
						          @IndexOperation + ' (ONLINE)'
						       ELSE
						          @IndexOperation + ' (OFFLINE)'
						       END, @OpStartTime, @OpEndTime,'Completed. Command executed (' + @SQL + ')'
					      FROM dbo.MasterIndexCatalog MIC
					     WHERE MIC.DatabaseID = @DatabaseID
					       AND MIC.TableID = @TableID
					       AND MIC.IndexID = @IndexID 
				
                        -- Check to make sure the transaction log file on the current database is not full.
                        -- If the transaction log file is full, we cannot maintain any more indexes for current database.

                        IF EXISTS (SELECT * FROM tempdb.sys.all_objects WHERE name LIKE '#TLogSpace%')
                            DELETE FROM #TLogSpace
                        ELSE
                            CREATE TABLE #TLogSpace (DBName sysname, LogSize float, LogSpaceUsed float, LogStatus smallint)

                        INSERT INTO #TLogSpace
                        EXEC ('DBCC SQLPERF(LOGSPACE)')

                        SELECT @LogSpacePercentage = LogSpaceUsed
                          FROM #TLogSpace
                         WHERE DBName = db_name(@DatabaseID)

                        IF (@LogSpacePercentage > @MaxLogSpaceUsageBeforeStop)
                        BEGIN
						    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
						    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'WARNING', GETDATE(), GETDATE(),
						           'Database reached Max Log Space Usage limit, therefore no further indexes will be maintained in this maintenance window current database.'
                              FROM dbo.MasterIndexCatalog MIC
					         WHERE MIC.DatabaseID = @DatabaseID
					           AND MIC.TableID = @TableID
					           AND MIC.IndexID = @IndexID 

                            UPDATE dbo.DatabaseStatus
                               SET IsLogFileFull = 1
                             WHERE DatabaseID = @DatabaseID
                        END

				    END
				    ELSE
				    BEGIN -- BEING -- Index Skipped due to Maintenance Window constraint
				
					    IF (@LastManaged < DATEADD(DAY,-14,GETDATE()))
					    BEGIN

                            IF (@OpTime > 0)
                            BEGIN -- BEGIN -- Operation Time Adjustment

                                -- Since the index was skipped due to maintenance window constraint, we are going to decrement
                                -- the time it takes to maintain the said index by 5% of it's current cost.
                                -- 
                                -- Current version of SmartIndexManagement does not have statistical analysis of the
                                -- operation times.  Therefore last value is used.  However what if last operation took
                                -- extended time due to blocking, or other factors.  It will plague the index forever
                                -- without decrementing the cost.

                                SET @OpTime = @OpTime - (@OpTime * 0.05)

                                IF (@OpTime <= 0)
                                    SET @OpTime = 1000 -- If the Op Time falls below or equip to zero; we'll reinitialize to 1 Second.

					            IF (@IndexOperation = 'REORGANIZE')
					            BEGIN
						            UPDATE dbo.MasterIndexCatalog
						                SET LastReorgTime = @OpTime
						                WHERE DatabaseID = @DatabaseID
						                AND TableID = @TableID
						                AND IndexID = @IndexID  
					            END
					            ELSE
					            BEGIN
						            UPDATE dbo.MasterIndexCatalog
						                SET LastRebuildTime = @OpTime
						                WHERE DatabaseID = @DatabaseID
						                AND TableID = @TableID
						                AND IndexID = @IndexID  
					            END

                            END -- END -- Operation Time Adjustment
										
						    INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
						    SELECT MIC.ID, @PageCount, @FragmentationLevel, 'WARNING', GETDATE(), GETDATE(),
						           'Index has not been managed in last 14 day due to maintenance window constraint.'
                              FROM dbo.MasterIndexCatalog MIC
					         WHERE MIC.DatabaseID = @DatabaseID
					           AND MIC.TableID = @TableID
					           AND MIC.IndexID = @IndexID 
								       
					    END

						-- Index was skipped due to maintenance window constraints.
                        -- i.e. if this index was to be maintained based on previous history it would go past the
                        -- maintenance window threshold.  Therefore it was skipped.  However if it is maintained
                        -- at start of maintenance window it should get maintained next cycle.

						UPDATE dbo.MasterIndexCatalog
						   SET SkipCount = @MaxSkipCount
						 WHERE DatabaseID = @DatabaseID
						   AND TableID = @TableID
						   AND IndexID = @IndexID 
					
					    SET @EstOpEndTime = DATEADD(MILLISECOND,@FiveMinuteCheck,GETDATE())
					
					    -- We have reached the end of mainteance window therefore
					    -- we do not want to maintain any additional indexes.
					    IF (@EstOpEndTime > @MWEndTime)
						    RETURN
					
				    END -- END -- Index Skipped due to Maintenance Window constraint
			    END -- END -- Calculate and Execute Index Operation
			    ELSE
			    BEGIN -- START -- No Operation for current Index.
			    
					-- If index is not disabled we need to do some calculation regarding FFA, because if NOOP was chosen
					-- for an active index, it means it is not fragmented therefore we can adjust the Fill Factor setting 
					-- to better tune it for next time it becomes fragmented.
					--
					-- However if index is disabled we do not need to do anything just record it in history table the state
					-- and reason for NOOP.
					
					IF (@IsDisabled = 0)
					BEGIN -- START -- No Operation for current index and it is not disabled
					
						IF (@IndexFillFactor = 0)
						BEGIN
							SET @IndexFillFactor = 95
							SET @FFA = 0
						END
						ELSE
						BEGIN
							SET @FFA = DATEDIFF(DAY,@LastScanned,Getdate())

							IF (@FFA < 1)
								SET @FFA = 1

							IF (@FFA > 5)
								SET @FFA = 5
						END
									
						SET @IndexFillFactor = @IndexFillFactor + @FFA
									
						IF (@IndexFillFactor > 99)
							SET @IndexFillFactor = 99
									
						UPDATE dbo.MasterIndexCatalog
						   SET IndexFillFactor = @IndexFillFactor,
							   MaxSkipCount = CASE WHEN (@LastScanned = '1900-01-01 00:00:00.000') AND @MaxSkipCount >= 0 THEN @MaxSkipCount + 1
												   WHEN (@LastScanned = '1900-01-01 00:00:00.000') AND @MaxSkipCount < 0 THEN 0
												   WHEN (@MaxSkipCount + DATEDIFF(DAY,@LastScanned,GetDate()) > 30) THEN 30
												   ELSE @MaxSkipCount + DATEDIFF(DAY,@LastScanned,GetDate()) END
						 WHERE DatabaseID = @DatabaseID
						   AND TableID = @TableID
						   AND IndexID = @IndexID

					END -- END -- No Operation for current index and it is not disabled

					INSERT INTO dbo.MaintenanceHistory (MasterIndexCatalogID, Page_Count, Fragmentation, OperationType, OperationStartTime, OperationEndTime, ErrorDetails)
					SELECT MIC.ID,
							@PageCount,
							@FragmentationLevel,
							'NOOP', @OpStartTime, @OpEndTime, @ReasonForNOOP
						FROM dbo.MasterIndexCatalog MIC
						WHERE MIC.DatabaseID = @DatabaseID
						AND MIC.TableID = @TableID
						AND MIC.IndexID = @IndexID 
							
			    END -- END -- No Operation for current Index.
			
            END -- END -- Maintain Indexes for Databases where TLog is not Full.
            ELSE
            BEGIN -- START -- Either TLog is Full or Skip Count has not reached Max Skip Count or We are out of time!

                -- There is no operation to execute if database TLog is full.  However if 
                -- skip count has not been reached.  We must increment Skip Count for next time.
                --
                -- However if Database TLog is full then the index in fact did not get skipped, it got ignored.
                -- Therefore skip counter should not be adjusted; neither should the last evaluated date
                -- as index was not evaluated due to tlog being full.

                IF ((NOT EXISTS (SELECT * FROM dbo.DatabaseStatus WHERE DatabaseID = @DatabaseID AND IsLogFileFull = 1)) AND
                    (DATEADD(MILLISECOND,@FiveMinuteCheck,GETDATE())) < @MWEndTime)
                BEGIN -- START -- Database T-Log Is Not Full And We Are Not Out Of Time; i.e. Index was skipped due to skip count.
                    IF (@SkipCount <= @MaxSkipCount)
                    BEGIN -- START -- Increment Skip Count

				        UPDATE dbo.MasterIndexCatalog
				           SET SkipCount = @SkipCount + DATEDIFF(DAY,@LastEvaluated,GetDate()),
                               LastEvaluated = GetDate()
				         WHERE DatabaseID = @DatabaseID
				           AND TableID = @TableID
				           AND IndexID = @IndexID 

                    END -- END -- Increment Skip Count
                END -- END -- Database T-Log Is Not Full And We Are Not Out Of Time
                ELSE
                BEGIN
                    IF ((NOT EXISTS (SELECT * FROM dbo.DatabaseStatus WHERE DatabaseID = @DatabaseID AND IsLogFileFull = 1)) AND
                        (DATEADD(MILLISECOND,@FiveMinuteCheck,GETDATE())) > @MWEndTime)
                    BEGIN -- START -- Database T-Log Is Not Full But We Are Out Of Time
                        RETURN
                    END -- END -- Database T-Log Is Not Full But We Are Out Of Time
                END
            END -- END -- Either TLog is Full or Skip Count has not reached Max Skip Count

			FETCH NEXT FROM cuIndexList
			INTO @DatabaseID, @DatabaseName, @SchemaName, @TableID, @TableName, @IndexID, @IndexName, @IndexFillFactor, @OfflineOpsAllowed, @LastManaged, @LastScanned, @LastEvaluated, @SkipCount, @MaxSkipCount
		
		END -- END -- CURSOR
		
	CLOSE cuIndexList
	
	DEALLOCATE cuIndexList
	-- End of Stored Procedure

END
GO