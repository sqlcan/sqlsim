/*
   Developed by: Mohit K. Gupta
   Avaliable at: http://www.sqlcan.com

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
-- Last Updated: Feb. 9, 2021
--
-- Version: 2.17.00
--
-- 2.00.00 Updated for Partitions and SQL 2019.
-- 2.01.00 Resolved Issue #1.
-- 2.02.00 Resolved Issue #9.
-- 2.02.01 Resovled Issue #10.
-- 2.02.02 Resovled Issue #13. -- Fixed Spelling Mistake in Column Name
--                             -- Broke up script into Tables and Procedures.
--                             -- This script contains up-to-date copy of the database.
--                             -- 
--                             -- If an older version exists, apply SmartIndexMaintenance_V2.02.02.sql first.
-- 2.03.00 Resolved Issue #12.
-- 2.04.00 Resolved Issue #11.
-- 2.04.01 Resovled Issue #16.
-- 2.05.00 Implemented #15.
-- 2.06.00 Implemented #14.
-- 2.06.01 Post Release Minor Bug Fixes.
-- 2.07.00 Implemented #18.
-- 2.12.00 Fixed various issues with @PrintOnlyNoExecute parameter. (Fixed #5).
--         - Added date time stamp if @Debug is supplied.
--         - Suspended TLOG Space Check when running in Print Only.
--         - Suspended DBCC Info Messages
--         - Removed extra white space from TSQL command.
--         - Skip mainteance window check when running in Print Mode.
-- 2.14.00 Fixed white space issue with TSQL Command.
--		   Ignoring maintenance window led to another bug where no indexes were evaluated.
--		   Fixed number of issues with MasterIndexCatalog update.
-- 2.15.00 Fixed format bug issues with PRINT.
--		   Fixed MWEndTime calculation.
--		   Added additional detail for information messsages.
--		   Fixed multiple spelling mitakes in output.
-- 2.16.00 Updated how reporting is completed for current activity.
--	       Introduced new view to summarize master catalog with last operation details.
-- 2.17.00 Updated logic for how mainteance windows are assigned (Issue #21).
-- 2.17.01 Fixed Issue #22.
--------------------------------------------------------------------------------------

USE [master]
GO

-- If database does not exist, create it with default configuration.
--
-- If database already exist, we expect all the code to respect existing
-- structures and data.  There by providing continuation for the solution
-- and protect existing data and settings.

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SQLSIM')
BEGIN
	CREATE DATABASE [SQLSIM]
	ALTER DATABASE [SQLSIM] SET RECOVERY SIMPLE 
END
GO

-- Create a new database setting the recovery model to SIMPLE.

USE [SQLSIM]
GO

-- Setup up the SQLSIM Meta-data tables used to maintain the indexes
IF OBJECT_ID('dbo.MaintenanceWindow') IS NULL
BEGIN
	CREATE TABLE dbo.MaintenanceWindow (
		MaintenanceWindowID			int							NOT NULL	IDENTITY(1,1) 
		CONSTRAINT pkMaintenanceWindow_MaintenanceWindowID	PRIMARY KEY,
		MaintenanceWindowName		nvarchar(255)				NOT NULL,
		MaintenanceWindowStartTime	time						NOT NULL
		CONSTRAINT dfSTNullValue								DEFAULT ('0:00'),
		MaintenanceWindowEndTime	time						NOT NULL
		CONSTRAINT dfETNullValue								DEFAULT ('0:00'),
		MaintenanceWindowWeekdays    varchar(255)                NOT NULL
		CONSTRAINT MaintenanceWindowWeekdays                  DEFAULT ('None'),
		[MaintenanceWindowDateModifer]  AS (CASE WHEN [MaintenanceWindowStartTime]>[MaintenanceWindowEndTime] THEN (-1) ELSE (0) END) PERSISTED NOT NULL
	);

	CREATE UNIQUE NONCLUSTERED INDEX uqMaintenanceWindowName ON dbo.MaintenanceWindow(MaintenanceWindowName);

	INSERT INTO dbo.MaintenanceWindow (MaintenanceWindowName,MaintenanceWindowStartTime,MaintenanceWindowEndTime, MaintenanceWindowWeekdays)
		VALUES ('No Maintenance','0:00','0:00','None'),
				('HOT Tables','23:00','1:00','None'),
				('Maintenance Window #1','1:00','1:45','None');
END


IF OBJECT_ID('dbo.DatabaseStatus') IS NULL	
BEGIN
	CREATE TABLE dbo.DatabaseStatus (
		DatabaseID				 int				NOT NULL,
		IsLogFileFull            bit                NOT NULL
	);
END

IF OBJECT_ID('dbo.MetaData') IS NULL
BEGIN
	CREATE TABLE dbo.MetaData (
		LastIndexUsageScanDate   datetime           NOT NULL
			CONSTRAINT dfLastIndexUsageScanDate     DEFAULT ('1900-01-01')
	);
END

IF OBJECT_ID('dbo.DatabasesToSkip') IS NULL
BEGIN	
	CREATE TABLE dbo.DatabasesToSkip (
		DatabaseName sysname NOT NULL
			CONSTRAINT pkDatabasesToSkip_DBName PRIMARY KEY);
END

IF OBJECT_ID('dbo.MasterIndexCatalog') IS NULL
BEGIN
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
		PartitionNumber			 int				NOT NULL,
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
		LastManaged				 datetime		    NULL
			CONSTRAINT dfLastManaged				DEFAULT('1900-01-01'),
		LastEvaluated            datetime           NOT NULL
			CONSTRAINT dfLastEvaluated              DEFAULT(GetDate())
	)
END

IF OBJECT_ID('dbo.MaintenanceHistory') IS NULL
BEGIN
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
END
GO