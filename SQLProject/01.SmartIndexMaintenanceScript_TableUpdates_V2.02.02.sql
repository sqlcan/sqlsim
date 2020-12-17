/*
	Created On: Dec. 16, 2020.
	Issue #: 13

	Purpose: Fix Spelling Mistake in Column Name

*/
SELECT MaintenanceWindowID, MaintenanceWindowName, MaintenanceWindowStartTime, 
       MaintenanceWindowEndTime, MainteanceWindowWeekdays
  INTO #TmpMaintenanceWindow
  FROM dbo.MaintenanceWindow
  
ALTER TABLE [dbo].[MasterIndexCatalog] DROP CONSTRAINT [fkMaintenanceWindowID_MasterIndexCatalog_MaintenanceWindowID]

DROP TABLE dbo.MaintenanceWindow

CREATE TABLE [dbo].[v20102MaintenanceWindow](
	[MaintenanceWindowID] [int] IDENTITY(1,1) NOT NULL,
	[MaintenanceWindowName] [nvarchar](255) NOT NULL,
	[MaintenanceWindowStartTime] [time](7) NOT NULL,
	[MaintenanceWindowEndTime] [time](7) NOT NULL,
	[MaintenanceWindowWeekdays] [varchar](255) NOT NULL,
	[MaintenanceWindowDateModifer]  AS (case when [MaintenanceWindowStartTime]>[MaintenanceWindowEndTime] then (-1) else (0) end) PERSISTED NOT NULL,
 CONSTRAINT [pkMaintenanceWindow_MaintenanceWindowID] PRIMARY KEY CLUSTERED 
(
	[MaintenanceWindowID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]

ALTER TABLE [dbo].[v20102MaintenanceWindow] ADD  CONSTRAINT [dfSTNullValue]  DEFAULT ('0:00') FOR [MaintenanceWindowStartTime]

ALTER TABLE [dbo].[v20102MaintenanceWindow] ADD  CONSTRAINT [dfETNullValue]  DEFAULT ('0:00') FOR [MaintenanceWindowEndTime]

ALTER TABLE [dbo].[v20102MaintenanceWindow] ADD  CONSTRAINT [MainteanceWindowWeekdays]  DEFAULT ('None') FOR [MaintenanceWindowWeekdays]

SET IDENTITY_INSERT dbo.[v20102MaintenanceWindow] ON
INSERT INTO [v20102MaintenanceWindow] (MaintenanceWindowID, MaintenanceWindowName, MaintenanceWindowStartTime, 
       MaintenanceWindowEndTime, [MaintenanceWindowWeekdays])
SELECT MaintenanceWindowID, MaintenanceWindowName, MaintenanceWindowStartTime, 
       MaintenanceWindowEndTime, MainteanceWindowWeekdays
  FROM #TmpMaintenanceWindow
SET IDENTITY_INSERT dbo.[v20102MaintenanceWindow] OFF

EXEC sp_rename 'v20102MaintenanceWindow', 'MaintenanceWindow'

ALTER TABLE [dbo].[MasterIndexCatalog]  WITH CHECK ADD  CONSTRAINT [fkMaintenanceWindowID_MasterIndexCatalog_MaintenanceWindowID] FOREIGN KEY([MaintenanceWindowID])
REFERENCES [dbo].[MaintenanceWindow] ([MaintenanceWindowID])

ALTER TABLE [dbo].[MasterIndexCatalog] CHECK CONSTRAINT [fkMaintenanceWindowID_MasterIndexCatalog_MaintenanceWindowID]
GO

