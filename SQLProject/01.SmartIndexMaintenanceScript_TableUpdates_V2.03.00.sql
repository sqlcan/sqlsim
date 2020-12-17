/*
	Created On: Dec. 16, 2020.
	Issue #: 12

	Purpose: Project MaintenanceWindow table to stop the user from accidently
	         updating the Name for ID (1,2), or other propteries for ID (1).

*/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER TRIGGER dbo.tr_u_MaintenanceWindow 
   ON  dbo.MaintenanceWindow 
   AFTER UPDATE
AS 
BEGIN

	SET NOCOUNT ON;

	DECLARE @ID INT
	DECLARE @MaintenanceWindowName NVARCHAR(255)
	DECLARE @StartTime TIME
	DECLARE @EndTime TIME
	DECLARE @Weekdays VARCHAR(255)


	SELECT @ID = MaintenanceWindowID,
		   @MaintenanceWindowName = MaintenanceWindowName,
		   @StartTime = MaintenanceWindowStartTime,
		   @EndTime = MaintenanceWindowEndTime,
		   @Weekdays = MaintenanceWindowWeekdays
	  FROM INSERTED

	IF (@ID IN (1,2) AND UPDATE (MaintenanceWindowName)) OR
	   ((@ID = 1) AND UPDATE (MaintenanceWindowStartTime)) OR
	   ((@ID = 1) AND UPDATE (MaintenanceWindowEndTime)) OR
	   ((@ID = 1) AND UPDATE (MaintenanceWindowWeekdays))
	BEGIN
		RAISERROR ('Not allowed to update certain properties for ''No Maintenance'' and ''HOT Tables''.',1,1);
		ROLLBACK
	END
END
GO

CREATE OR ALTER TRIGGER dbo.tr_d_MaintenanceWindow 
   ON  dbo.MaintenanceWindow 
   AFTER DELETE
AS 
BEGIN

	SET NOCOUNT ON;

	DECLARE @ID INT


	SELECT @ID = MaintenanceWindowID
	  FROM DELETED

	IF (@ID IN (1,2))
	BEGIN
		RAISERROR ('Not allowed to delete ''No Maintenance'' and ''HOT Tables''.',1,1);
		ROLLBACK
	END
END
GO
