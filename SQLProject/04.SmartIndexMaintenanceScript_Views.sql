USE [SQLSIM]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[vMasterIndexCatalog]
AS
WITH CTE AS (
  SELECT MasterIndexCatalogID, MAX(HistoryID) AS LastHistoryID
    FROM dbo.MaintenanceHistory
GROUP BY MasterIndexCatalogID)
  SELECT MIC.ID, MIC.DatabaseName, MIC.SchemaName, MIC.TableName, MH.Page_Count, MH.Fragmentation, MW.MaintenanceWindowName,
         MH.OperationType AS LastOperationType,
	     MH.OperationStartTime AS LastOperationStartTime,
  	     MH.OperationEndTime AS LastOperationEndTime,
		 CASE WHEN OperationEndTime = '1900-01-01 00:00:00' THEN 0 ELSE
	     DATEDIFF(S,MH.OperationStartTime, MH.OperationEndTime) END AS Duration_in_sec,
	     MH.ErrorDetails AS LastOperationErrorDetails
    FROM dbo.MasterIndexCatalog MIC
    JOIN dbo.MaintenanceHistory MH
      ON MIC.ID = MH.MasterIndexCatalogID
	JOIN dbo.MaintenanceWindow MW
	  ON MIC.MaintenanceWindowID = MW.MaintenanceWindowID
    JOIN CTE
      ON CTE.LastHistoryID = MH.HistoryID
     AND CTE.MasterIndexCatalogID = MH.MasterIndexCatalogID
GO
