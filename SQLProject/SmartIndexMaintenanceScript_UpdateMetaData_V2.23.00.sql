/*

  This script should only need to be executed one time after release of
  version 2.23.00.  And only needs to executed on existing installs.
  New installs do not need this update.
  
*/

USE [SQLSIM]

exec sp_MSforeachdb '
 UPDATE MIC
    SET MIC.IndexType = i.type
   FROM dbo.MasterIndexCatalog MIC
   JOIN [?].sys.tables t
     ON MIC.TableID = t.object_id
	AND MIC.SchemaID = t.schema_id
	AND MIC.DatabaseID = db_id(''?'')
   JOIN [?].sys.indexes i
     ON t.object_id = i.object_id
	AND MIC.IndexID = i.index_id'