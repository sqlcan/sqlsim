# SQLSIM
SQL Server Smart Index Management

SQL Smart Index Management (SQLSIM) was developed as part of graduate studies project to provide another approach to index management than traditionally accepted.

SQLSIR learns how quickly an index will become fragmented and then maintain an index based on the learning.   Minimizing the time needed to be spent on scanning for fragmentation.

Implementing this solution at a client with 5TB database we were able to reduce disk I/O activities during maintenance window by 80% because we did not waste time scanning for fragmentation.

Some of the goals this project is trying to address are:

* Target the databases that need to be excluded, by updating the dbo.DatabasesToExclude table.
* Provides ability to selectively choose which indexes should be maintained offline and which should not be maintained at all, by updating dbo.MasterIndexCatalog.
* The toolset actively manages the fill factor for the indexes over time, in order to minimize the fragmentation generated within a single week to 10% or less.
* Maintenance window allow the DBA team to control which indexes should be maintained in which cycle.  By 1) defining the maintenance window in dbo.MaintenanceWindows and then updating 2) dbo.MasterIndexCatalog to map to the maintenance window defined.
* Indexes by default are maintained, if they have scans.  The index usage stats are tracked and saved.
* The solution learns the frequency of fragmentation, based on this learning, it decides when to scan and when to assess the index for maintenance.
* The solution performs only operations above all, it will only consider offline operation if explicitly enabled.
* Dynamic adjust the MAXDOP from one to value supplied (defaults to max value of four).  
* Monitors the transaction log, if it reaches capacity (80% default) of current size.  Maintenance is blocked for current index.
* Indexes are maintained to fit inside the maintenance Window defined.  It does statistical calculation to understand how long an operation will take to complete.
* If a new index is encountered, it will use other indexes of similar size on the server to estimate the effort.

## Deployment Guides
* Run the Solution Script.  It will create multiple tables and procedures.
* Update the dbo.MainteanceWindow to your required values.  Do not change name for "No Mainteance" and "HOT Tables".  
  * You can classify tables as "HOT Tables" which need frequently mainteance but should not be blocked due to their size.
* Create new job with three steps.
  1. Execute upUpdateMasterIndexCatalog, supply the mainteance window that you wish to default to.  If nothing it supplied it will default to "No Mainteance".
  2. Execute upUpdateIndexUsageStats.
  3. Execute upMaintainIndexes.  Provides list of multiple parameters that can be adjusted.  
      * Ignore Range Scans : By default solution only maintains indexes that have range scan.  As fragmentation has biggest impact on this component. Defaults to 0 (False).
      * PrintOnlyNoexecute : Take it for trial run assess what indexes will be maintained. Defaults to 0 (False).
      * MAXDOPSetting : What is the maximum number of processes it should use for index operations?  Defaults to 4.
      * LastOpTimeGap : Value in minutes, assess when to execute the last operation.  If I am with in 5 minutes of end-of mainteance window, it will stop the script.  Defaults to 5.
      * MaxLogSpaceUsagebeforeStop : Defaults to 80.
      * LogNOOPMsgs : Defaults to 0.  Minimizes the amount of logging in dbo.MainteanceHistory table.
      * DebugMode : Defaults to 0.  Should only be enabled in interactive mode to understand why certain indexes are not being maintained.
