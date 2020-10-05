# sqlsir
SQL Server Smart Index Management

SQL Smart Index Management (SQLSIM) was developed as part of graduate studies project to provide another approach to index management than traditionally accepted.

SQLSIR learns how quickly an index will become fragmented and then maintain an index based on the learning.   Minimizing the time needed to be spent on scanning for fragmentation.

Implementing this solution at a client with 5TB database we were able to reduce disk I/O activities during maintenance window by 80%because we did not waste time scanning for fragmentation.

Some of the goals this project is trying to address are:

* Rolling window maintenance, therefore indexes have a fixed time to maintain an index.  If it cannot, it stops and picks up where it left off.
* Instead of scanning all indexes and then deciding which indexes to maintain; the solution takes read-write approach.  Scan one index and maintain one index; this gives disk subsystems a break between reading and writing.
* Dynamically adjusts MAXDOP between configured value and one; this adjustment is based on ALLOW_PAGE_LOCKS setting.
* Transaction log management, if transaction log of a database reaches set value (70% default), it stops maintain the index to prevent out of t-log space errors.
* Learns how quickly indexes are fragmented?
* Learns the ideal fill factor setting per index.












