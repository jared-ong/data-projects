--Quick overview
 
--Find all open transactions on all databases, use the session_id and sysprocesses or sp_who2 to locate which database
SELECT 'Long running transactions' as description,
       CAST(DATEDIFF(minute, transaction_begin_time, getUTCDate()) as numeric(17,2)) as MinutesOpen,
       'dbcc inputbuffer(' + cast(st.session_id as varchar(20))+ ')' as inputbuffer, at.transaction_id,
       st.session_id,
       s.hostname,
       s.program_name,
       s.loginame,
       at.transaction_id,
       at.name as transaction_name,
       at.transaction_begin_time,
       s.login_time,
       s.last_batch,
       at.transaction_type,
       CASE(at.transaction_type) WHEN 1 THEN 'Read/Write' WHEN 2 THEN 'Read-only' WHEN 3 THEN 'System transaction' WHEN 4 THEN 'Distributed transaction' ELSE NULL END AS transaction_type_desc,
       CASE(at.transaction_state) WHEN 0 THEN 'The transaction has not been completely initialized yet.' WHEN 1 THEN 'The transaction has been initialized but has not started.' WHEN 2 THEN 'The transaction is active.' WHEN 3 THEN 'The transaction has ended. This is used for read-only transactions.' WHEN 4 THEN 'The commit process has been initiated on the distributed transaction. This is for distributed transactions only. The distributed transaction is still active but further processing cannot take place.' WHEN 5 THEN 'The transaction is in a prepared state and waiting resolution.' WHEN 6 THEN 'The transaction has been committed.' WHEN 7 THEN 'The transaction is being rolled back.' WHEN 8 THEN 'The transaction has been rolled back.' ELSE NULL END AS transaction_state_desc
FROM sys.dm_tran_active_transactions at
INNER JOIN sys.dm_tran_session_transactions st
    ON at.transaction_id = st.transaction_id
INNER JOIN sys.sysprocesses s on st.session_id = s.spid
ORDER BY transaction_begin_time
  
  
--Query to locate currently executing SQL jobs and when they started
SELECT
    'Currently executing SQL Jobs' as description,
    CAST(DATEDIFF(minute, ja.start_execution_date, getUTCDate()) as numeric(17,2)) as MinutesRunning,
    j.name AS job_name,
    ISNULL(last_executed_step_id,0)+1 AS current_executing_step_id,
    Js.step_name,
    ja.job_id,
    ja.start_execution_date    
FROM msdb.dbo.sysjobactivity ja
LEFT JOIN msdb.dbo.sysjobhistory jh
    ON ja.job_history_id = jh.instance_id
JOIN msdb.dbo.sysjobs j
    ON ja.job_id = j.job_id
JOIN msdb.dbo.sysjobsteps js
    ON ja.job_id = js.job_id
    AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
AND j.category_id <> 10 --Filters replication jobs because they are always running
AND start_execution_date is not null
AND stop_execution_date is null
ORDER BY CAST(DATEDIFF(minute, ja.start_execution_date, getUTCDate()) as numeric(17,2)) DESC;
  
--Check if current backup or restore jobs are running
SELECT 'Backup/Restore jobs running' as description, session_id as SPID, command, aa.text AS Query, start_time,percent_complete,
dateadd(second,estimated_completion_time/1000, getdate()) asestimated_completion_time
FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) aa
WHERE r.command in('BACKUP DATABASE','RESTORE DATABASE')
-- MEMORY: Check buffer cache hit ratio.  Usually good over 90%
SELECT 'Memory Buffer Cach Hit Ratio' as description,
(a.cntr_value * 1.0 / b.cntr_value) * 100.0 as BufferCacheHitRatio
FROM sys.dm_os_performance_counters  a
JOIN  (SELECT cntr_value,OBJECT_NAME
    FROM sys.dm_os_performance_counters
    WHERE counter_name = 'Buffer cache hit ratio base'
        AND OBJECT_NAME = 'SQLServer:Buffer Manager') b ON  a.OBJECT_NAME = b.OBJECT_NAME
WHERE a.counter_name = 'Buffer cache hit ratio'
AND a.OBJECT_NAME = 'SQLServer:Buffer Manager'
  
  
-- MEMORY: Check Page Life Expectancy.  Usually good over 300.
SELECT 'Memory Page Life Expectancy' as description,
[object_name],
[counter_name],
[cntr_value]
FROM sys.dm_os_performance_counters
WHERE [object_name] LIKE '%Manager%'
AND [counter_name] = 'Page life expectancy'
  
  
--DISK I/O: If Disk I/O 100% look at which databases are responsible for the read/writes
SELECT 'Disk I/O Which Databases' as description,
name AS 'Database Name'
      ,SUM(num_of_reads) AS 'Number of Read'
      ,SUM(num_of_writes) AS 'Number of Writes'
FROM sys.dm_io_virtual_file_stats(NULL, NULL) I
  INNER JOIN sys.databases D
      ON I.database_id = d.database_id
GROUP BY name ORDER BY 'Number of Read' DESC;
  
  
--DISK I/O: For Disk I/0 issues, this will display I/O statistics by physical drive letter
SELECT 'Disk I/O Which Drive Letter' as description,
    left(f.physical_name, 1) AS DriveLetter,
    DATEADD(MS,sample_ms * -1, GETDATE()) AS [Start Date],
    SUM(v.num_of_writes) AS total_num_of_writes,
    SUM(v.num_of_bytes_written) AS total_num_of_bytes_written,
    SUM(v.num_of_reads) AS total_num_of_reads,
    SUM(v.num_of_bytes_read) AS total_num_of_bytes_read,
    SUM(v.size_on_disk_bytes) AS total_size_on_disk_bytes
FROM sys.master_files f
INNER JOIN sys.dm_io_virtual_file_stats(NULL, NULL) v
ON f.database_id=v.database_id and f.file_id=v.file_id
GROUP BY left(f.physical_name, 1),DATEADD(MS,sample_ms * -1, GETDATE());
  
  
--Check blocking queries
--Locate any blocking queries in general
select 'Query Sessions Being Blocked' as description, * from
    (select  er.session_id
           ,er.status
           ,case when er.blocking_session_id = 0 then 0 else 1 end as blocked
           ,case when exists (select 1 from sys.dm_exec_requests ersub where blocking_session_id = er.session_id) then 1 else 0 end as blocking
           ,er.blocking_session_id
           ,wait_type, wait_time
           ,er.start_time
           ,er.database_id
           ,er.writes
           ,er.logical_reads
           ,er.logical_reads*8/1024 [MBs of logical reads]
    from sys.dm_exec_requests er cross apply
         sys.dm_exec_sql_text(sql_handle) st
    where er.session_id > 50) sub
where Blocked = 1 or Blocking = 1
order by status, blocking desc, blocked desc, start_time
  
--Locate any blocking queries with number of sessions blocked and dbcc inputbuffer
select 'Sessions blocking other sessions' as description, 'dbcc inputbuffer(' + cast(blocking_session_id as varchar(50)) + ')' as inputbuffer, blocking_session_id, COUNT(session_id) as blocking_this_many_sessions from
    (select  er.session_id
           ,case when er.blocking_session_id = 0 then null else 1 end as blocked
           ,case when exists (select 1 from sys.dm_exec_requests ersub where blocking_session_id = er.session_id) then 1 else 0 end as blocking
           ,er.blocking_session_id
    from sys.dm_exec_requests er cross apply
         sys.dm_exec_sql_text(sql_handle) st
    where er.session_id > 50) sub
where Blocked = 1 or Blocking = 1
group by blocking_session_id
 
--Check the SQL Logs, the sp_readerrorlog takes four parameters:
--1) Value of error log file you want to read: 0 = current, 1 = Archive #1, 2 = Archive #2, etc...
--2) Log file type: 1 or NULL = error log, 2 = SQL Agent log
--3) Search string 1: String one you want to search for
--4) Search string 2: String two you want to search for to further refine the results
Declare @tablevar table(LogDate datetime, processInfo nvarchar(50), Text nvarchar(max))
Insert into @tablevar(LogDate, processInfo, Text) exec sp_readerrorlog 0, 1
SELECT TOP 200 'SQL Server Log Recent' as description, LogDate, processInfo, Text FROM @tablevar where Text NOT LIKE 'SQL Trace%' and Text NOT LIKE 'Setting database option RECOVERY to SIMPLE for %' order by LogDate DESC