/****
Check currently executing queries
RUNNING–meaning that the query is actively running on the CPU
RUNNABLE–meaning that the query is ready to run but CPU resources are not yet available. It is waiting in the Runnable Queue for a CPU to open up
SUSPENDED–meaning that the query is waiting for a third party resource to become available (for example,, disk I/O, blocking and so on)
****/
-- This query is based around sys.dm_exec_requests which shows one row for each statement currently executing on the server.
-- Other views and columns have been brought in to complete the information but it is still 1 row per request.
select er.session_id
       ,s.kpid
       ,'dbcc inputbuffer ('+cast(er.session_id as varchar(15)) + ');' as dbccinputbuffer
       ,es.login_name
       ,s.status
       ,er.wait_type
       ,s.lastwaittype
       ,s.waittime/1000 as wait_time_seconds
       ,datediff(MINUTE,s.last_batch,GETDATE()) as last_batch_elapsed_minutes
       ,er.cpu_time
       ,s.cpu
       ,s.memusage
       ,s.physical_io
       ,es.row_count
       ,case when blocking_session_id = 0 then 0 else 1 end as blocked
       ,case when exists (select 1 from sys.dm_exec_requests ersub where blocking_session_id = er.session_id) then 1 else 0 end as blocking
       ,er.blocking_session_id
	   ,er.writes
       ,er.logical_reads
       ,er.logical_reads*8/1024 [MBs of logical reads]
       ,er.start_time
       ,es.login_time
       ,s.last_batch
       ,er.database_id
       ,es.program_name
       ,es.host_name
       ,es.client_interface_name
       ,(select name from msdb..sysjobs where convert(varchar(50), convert(varbinary(50), job_id), 1) = substring(es.program_name, 30, 34)) as jobname
from  sys.dm_exec_requests er
inner join sys.dm_exec_sessions es on er.session_id = es.session_id
inner join sys.sysprocesses s on er.session_id = s.spid
where er.session_id > 50 
and er.session_id <> @@SPID
and es.program_name not like 'SqlQueryNotificationService%'
order by s.spid, er.status, blocking desc, blocked desc, er.start_time