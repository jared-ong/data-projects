--Locate any current backups or restores in progress with estimated percent complete
select session_id as SPID
, command
, dest.text AS Query
, start_time
, percent_complete,
dateadd(second,estimated_completion_time/1000, getdate()) asestimated_completion_time
FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) dest
WHERE r.command in('BACKUP DATABASE','RESTORE DATABASE','BACKUP LOG')