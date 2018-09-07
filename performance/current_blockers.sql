--Sessions that are blocking other sessions.  Sometimes sessions that are blocking are also being blocked.  This helps figure out which is the primary blocker.
select blockingsessions.blocking_session_id, blockingsessions.blocking_this_many_sessions, 'Being blocked by session: ' + cast(s.blocking_session_id as varchar(20)), 'dbcc inputbuffer(' + cast(blockingsessions.blocking_session_id as varchar(20)) + ')' as dbccinput, sysprocess.status, sysprocess.lastwaittype 
from
(
SELECT count(session_id) as blocking_this_many_sessions, blocking_session_id
FROM sys.dm_exec_requests
WHERE blocking_session_id <> 0
group by blocking_session_id
) blockingsessions
inner join sys.dm_exec_requests s on blockingsessions.blocking_session_id = s.session_id
inner join sys.sysprocesses sysprocess on blockingsessions.blocking_session_id = sysprocess.spid
order by blockingsessions.blocking_this_many_sessions desc
