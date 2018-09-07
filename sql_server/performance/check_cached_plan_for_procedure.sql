--Replace the procedure name in the where statement to pull back a specific procedure
select plan_handle,usecounts, cacheobjtype, objtype, size_in_bytes, text, query_plan
from sys.dm_exec_cached_plans
cross apply sys.dm_exec_sql_text(plan_handle)
cross apply sys.dm_exec_query_plan(plan_handle)
where usecounts > 1
and text like '%procedurename%'
and objtype = 'Proc'
order by usecounts desc;