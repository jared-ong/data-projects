
--Only top level job
select *
from
(
	select j.name as Task
	, CAST(msdb.dbo.agent_datetime(jh.run_date,jh.run_time) as smalldatetime) as Start
	, CAST(DATEADD(second, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100, msdb.dbo.agent_datetime(jh.run_date,jh.run_time)) as smalldatetime) as Finish
	, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100 as run_duration_seconds
	, (run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100) / 60 as run_duration_minutes
	from msdb.dbo.sysjobhistory jh
	inner join msdb.dbo.sysjobs j on jh.job_id = j.job_id
	inner join msdb.dbo.syscategories s on j.category_id = s.category_id
	where jh.step_name = '(Job outcome)'
	and j.name not like '%DBAMon%'
	and s.name not like '%REPL%'
) sub
where sub.run_duration_minutes > 5
and Start > getdate() - 7


--Each step of each job plus summary record
select j.name
, jh.step_name
, j.job_id
, jh.step_id
, jh.run_status
, CASE(jh.run_status) WHEN 0 THEN 'Failed' WHEN 1 THEN 'Succeeded' WHEN 2 THEN 'Retry' WHEN 3 THEN 'Canceled' WHEN 4 THEN 'In Progress' END as run_status_desc
,  stuff(stuff(replace(str(run_duration,6,0),' ','0'),3,0,':'),6,0,':') as run_duration_desc
, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100 as run_duration_seconds
, jh.run_date
, jh.run_time
, msdb.dbo.agent_datetime(jh.run_date,jh.run_time) as run_start_time
, DATEADD(second, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100, msdb.dbo.agent_datetime(jh.run_date,jh.run_time)) as run_end_time
from msdb.dbo.sysjobhistory jh
inner join msdb.dbo.sysjobs j on jh.job_id = j.job_id

