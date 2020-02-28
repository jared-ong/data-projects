--Only top level job
select *
from
(
	select j.name as Task
	, CAST(msdb.dbo.agent_datetime(jh.run_date,jh.run_time) as smalldatetime) as Start
	, CAST(DATEADD(second, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100, msdb.dbo.agent_datetime(jh.run_date,jh.run_time)) as smalldatetime) as Finish
	from msdb.dbo.sysjobhistory jh
	inner join msdb.dbo.sysjobs j on jh.job_id = j.job_id
	inner join msdb.dbo.syscategories s on j.category_id = s.category_id
	where jh.step_name = '(Job outcome)'
	and j.name not like '%DBAMon%'
	and j.name not like '%Reindex%'
	and s.name not like '%REPL%'
	and j.name not like '%Delay Alert%'
	and j.name not like '%View Deletion%'
	and j.name not like '%Rave Role Status Partition Management%'
	and j.name not like '%Views Update%'
) sub
where Start > getdate() - 7
and Task like '%AbbottBA%'