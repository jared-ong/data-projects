select Subject,
[Start Date],
[Start Time],
[End Date],
[End Time],
[All Day Event],
Description,
Location,
Private
from
(
	select j.name as Subject
	, CAST(msdb.dbo.agent_datetime(jh.run_date,jh.run_time) as date) as [Start Date]
	, CONVERT(VARCHAR(5), msdb.dbo.agent_datetime(jh.run_date,jh.run_time), 108) as [Start Time]
	, CAST(DATEADD(second, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100, msdb.dbo.agent_datetime(jh.run_date,jh.run_time)) as date) as [End Date]
	, CONVERT(VARCHAR(5), DATEADD(second, run_duration/10000 * 3600 + run_duration/100%100 * 60 + run_duration%100, msdb.dbo.agent_datetime(jh.run_date,jh.run_time)), 108) as [End Time]
	, 'False' as [All Day Event]
	, j.description as Description
	, @@SERVERNAME as Location
	, 'False' as Private
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
and [Start Date] > getdate() - 7
