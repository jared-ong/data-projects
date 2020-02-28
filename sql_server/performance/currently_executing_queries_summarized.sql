declare @var_long_running_minutes int = 0
declare @session_id int
declare @login_name nvarchar(255)
declare @elapsed_minutes bigint
declare @login_time datetime
declare @last_batch datetime
declare @database_name nvarchar(255)
declare @jobname nvarchar(500)
declare @program_name nvarchar(500)
declare @client_server_name nvarchar(500)
declare @client_interface_name nvarchar(500)
declare @status_desc nvarchar(255)

if OBJECT_ID('tempdb..#input_buffer_sessions') IS NOT NULL
    drop table #input_buffer_sessions

create table #input_buffer_sessions (
	session_id int,
	login_name1 nvarchar(255),
	elapsed_minutes bigint,
	login_time datetime,
	last_batch datetime,
	database_name nvarchar(255),
	jobname nvarchar(255),
	program_name1 nvarchar(500),
	client_server_name nvarchar(500),
	client_interface_name nvarchar(500),
	status_desc nvarchar(255),
	EventType nvarchar(30) NULL,
	Parameters int NULL,
	EventInfo nvarchar(max) NULL
	)

if OBJECT_ID('tempdb..#input_buffer') IS NOT NULL
    drop table #input_buffer

create table #input_buffer (
	EventType NVARCHAR(30) NULL,
	Parameters INT NULL,
	EventInfo NVARCHAR(max) NULL
)

declare sessions_cursor cursor for 
select session_id, login_name, elapsed_minutes, login_time, last_batch, database_name, jobname, program_name, client_server_name, client_interface_name, MAX(status_desc) as status_desc
from
(
	select er.session_id
		   ,s.loginame as login_name
		   ,datediff(MINUTE,s.last_batch,GETDATE()) as elapsed_minutes
		   ,s.login_time
		   ,s.last_batch
		   ,DB_Name(er.database_id) as database_name
		   ,(select name from msdb..sysjobs where convert(varchar(50), convert(varbinary(50), job_id), 1) = substring(es.program_name, 30, 34)) as jobname
		   ,es.program_name
		   ,es.host_name as client_server_name
		   ,es.client_interface_name
		   ,CASE(s.status) WHEN 'background' then '0 - background'
				WHEN 'sleeping' then '1 - sleeping'
				WHEN 'rollback' then '2 - rollback'
				WHEN 'dormant' then '3 - dormant'
				WHEN 'pending' then '4 - pending'
				WHEN 'spinloop' then '5 - spinloop'
				WHEN 'suspended' then '6 - suspended'
				WHEN 'runnable' then '7 - runnable'
				WHEN 'running' then '8 - running' END AS status_desc
	from sys.dm_exec_requests er 
		 inner join sys.dm_exec_sessions es on er.session_id = es.session_id 
		 inner join sys.sysprocesses s on er.session_id = s.spid
	where er.session_id > 50
	and er.session_id <> @@SPID
	and s.loginame is not null
	and s.loginame <> ''
	and es.program_name not like 'SqlQueryNotificationService%'
) sub
group by session_id, login_name, elapsed_minutes, login_time, last_batch, database_name, jobname, program_name, client_server_name, client_interface_name

open sessions_cursor  
fetch next from sessions_cursor into @session_id, @login_name, @elapsed_minutes, @login_time, @last_batch, @database_name, @jobname, @program_name, @client_server_name, @client_interface_name, @status_desc

while @@FETCH_STATUS = 0  
begin  
    declare @input_buffer_query nvarchar(max)
	set @input_buffer_query = 'DBCC INPUTBUFFER(' + cast(@session_id as nvarchar(10)) + ')'
	
	insert #input_buffer
	exec(@input_buffer_query)
	
	insert into #input_buffer_sessions (session_id,	login_name1, elapsed_minutes, login_time, last_batch, database_name, jobname, program_name1, client_server_name, client_interface_name, status_desc, EventType, Parameters, EventInfo)
	select @session_id, @login_name, @elapsed_minutes, @login_time, @last_batch, @database_name, @jobname, @program_name, @client_server_name, @client_interface_name, @status_desc, EventType, Parameters, EventInfo
	from #input_buffer

	truncate table #input_buffer

    fetch next from sessions_cursor into @session_id, @login_name, @elapsed_minutes, @login_time, @last_batch, @database_name, @jobname, @program_name, @client_server_name, @client_interface_name, @status_desc
end 

close sessions_cursor  
deallocate sessions_cursor 

select session_id, elapsed_minutes, status_desc, login_name1 as login_name, login_time, last_batch, database_name, COALESCE(jobname, program_name1) as program_or_job_name, client_server_name as client_server_name, client_interface_name, EventInfo as sql_query
from #input_buffer_sessions
order by login_name, elapsed_minutes desc

if OBJECT_ID('tempdb..#input_buffer_sessions') IS NOT NULL
    drop table #input_buffer_sessions

if OBJECT_ID('tempdb..#input_buffer') IS NOT NULL
    drop table #input_buffer

