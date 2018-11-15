IF OBJECT_ID('dbo.TableA', 'U') IS NOT NULL 
	DROP TABLE dbo.TableA;
go
create table TableA (ID bigint NOT NULL PRIMARY KEY, Value bigint NOT NULL)
go
CREATE UNIQUE NONCLUSTERED INDEX [IX_TableA] ON [dbo].[TableA]
(
	[Value] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO

declare @varRows bigint = 10000
declare @varCounter bigint = 0
while @varCounter <= @varRows
begin
	insert into TableA values (@varCounter, @varCounter)
	set @varCounter = @varCounter + 1
end
go

exec test.sys.sp_cdc_disable_db
go
exec test.sys.sp_cdc_enable_db
go

exec test.sys.sp_cdc_enable_table
	@source_schema = N'dbo',
	@source_name   = 'TableA',
	@filegroup_name = 'PRIMARY',
	@role_name	  = null,
	@supports_net_changes = 0;
go


use test
go
--single update
update TableA
set Value = 200003
where Value = 3
go
WAITFOR DELAY '00:00:10';
go
select * from test.cdc.dbo_TableA_CT
go
truncate table test.cdc.dbo_TableA_CT
go

--multiple update
update TableA
set Value = 100001 + Value
where ID < 5
go
WAITFOR DELAY '00:00:10';
go
select * from test.cdc.dbo_TableA_CT
go
truncate table test.cdc.dbo_TableA_CT
go
