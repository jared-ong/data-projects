create procedure generate_load
	@maxUpdates bigint = 5000
as
begin
	set nocount on
	declare @randomNumber numeric(17,15)
	declare @maxAuditID bigint
	declare @maxDatapointID bigint
	declare @randAuditID bigint
	declare @randDataPointID bigint
	declare @x bigint = 0
	select @maxAuditID = (auditID) from Audits
	select @maxDatapointID = max(datapointID) from Datapoints

	while @x <= @maxUpdates
	begin
		set @randomNumber = rand()
		set @randAuditID = @maxAuditID * @randomNumber
		set @randDataPointID = @maxDatapointID * @randomNumber
		update Datapoints set Updated = GETUTCDATE() where DatapointID = @randDataPointID
		update Audits set DatabaseTime = GETUTCDATE() where AuditID = @randAuditID
		--select 1
		--print @randAuditID
		print @randDataPointID
		WAITFOR DELAY '00:00:01'
		--print @randAuditID
		--print @randDatapointID
		set @x = @x + 1
	end
end