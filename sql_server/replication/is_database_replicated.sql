DECLARE @isAGPrimary bit = 0
DECLARE @isAGReplica bit = 0
DECLARE @isTransactionalReplicationPublisher bit = 0
DECLARE @isTransactionalReplicationSubscriber bit = 0

IF EXISTS (SELECT name FROM sys.objects where name = 'MSreplication_objects')
	SET @isTransactionalReplicationSubscriber = 1
IF EXISTS (SELECT * from sys.databases WHERE is_published = 1 and name = db_name())
	SET @isTransactionalReplicationPublisher = 1

DECLARE @varSQL nvarchar(2000)
SET @varSQL = 'IF (sys.fn_hadr_is_primary_replica (db_name()) = 1 
AND EXISTS (select * from sys.dm_hadr_database_replica_cluster_states where database_name = db_name()))
BEGIN
SET @isAGPrimaryOUT = 1
END
'
DECLARE @sqlversion int
SELECT @sqlversion = CAST(CAST(SERVERPROPERTY('productversion') as varchar(2)) as int)
IF @sqlversion >= 12
BEGIN
	DECLARE @ParamDefinition nvarchar(100) = N'@isAGPrimaryOUT bit OUTPUT'
	EXECUTE sp_executesql
	@varSQL,
	@ParamDefinition,
	@isAGPrimaryOUT=@isAGPrimary OUTPUT
END

DECLARE @varSQL2 nvarchar(2000)
SET @varSQL2 = 'IF (sys.fn_hadr_backup_is_preferred_replica (db_name()) = 1
AND EXISTS (select * from sys.dm_hadr_database_replica_cluster_states where database_name = db_name()))
BEGIN
SET @isAGReplicaOUT = 1
END
'
IF @sqlversion >= 12
BEGIN
	DECLARE @ParamDefinition2 nvarchar(100) = N'@isAGReplicaOUT bit OUTPUT'
	EXECUTE sp_executesql
	@varSQL2,
	@ParamDefinition2,
	@isAGReplicaOUT=@isAGReplica OUTPUT
END

SELECT @isAGPrimary as IsAlwaysOnPrimary,
@isAGReplica as IsAlwaysOnReplica,
@isTransactionalReplicationPublisher as IsTransactionalReplicationPublisher,
@isTransactionalReplicationSubscriber as IsTransactionalReplicationSubscriber