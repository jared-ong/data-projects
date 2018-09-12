dbcc shrinkdatabase('tempdb')
go

CHECKPOINT;
GO
DBCC DROPCLEANBUFFERS;
GO
DBCC FREEPROCCACHE;
GO
DBCC FREESYSTEMCACHE ('ALL') ;
GO
DBCC FREESESSIONCACHE;
GO
DECLARE @databaseid int
SELECT @databaseid = DB_ID();
DBCC FLUSHPROCINDB( @databaseid )
GO

dbcc shrinkdatabase('tempdb')
go
