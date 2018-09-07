--This should flush all SQL caches.  Usually, I run this on a specific database other than master to get all the caches.
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