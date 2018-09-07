declare @varTableName nvarchar(255)
declare table_cursor CURSOR FOR 
select table_name from information_schema.tables

open table_cursor;
fetch next from table_cursor into @varTableName;

while @@FETCH_STATUS = 0
begin

	print @varTableName
	fetch next from table_cursor into @varTableName;

end;
close table_cursor;
deallocate table_cursor;