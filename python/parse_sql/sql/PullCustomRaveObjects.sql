select * 
from RaveDev.sys.objects o
left outer join (select distinct object_name from parse_sql.dbo.parse_sql_ddl) sub on o.name = sub.object_name
where o.is_ms_shipped = 0
and sub.object_name is null
and type_desc NOT IN ('DEFAULT_CONSTRAINT', 'CHECK_CONSTRAINT', 'FOREIGN_KEY_CONSTRAINT', 'PRIMARY_KEY_CONSTRAINT', 'UNIQUE_CONSTRAINT')