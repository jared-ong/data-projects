select (pages_kb + awe_allocated_kb + shared_memory_reserved_kb + shared_memory_committed_kb)/1024/1024 as MemoryGB
, type
, name
, pages_kb
, virtual_memory_reserved_kb
, virtual_memory_committed_kb
, awe_allocated_kb
, shared_memory_reserved_kb
, shared_memory_committed_kb
from sys.dm_os_memory_clerks ORDER BY (pages_kb + awe_allocated_kb + shared_memory_reserved_kb + shared_memory_committed_kb) desc