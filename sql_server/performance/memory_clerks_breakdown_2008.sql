SELECT (single_pages_kb + multi_pages_kb +  virtual_memory_committed_kb + awe_allocated_kb + shared_memory_reserved_kb + shared_memory_committed_kb)/1024/1024 as MemoryGB
, *
FROM sys.dm_os_memory_clerks 
ORDER BY (single_pages_kb + multi_pages_kb +  virtual_memory_committed_kb + awe_allocated_kb + shared_memory_reserved_kb + shared_memory_committed_kb) DESC
