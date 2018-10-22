SELECT do.url, count(do.object_name) as count_of_objects, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
group by do.url, do.type_desc
order by count_of_objects desc

SELECT do.url, do.object_name, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
AND do.url = 'incr2.mdsol.com'

SELECT do.object_name, count(do.url) as object_frequency_count, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
group by do.object_name, do.type_desc
order by object_frequency_count desc