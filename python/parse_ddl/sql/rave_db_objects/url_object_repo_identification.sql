DECLARE @varURL nvarchar(255) = 'abbvie.mdsol.com'

SELECT do.url, COUNT(do.object_name) as count_of_objects, COALESCE(ogrr.git_repo_main, 'Object Not Found In Git Repo') as git_repo_main
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN (@varURL)
GROUP BY do.url, COALESCE(ogrr.git_repo_main, 'Object Not Found In Git Repo')
ORDER BY COUNT(do.object_name) DESC