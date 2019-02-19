--Count of object repo correlations by URL
SELECT u.url, u.RaveVersionSortable, u.IsRaveX, ogrr.git_repo_main, count(do.object_name) as count_of_ufos
FROM StatusRollupsHelper.dbo.urls u
LEFT OUTER JOIN rave_db_objects.dbo.database_objects do on u.url = do.url
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
group by u.url, u.RaveVersionSortable, u.IsRaveX, ogrr.git_repo_main
order by url desc

--Count of unidentified foreign objects by URL, type
SELECT do.url, count(do.object_name) as count_of_ufos, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
group by do.url, do.type_desc
order by count_of_ufos desc

--Count of unidentified foreign objects by object frequency, type
SELECT do.object_name, count(do.url) as object_frequency_count, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
group by do.object_name, do.type_desc
order by object_frequency_count desc

/***
SELECT do.url, do.object_name, do.type_desc
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE git_repo_refs is null
AND do.url = 'cardinalhealth.mdsol.com'

select * from rave_db_objects.dbo.database_objects where object_name = 'sp_insdel_846277644'

***/