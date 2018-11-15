SELECT do.url, do.object_name, do.type_desc, COALESCE(ogrr.git_repo_refs, 'Unknown') as git_repo_refs
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN ('abbvieravex.mdsol.com')

SELECT do.url, COUNT(do.object_name) as count_of_objects, do.type_desc, COALESCE(ogrr.git_repo_refs, 'Unknown') as git_repo_refs
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN ('abbvieravex.mdsol.com')
GROUP BY do.url, do.type_desc, COALESCE(ogrr.git_repo_refs, 'Unknown')

SELECT do.url, COUNT(do.object_name) as count_of_objects, do.type_desc, COALESCE(ogrr.git_repo_main, 'Unknown') as git_repo_main
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN ('abbvieravex.mdsol.com')
GROUP BY do.url, do.type_desc, COALESCE(ogrr.git_repo_main, 'Unknown')

SELECT do.url, COUNT(do.object_name) as count_of_objects, COALESCE(ogrr.git_repo_main, 'Unknown') as git_repo_main
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN ('abbvieravex.mdsol.com')
GROUP BY do.url, COALESCE(ogrr.git_repo_main, 'Unknown')
order by COUNT(do.object_name) DESC

SELECT do.*
FROM rave_db_objects.dbo.database_objects do
LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name
WHERE url IN ('abbvieravex.mdsol.com')
and ogrr.object_name is null


--38089 rows
SELECT object_name, type_desc, count(url) as count_urls
FROM
(
	SELECT do.*
	FROM rave_db_objects.dbo.database_objects do
	LEFT OUTER JOIN parse_ddl.dbo.git_parse_ddl_objects gpdo ON do.object_name = gpdo.object_name
	WHERE gpdo.object_name is null
) sub
GROUP BY object_name, type_desc
order by count_urls desc


SELECT object_name, type_desc, count(url) as count_urls
FROM
(
	SELECT do.*
	FROM rave_db_objects.dbo.database_objects do
	LEFT OUTER JOIN parse_ddl.dbo.git_parse_ddl_objects gpdo ON do.object_name = gpdo.object_name
	WHERE gpdo.object_name is null
) sub
GROUP BY object_name, type_desc
order by count_urls desc

/***
delete FROM parse_ddl.dbo.git_parse_ddl WHERE git_repo = 'RaveStatusCustomCode'
delete FROM parse_ddl.dbo.git_parse_ddl_objects WHERE git_repo = 'RaveStatusCustomCode'
***/

SELECT object_name, COUNT(git_repo) as countRepos
FROM
(
	SELECT distinct object_name, git_repo
	FROM parse_ddl.dbo.git_parse_ddl_objects
) sub
GROUP BY object_name
having count(git_repo) = 1
order by countrepos asc


/****
--Reload object_git_repo_refs
CREATE TABLE object_git_repo_refs (object_name nvarchar(255) NOT NULL, git_repo_refs nvarchar(400), PRIMARY KEY (object_name))

TRUNCATE TABLE object_git_repo_refs

INSERT INTO object_git_repo_refs (object_name, git_repo_refs, git_repo_main)
SELECT ora.object_name, ora.git_repo_refs, orm.git_repo FROM
(
	SELECT object_name,  STUFF((SELECT  ', ' + git_repo
				FROM (SELECT distinct object_name, git_repo FROM dbo.git_parse_ddl_objects WHERE object_name is not null) EE
				WHERE  EE.object_name = E.object_name
				ORDER BY git_repo
			FOR XML PATH('')), 1, 1, '') AS git_repo_refs
	FROM (SELECT distinct object_name, git_repo FROM dbo.git_parse_ddl_objects WHERE object_name is not null) E
	GROUP BY E.object_name
) ora
INNER JOIN
(
	SELECT sub.object_name, gt2.git_repo
	FROM
	(
		SELECT gpdo.object_name,
		MIN(gt.git_repo_order_priority) as min_priority
		FROM dbo.git_parse_ddl_objects gpdo
		LEFT OUTER JOIN dbo.git_tags gt ON gpdo.git_repo = gt.git_repo
		GROUP BY gpdo.object_name
	) sub
	LEFT OUTER JOIN dbo.git_tags gt2 ON sub.min_priority = gt2.git_repo_order_priority
) orm ON ora.object_name = orm.object_name

****/

SELECT * FROM git_parse_ddl_objects