select do.url, do.object_name, do.type_desc, COALESCE(ogrr.git_repo_refs, 'Unknown') as git_repo_refs
from rave_db_objects.dbo.database_objects do
left outer join parse_ddl.dbo.object_git_repo_refs ogrr on do.object_name = ogrr.object_name
where url IN ('jnj.mdsol.com')

--38089 rows
select object_name, type_desc, count(url) as count_urls
from
(
	select do.*
	from rave_db_objects.dbo.database_objects do
	left outer join parse_ddl.dbo.git_parse_ddl_objects gpdo on do.object_name = gpdo.object_name
	where gpdo.object_name is null
) sub
group by object_name, type_desc
order by count_urls desc


select object_name, type_desc, count(url) as count_urls
from
(
	select do.*
	from rave_db_objects.dbo.database_objects do
	left outer join parse_ddl.dbo.git_parse_ddl_objects gpdo on do.object_name = gpdo.object_name
	where gpdo.object_name is null
) sub
group by object_name, type_desc
order by count_urls desc

/***
delete from parse_ddl.dbo.git_parse_ddl where git_repo = 'RaveStatusCustomCode'
delete from parse_ddl.dbo.git_parse_ddl_objects where git_repo = 'RaveStatusCustomCode'
***/

select object_name, COUNT(git_repo) as countRepos
from
(
	select distinct object_name, git_repo
	from parse_ddl.dbo.git_parse_ddl_objects
) sub
group by object_name
having count(git_repo) = 1
order by countrepos asc

select * from parse_ddl.dbo.git_parse_ddl_objects where object_name = 'spRptAuditTrail_DataPageV2'

/****
--Reload object_git_repo_refs
CREATE TABLE object_git_repo_refs (object_name nvarchar(255) NOT NULL, git_repo_refs nvarchar(400), PRIMARY KEY (object_name))

INSERT INTO object_git_repo_refs (object_name, git_repo_refs)
SELECT object_name,  STUFF((SELECT  ', ' + git_repo
            FROM (select distinct object_name, git_repo from dbo.git_parse_ddl_objects where object_name is not null) EE
            WHERE  EE.object_name = E.object_name
            ORDER BY git_repo
        FOR XML PATH('')), 1, 1, '') AS git_repo_refs
FROM (select distinct object_name, git_repo from dbo.git_parse_ddl_objects where object_name is not null) E
GROUP BY E.object_name
****/

select * from git_parse_ddl_objects where object_name is null