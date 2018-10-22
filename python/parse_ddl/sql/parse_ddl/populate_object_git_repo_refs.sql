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