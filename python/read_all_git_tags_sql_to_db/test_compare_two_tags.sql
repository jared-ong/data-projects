--unchanged files
select * from
(select * from develop_branch_sql where tag = 'v2018.1.3' and file_name <> '20100920-02-RemoveTaggingReports.sql' and file_name <> 'Create_missing_indexes_on_FK.sql' and file_name <> '20110506-01-Table_CoderFieldConfigurationWorkFlowDataAddUpdatedAndCreated.sql') a
inner join (select * from develop_branch_sql where tag = 'v2017.2.0' and file_name <> '20100920-02-RemoveTaggingReports.sql' and file_name <> 'Create_missing_indexes_on_FK.sql' and file_name <> '20110506-01-Table_CoderFieldConfigurationWorkFlowDataAddUpdatedAndCreated.sql') b on a.file_content_hash = b.file_content_hash

--new files
select * from
(select * from develop_branch_sql where tag = 'v2018.1.3' and file_name <> '20100920-02-RemoveTaggingReports.sql' and file_name <> 'Create_missing_indexes_on_FK.sql' and file_name <> '20110506-01-Table_CoderFieldConfigurationWorkFlowDataAddUpdatedAndCreated.sql') a
left outer join (select * from develop_branch_sql where tag = 'v2017.2.0' and file_name <> '20100920-02-RemoveTaggingReports.sql' and file_name <> 'Create_missing_indexes_on_FK.sql' and file_name <> '20110506-01-Table_CoderFieldConfigurationWorkFlowDataAddUpdatedAndCreated.sql') b on a.file_name = b.file_name or a.file_content_hash = b.file_content_hash
where b.file_name is null

--changed files
select * from develop_branch_sql
where file_content_hash in ('8becbaaf7933c006f531f9a6060f99db','9baeeee08f8d2e5a67e2c6e71a99f5cf','9dfc602704884d636127ea2b2d65d405')
and (tag = 'v2018.1.3')
