use buzzard
go
create view rave_version_git_tags as
select distinct rave_version, release, case when rave_version like '5.6.5%' then 2 when rave_version like '5.6.4%' then 1 end as raveversionsortable1, cast(replace(replace(rave_version,'5.6.5.',''),'5.6.4.','') as int) as raveversionsortable2, 
case when release = 'Rave 5.6.4 Patch 01' then 'v5.6.4-Patch1' 
when release = 'Rave 5.6.4 Patch 02' then 'v5.6.4-Patch2' 
when release = 'Rave 5.6.4 Patch 04' then 'v5.6.4-Patch4'
when release = 'Rave 5.6.4 Patch 05' then 'v5.6.4-Patch5'
when release = 'Rave 5.6.4 Patch 07' then 'v5.6.4-Patch7'
when release = 'Rave 5.6.4 Patch 09' then 'v5.6.4-Patch9'
when release = 'Rave 5.6.4 Patch 10.1' then 'v5.6.4-Patch10.1'
when release = 'Rave 5.6.4 Patch 11' then 'v5.6.4-Patch11'
when release = 'Rave 5.6.4 Patch 12' then 'v5.6.4-Patch12'
else 'v' + replace(replace(replace(release,'Medidata Rave® ',''),'Medidata Rave ',''),'Medidata Rave? ','') end as tag
from falcon_sites 
where rave_version NOT IN ('5.6.5.5001','5.6.5.5002') and release not in ('Medidata Rave 2016.5.3')
order by raveversionsortable1, raveversionsortable2

select * into parse_sql.dbo.rave_version_git_tags from rave_version_git_tags

use parse_sql
go
select distinct tag into all_tags from parse_sql

truncate table rave_version_prod_urls_git_tags

insert into rave_version_prod_urls_git_tags (rave_version, release, raveversionsortable1, raveversionsortable2, tag)
select rvgt.*
from rave_version_git_tags rvgt
inner join all_tags at on rvgt.tag = at.tag
order by rvgt.raveversionsortable1 asc, rvgt.raveversionsortable2 asc
