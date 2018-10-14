#Run this script from HDC405PRDBSV002 server C:\dba-msqmanualreports
$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
set-location $PSScriptRoot
#bring in the required functions
. .\invoke-sqlcmd3.ps1
. .\multiserverparallelquery_DS.ps1
. .\multiserverparallelquery.ps1
. .\multiserverserialquery.ps1
. .\msq-ToMSSQLTable.ps1
. .\msq-ToCsv.ps1
. .\msqs-ToMSSQLTable.ps1
. .\msqs-ToCsv.ps1

$enddate = (Get-Date).tostring("yyyyMMdd")
$filename = "C:\Temp\AllDatabaseObjects" + $enddate + ".csv"

$thequery = "
set transaction isolation level read uncommitted

select o.name as object_name, o.type_desc, po.name as parent_object_name, o.create_date, o.modify_date 
from sys.objects o
left outer join sys.objects po on o.parent_object_id = po.object_id
--left outer join (select distinct object_name from parse_sql.dbo.parse_sql_ddl) sub on o.name = sub.object_name
left outer join (select ViewName from dbo.ClinicalViews) cvs on o.name = cvs.ViewName
where o.is_ms_shipped = 0
--and sub.object_name is null
and cvs.ViewName is null
and o.name not like 'BK_%'
and o.type_desc NOT IN ('DEFAULT_CONSTRAINT', 'CHECK_CONSTRAINT', 'FOREIGN_KEY_CONSTRAINT', 'PRIMARY_KEY_CONSTRAINT', 'UNIQUE_CONSTRAINT')
and o.name not like 'V_%Metrics$%'

"

$whoquery = "select lower(url) as url, upper(sqlserver) as sqlserver, dbname, raveversion, urltype, (select clientname from clients where client_id = sites.client) as clientname, (select clientname from clients where client_id = sites.partner_id) as partnername, 
    (select RaveVersionSortable from RaveVersionLookup where RaveVersion = sites.RaveVersion) as RaveVersionSortable
    from dbo.sites
    where disabled = 0"

msq-ToCSV -WhoIsQuery $whoquery -Query $thequery -ColumnIncludes url, urltype, sqlserver, dbname -OutFile $filename