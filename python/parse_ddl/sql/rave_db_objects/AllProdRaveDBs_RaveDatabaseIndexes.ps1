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
$filename = "C:\Temp\AllDatabaseIndexes" + $enddate + ".csv"

$thequery = "
set transaction isolation level read uncommitted

select i.name as index_name, o.name as table_name, i.index_id, i.type_desc, i.is_unique, i.is_primary_key, i.is_unique_constraint
from sys.indexes i
inner join sys.objects o on i.object_id = o.object_id
left outer join (select ViewName from dbo.ClinicalViews) cvs on o.name = cvs.ViewName
where o.is_ms_shipped = 0
and cvs.ViewName is null
and o.name not like 'BK_%'
and o.name not like 'V_%Metrics$%'

"

$whoquery = "select lower(url) as url, upper(sqlserver) as sqlserver, dbname, raveversion, urltype, (select clientname from clients where client_id = sites.client) as clientname, (select clientname from clients where client_id = sites.partner_id) as partnername, 
    (select RaveVersionSortable from RaveVersionLookup where RaveVersion = sites.RaveVersion) as RaveVersionSortable
    from dbo.sites
    where disabled = 0"

msq-ToCSV -WhoIsQuery $whoquery -Query $thequery -ColumnIncludes url, urltype, sqlserver, dbname -OutFile $filename