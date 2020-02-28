# Invoke-sqlcmd Connection string parameters
$RaveServer = 'hdcu10dvdbsv087.backup.hdc.mdsol.com'
$RaveDB = 'hdcvcl02052'
$DestServer = 'hdc405prdbsv002'
$DestDB = 'tempdb'
$DestTable = 'testtable'
$TruncateDestTableBeforeReInsert = $true

# Query the source Rave DB
$sqlQuery = @"
select top 10000 * from
Datapoints
order by 1
"@

# Results temporarily stores the query results
$dataTable = Invoke-Sqlcmd -ServerInstance $RaveServer -Database $RaveDB -Query $sqlQuery 


if ($TruncateDestTableBeforeReInsert)
{
    $truncateQuery = "truncate table " + $DestTable
    Invoke-Sqlcmd -ServerInstance $DestServer -Database $DestDB -Query $truncateQuery 
}

#Define Connection string
$connectionString = "Data Source=" + $DestServer + "; Integrated Security=True;Initial Catalog=" + $DestDB + ";"
#Bulk copy object instantiation
$bulkCopy = new-object ("Data.SqlClient.SqlBulkCopy") $connectionString
#Define the destination table 
$bulkCopy.DestinationTableName = $DestTable
#load the data into the target
$bulkCopy.WriteToServer($dataTable)
