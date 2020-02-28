IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'find_url_in_job_data')
DROP PROCEDURE find_url_in_job_data
GO

CREATE PROCEDURE find_url_in_job_data (@var_url_search nvarchar(255)) AS

--DECLARE @var_url_search nvarchar(255) = 'az2.mdsol.com'

IF OBJECT_ID('tempdb..#JobData') IS NOT NULL
    DROP TABLE #JobData

IF OBJECT_ID('tempdb..#JobData2') IS NOT NULL
    DROP TABLE #JobData2

select Description as urlDesc, * 
into #JobData
from VC_JobDataTable 
where Description like '%.mdsol.com%'

--Add a space to front for reversal
update #JobData
set urlDesc = REVERSE(urlDesc) + ' '

--NULL
update #JobData set urlDesc = Replace(urlDesc,CHAR(0),' ');
--Horizontal Tab
update #JobData set urlDesc = Replace(urlDesc,CHAR(9),' ');
--Line Feed
update #JobData set urlDesc = Replace(urlDesc,CHAR(10),' ');
--Vertical Tab
update #JobData set urlDesc = Replace(urlDesc,CHAR(11),' ');
--Form Feed
update #JobData set urlDesc = Replace(urlDesc,CHAR(12),' ');
--Carriage Return
update #JobData set urlDesc = Replace(urlDesc,CHAR(13),' ');
--Column Break
update #JobData set urlDesc = Replace(urlDesc,CHAR(14),' ');
--Non-breaking space
update #JobData set urlDesc = Replace(urlDesc,CHAR(160),' ');
-- (
update #JobData set urlDesc = Replace(urlDesc,CHAR(40),' ');
-- )
update #JobData set urlDesc = Replace(urlDesc,CHAR(41),' ');
-- ,
update #JobData set urlDesc = Replace(urlDesc,CHAR(44),' ');

select ltrim(reverse(substring(urlDesc, charindex('moc.losdm.', urlDesc), charindex(char(32), urlDesc, charindex('moc.losdm.', urlDesc)) - charindex('moc.losdm.', urlDesc)))) as url, *
into #JobData2
from #JobData

select *
from #JobData2
where url = @var_url_search

IF OBJECT_ID('tempdb..#JobData') IS NOT NULL
    DROP TABLE #JobData

IF OBJECT_ID('tempdb..#JobData2') IS NOT NULL
    DROP TABLE #JobData2
GO

exec find_url_in_job_data 'az2.mdsol.com'
GO