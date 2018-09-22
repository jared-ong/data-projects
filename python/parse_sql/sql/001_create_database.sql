USE [master]
GO
if exists (select * from sys.databases where name = 'parse_sql')
BEGIN
	ALTER DATABASE [parse_sql] SET SINGLE_USER;
	DROP DATABASE [parse_sql];
END
GO
USE master
GO
CREATE DATABASE [parse_sql]
GO

USE [parse_sql]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[git_tag_dates](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[git_repo] [nvarchar](255) NULL,
	[git_tag] [nvarchar](255) NULL,
	[git_tag_date] [datetime] NULL,
 CONSTRAINT [PK_git_tag_dates] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[parse_sql](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[full_path] [nvarchar](max) NULL,
	[dir_path] [nvarchar](max) NULL,
	[file_name] [nvarchar](255) NULL,
	[file_content] [nvarchar](max) NULL,
	[file_content_hash] [nvarchar](max) NULL,
	[file_size] [bigint] NULL,
	[git_repo] [nvarchar](255) NULL,
	[git_tag] [nvarchar](255) NULL,
 CONSTRAINT [PK_parse_sql] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[parse_sql_ddl](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[change_type] [nvarchar](50) NULL,
	[ddl] [nvarchar](max) NULL,
	[dir_path] [nvarchar](max) NULL,
	[file_name] [nvarchar](255) NULL,
	[full_path] [nvarchar](max) NULL,
	[git_repo] [nvarchar](255) NULL,
	[git_tag] [nvarchar](255) NULL,
	[object_action] [nvarchar](50) NULL,
	[object_name] [nvarchar](255) NULL,
	[object_type] [nvarchar](255) NULL,
 CONSTRAINT [PK_parse_sql_ddl] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[rave_version_git_tags](
	[rave_version] [nvarchar](255) NULL,
	[release] [nvarchar](255) NULL,
	[raveversionsortable1] [int] NULL,
	[raveversionsortable2] [int] NULL,
	[git_tag] [nvarchar](4000) NULL
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[rave_version_prod_urls_git_tags](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[rave_version] [nvarchar](255) NULL,
	[release] [nvarchar](255) NULL,
	[raveversionsortable1] [int] NULL,
	[raveversionsortable2] [int] NULL,
	[git_tag] [nvarchar](4000) NULL,
 CONSTRAINT [PK_rave_version_prod_urls_git_tags] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[git_tag_exclusions](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[git_repo] [nvarchar](255) NULL,
	[git_tag] [nvarchar](255) NULL,
 CONSTRAINT [PK_git_tag_exclusions] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE procedure [dbo].[checkdiff] (@firsttag nvarchar(50), @secondtag nvarchar(50)) as
--declare @firsttag nvarchar(50) = 'v5.6.4-Patch1'
--declare @secondtag nvarchar(50) = 'v2015.1.0'
select subA.dir_path, subA.file_name, subB.dir_path, subB.file_name from
(select * from develop_branch_sql where git_tag = @firsttag and dir_path like '%Rave_Viper_Lucy%' and (dir_path like '%Daily%Changes%' or dir_path like '%procedures%' or dir_path like '%views%' or dir_path like '%functions%' or dir_path like '%types%')) subA
full outer join (select * from develop_branch_sql where git_tag = @secondtag and dir_path like '%Rave_Viper_Lucy%' and (dir_path like '%Daily%Changes%' or dir_path like '%procedures%' or dir_path like '%views%' or dir_path like '%functions%' or dir_path like '%types%')) subB on (subA.file_name = subB.file_name) or (subA.file_content_hash = subB.file_content_hash) 
where subA.file_content_hash is null or subB.file_content_hash is null or subA.file_content_hash <> subB.file_content_hash
GO

USE [parse_sql]
GO

/****** Object:  View [dbo].[v_parse_sql_ddl_errors1]    Script Date: 9/20/2018 3:31:41 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[v_parse_sql_ddl_errors1] as
select ddl, file_name, count(*) as count_new, min(git_tag) as min_git_tag, max(git_tag) as max_git_tag
from parse_sql_ddl 
where change_type = 'new'
group by ddl, file_name
having count(*) > 1
GO


USE [parse_sql]
GO

/****** Object:  View [dbo].[v_parse_sql_ddl_errors2]    Script Date: 9/20/2018 3:32:03 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create view [dbo].[v_parse_sql_ddl_errors2] as
select *
from parse_sql_ddl 
where object_type is null
GO




insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Patch232_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Patch231_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-5.6.4.34')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-5.6.4.29')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-AutoBuild-5.6.4.30')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-CCAuto-5.6.4.30')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-CCAuto-5.6.4.31')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.30')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.31')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.32')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.33')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.34')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.35')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.36')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.37')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.38')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Patch234_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.39')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.40')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.41')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.42')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.43')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.44')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave5.6.4.45_Prod_Release')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','5.6.4.45_Prod_Release')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Orig-Release')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Orig-Release_build_45')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Auto-Build-5.6.4.45')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_VK_DT12371_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-1')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-2')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-3')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-4')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-5')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-6')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-7')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-9')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-10')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-11')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-12')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-13')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-14')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-8')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-15')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-18')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-21')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-19')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-20')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-23')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-24')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-25')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-26')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-27')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-28')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-29')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-30')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Patch1-Auto-Build-5.6.4.46')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Patch1-Auto-Build-5.6.4.47')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-Build-31')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CruiseControlNet-564-Patch1-Auto-Build-5.6.4.47')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Patch1-Build-5.6.4.47')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','CCNet-564-Patch1-Build-5.6.4.48')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Q42010Patch_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave564ReportingV1.0')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','PrePMS_Merge')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Patch237_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch3')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch3B')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch4_build_82')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4.82')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/RFD')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/push_events_to_sqs')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','Rave563_Patch238_unfinished_29-Jul-2011')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/SQS_Cache')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch5_build_95')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/TSDV1.2')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch6')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch6_build_97')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch7_build_100')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/FinalPerfLabRun')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/PerfLab')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch8')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch8_build_120')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/MEF_SubModule')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/MEF2_Tsdv_Temp')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch9_build_130')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch10')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch10_build_133')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','unmerged/feature/Rave564_LRP')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch6-with-CMPs')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.4-Patch13')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v5.6.5-hotfix/Rav2012.1.0.1')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2013.1.0_cmps_archive')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2013.3.0.2')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2012.1.0.1_cmps_archive')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2013.1.0.1_cmps_archive')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2013.2.0.2')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2013.4.0.2')
insert into git_tag_exclusions (git_repo, git_tag) values ('Rave','v2015.2.1')
