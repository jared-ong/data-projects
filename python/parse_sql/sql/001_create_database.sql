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
