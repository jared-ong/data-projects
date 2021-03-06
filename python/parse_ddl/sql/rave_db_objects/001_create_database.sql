USE [master]
GO
/****** Object:  Database [rave_db_objects]    Script Date: 10/1/2018 9:11:07 PM ******/
CREATE DATABASE [rave_db_objects]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'rave_objects', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL14.SQL2017\MSSQL\DATA\rave_objects.mdf' , SIZE = 3088384KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'rave_objects_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL14.SQL2017\MSSQL\DATA\rave_objects_log.ldf' , SIZE = 7151616KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO
ALTER DATABASE [rave_db_objects] SET COMPATIBILITY_LEVEL = 140
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [rave_db_objects].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [rave_db_objects] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [rave_db_objects] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [rave_db_objects] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [rave_db_objects] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [rave_db_objects] SET ARITHABORT OFF 
GO
ALTER DATABASE [rave_db_objects] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [rave_db_objects] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [rave_db_objects] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [rave_db_objects] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [rave_db_objects] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [rave_db_objects] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [rave_db_objects] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [rave_db_objects] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [rave_db_objects] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [rave_db_objects] SET  DISABLE_BROKER 
GO
ALTER DATABASE [rave_db_objects] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [rave_db_objects] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [rave_db_objects] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [rave_db_objects] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [rave_db_objects] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [rave_db_objects] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [rave_db_objects] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [rave_db_objects] SET RECOVERY FULL 
GO
ALTER DATABASE [rave_db_objects] SET  MULTI_USER 
GO
ALTER DATABASE [rave_db_objects] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [rave_db_objects] SET DB_CHAINING OFF 
GO
ALTER DATABASE [rave_db_objects] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [rave_db_objects] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [rave_db_objects] SET DELAYED_DURABILITY = DISABLED 
GO
EXEC sys.sp_db_vardecimal_storage_format N'rave_db_objects', N'ON'
GO
ALTER DATABASE [rave_db_objects] SET QUERY_STORE = OFF
GO
USE [rave_db_objects]
GO
ALTER DATABASE SCOPED CONFIGURATION SET IDENTITY_CACHE = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET LEGACY_CARDINALITY_ESTIMATION = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET MAXDOP = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET PARAMETER_SNIFFING = PRIMARY;
GO
ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO
ALTER DATABASE SCOPED CONFIGURATION FOR SECONDARY SET QUERY_OPTIMIZER_HOTFIXES = PRIMARY;
GO
USE [rave_db_objects]
GO
/****** Object:  Table [dbo].[database_indexes]    Script Date: 10/1/2018 9:11:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[database_indexes](
	[id] [bigint] NULL,
	[index_name] [nvarchar](255) NULL,
	[table_name] [nvarchar](255) NULL,
	[index_id] [int] NULL,
	[type_desc] [nvarchar](255) NULL,
	[is_unique] [smallint] NULL,
	[is_primary_key] [smallint] NULL,
	[is_unique_constraint] [smallint] NULL,
	[url] [nvarchar](500) NULL,
	[urltype] [nvarchar](50) NULL,
	[sqlserver] [nvarchar](255) NULL,
	[dbname] [nvarchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[database_objects]    Script Date: 10/1/2018 9:11:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[database_objects](
	[id] [bigint] NULL,
	[object_name] [nvarchar](255) NULL,
	[type_desc] [nvarchar](255) NULL,
	[parent_object_name] [nvarchar](255) NULL,
	[create_date] [datetime] NULL,
	[modify_date] [datetime] NULL,
	[url] [nvarchar](500) NULL,
	[urltype] [nvarchar](50) NULL,
	[sqlserver] [nvarchar](255) NULL,
	[dbname] [nvarchar](255) NULL
) ON [PRIMARY]
GO
/****** Object:  Index [ix_database_indexes_id]    Script Date: 10/1/2018 9:11:08 PM ******/
CREATE NONCLUSTERED INDEX [ix_database_indexes_id] ON [dbo].[database_indexes]
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [ix_database_objects_id]    Script Date: 10/1/2018 9:11:08 PM ******/
CREATE NONCLUSTERED INDEX [ix_database_objects_id] ON [dbo].[database_objects]
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
USE [master]
GO
ALTER DATABASE [rave_db_objects] SET  READ_WRITE 
GO
