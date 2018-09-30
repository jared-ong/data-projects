IF EXISTS (SELECT NULL FROM INFORMATION_SCHEMA.ROUTINES WHERE Specific_Name = 'spMyProcedure')
		DROP PROC spMyProcedure
GO
 
/*---------------------------------------------------------------------------------------
//   
//
//--------------------------------------------------------------------------------------*/
CREATE PROCEDURE dbo.spMyProcedure	@variablea varchar(100),
											@variableb int = 1
AS
BEGIN
		SET NOCOUNT ON

		PRINT @variablea


GO

-- Must give Rave_Reporter permission to the stored procedure
GRANT EXECUTE ON OBJECT::dbo.spMyProcedure TO Rave_Reporter
GO


/****** Object:  StoredProcedure [dbo].[spRandomProcedure]    Script Date: 10/6/2015 1:04:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[spRandomProcedure]
	@LocalString nvarchar(4000)
AS
PRINT @LocalString

GO

USE [pmasdfl]
GO

/****** Object:  Table [dbo].[all_tags]    Script Date: 9/23/2018 5:17:22 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE all_tags
GO

CREATE TABLE [dbo].[all_tags](
	[git_tag] [varchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO



