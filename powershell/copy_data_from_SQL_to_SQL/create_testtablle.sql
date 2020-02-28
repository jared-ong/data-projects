SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[testtable](
	[DataPointID] [int] NOT NULL,
	[RecordID] [int] NOT NULL,
	[VariableID] [int] NOT NULL,
	[Data] [nvarchar](2000) NULL,
	[DataDictEntryID] [int] NULL,
	[UnitDictEntryID] [int] NULL,
	[DataActive] [bit] NOT NULL,
	[Created] [datetime] NOT NULL,
	[Updated] [datetime] NOT NULL,
	[FieldID] [int] NOT NULL,
	[ChangeCount] [int] NOT NULL,
	[ChangeCode] [int] NULL,
	[Guid] [char](36) NOT NULL,
	[ServerSyncDate] [datetime] NULL,
	[LockTime] [datetime] NULL,
	[IsVisible] [bit] NOT NULL,
	[MissingCode] [int] NULL,
	[IsTouched] [bit] NOT NULL,
	[IsNonConformant] [bit] NOT NULL,
	[ReqVerification] [bit] NOT NULL,
	[IsVerified] [bit] NOT NULL,
	[ReqTranslation] [bit] NOT NULL,
	[ReqCoding] [bit] NOT NULL,
	[WasSigned] [bit] NOT NULL,
	[IsSignatureCurrent] [bit] NOT NULL,
	[SignatureLevel] [int] NULL,
	[IsFrozen] [bit] NOT NULL,
	[IsLocked] [bit] NOT NULL,
	[EntryLocale] [char](3) NOT NULL,
	[AnalyteRangeID] [int] NULL,
	[ReferenceRangeID] [int] NULL,
	[AlertRangeID] [int] NULL,
	[Deleted] [bit] NOT NULL,
	[RangeStatus] [smallint] NOT NULL,
	[RangeUpdated] [datetime] NULL,
	[IsHidden] [bit] NOT NULL,
	[LastEnteredDate] [datetime] NULL,
	[DataPageId] [int] NOT NULL,
	[InstanceId] [int] NULL,
	[SubjectId] [int] NOT NULL,
	[StudySiteId] [int] NOT NULL,
	[StudyId] [int] NOT NULL,
	[IsUserDeactivated] [bit] NULL,
	[EnteredLabUnitID] [int] NULL,
	[LabUnitID] [int] NULL,
	[AltCodedValue] [nvarchar](255) NULL,
	[DeletedOrdinal] [int] NOT NULL,
	[ReqCoderCoding] [bit] NOT NULL,
	[UnVerifiedByInactivation] [bit] NULL,
	[UnReviewedByInactivation] [int] NULL,
 CONSTRAINT [PK_DataPoints] PRIMARY KEY CLUSTERED 
(
	[DataPointID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90) ON [PRIMARY]
) ON [PRIMARY]
GO


