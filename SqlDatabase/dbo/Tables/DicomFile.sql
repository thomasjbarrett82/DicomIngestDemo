﻿CREATE TABLE [dbo].[DicomFile]
(
	[Id] INT IDENTITY(1,1) NOT NULL, 
    [SopClassUid] VARCHAR(70) NOT NULL, 
    [SopInstanceUid] VARCHAR(70) NOT NULL,
	[PatientId] VARCHAR(70) NULL,
	[StudyInstanceUid] VARCHAR(70) NULL,
	[SeriesInstanceUid] VARCHAR(70) NULL,
	[DicomJson] NVARCHAR(MAX) NOT NULL,
    CONSTRAINT [PK_DicomFile] PRIMARY KEY CLUSTERED
	(
		[Id] ASC
	) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IDX_DicomFile_SopClassUid] ON [dbo].[DicomFile]
(
	[SopClassUid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IDX_DicomFile_SopInstanceUid] ON [dbo].[DicomFile]
(
	[SopInstanceUid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IDX_DicomFile_PatientId] ON [dbo].[DicomFile]
(
	[PatientId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IDX_DicomFile_StudyInstanceUid] ON [dbo].[DicomFile]
(
	[StudyInstanceUid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO


CREATE NONCLUSTERED INDEX [IDX_DicomFile_SeriesInstanceUid] ON [dbo].[DicomFile]
(
	[SeriesInstanceUid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
