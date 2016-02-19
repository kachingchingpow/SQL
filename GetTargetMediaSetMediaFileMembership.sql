USE [ExportTracking]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

/****** Object:  Table [export].[cfgTMSMembership] ******/

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[export].[cfgTMSMembership]') AND type in (N'U'))
DROP TABLE [export].[cfgTMSMembership]
GO

CREATE TABLE [export].[cfgTMSMembership](
	[ExportID] [int] NULL,
	[ETLBatchID] [int] NULL,
	[MediaFileID] [int] NULL,
	[TargetMediaSetID] [int] NULL
) ON [PRIMARY]

GO

/****** Object:  Table [export].[expTMSMembership] ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[export].[expTMSMembership]') AND type in (N'U'))
DROP TABLE [export].[expTMSMembership]
GO

CREATE TABLE [export].[expTMSMembership](
	[MediaFileID] [int] NULL,
	[TargetMediaSetID] [int] NULL
) ON [PRIMARY]

GO


IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[export].[GetTargetMediaSetMediaFileMembership]') AND type in (N'P', N'PC'))
DROP PROCEDURE [export].[GetTargetMediaSetMediaFileMembership]
GO



CREATE PROCEDURE export.GetTargetMediaSetMediaFileMembership
AS
    SET NOCOUNT ON;
	
    DECLARE @CurrentETLBatchID	INT;
    DECLARE	@PreviousETLBatchID	INT;
    DECLARE @CurrentExportID	INT;
    DECLARE @PreviousExportID	INT;
    
    /* Get the current and previous state ETLBatchID's */
    SET @CurrentETLBatchID = (select MAX(ETLBatchID) from [NexidiaESIDW].[dbo].[cfgETLBatch]);
    SET @PreviousETLBatchID =(select MAX(ETLBatchID) from [ExportTracking].[export].[cfgTMSMembership]);
    
    /* Get the current and previous state ExportID's */
    --SET @CurrentExportID = (select MAX(ETLBatchID) from [NexidiaESIDW].[dbo].[cfgETLBatch] where BatchEndDateTime is null); -- is this the most recent?
    SET @PreviousExportID =(select MAX(ExportID) from [ExportTracking].[export].[cfgExportBatchTracking] where BatchStartDateTime is not null and BatchEndDateTime is not null);
    
    /* 
		just creating 3 temp tables to hold the result sets.  I'll load these with the results from the DW (current)
		and ExportTracking (previous), then full outer join them into the ExportInsert table.
	 */
    
    CREATE TABLE #PreviousMembership
		(
				ExportID					int,
				TargetMediaSetKey			int, 
				MediaFileKey				int, 
				ETLBatchID					int
		);
    
    CREATE TABLE #CurrentMembership
		(
				TargetMediaSetKey			int, 
				MediaFileKey				int, 
				ETLBatchID					int
		);
		
	CREATE TABLE #ExclusionTable
		(
				pTargetMediaSetKey			int, 
				pMediaFileKey				int,
				cTargetMediaSetKey			int, 
				cMediaFileKey				int

		);
		
	/* pull the current previous membership tree from the DW Bridge Tables */
	
	/* if previous membership tree exists, pull it */
	
	IF EXISTS (SELECT @PreviousETLBatchID)
		BEGIN
			/* previous membership */
			INSERT  INTO #PreviousMembership
			SELECT  extmsm.ExportID,
					dtms.TargetMediaSetKey,
					dmf.MediaFileKey,
					extmsm.ETLBatchID
			FROM	ExportTracking.export.cfgTMSMembership	extmsm
			INNER JOIN	NexidiaESIDW.dbo.dimTargetMediaSet	dtms								
				ON	dtms.TargetMediaSetID	= extmsm.TargetMediaSetID
			INNER JOIN	NexidiaESIDW.dbo.dimMediaFile	dmf
				ON	dmf.MediaFileId			= extmsm.MediaFileID
			where extmsm.ETLBatchId			= @PreviousETLBatchID
			order by 3,1,2;
			
			/* current membership */
			INSERT INTO #CurrentMembership
			SELECT  dtms.TargetMediaSetKey,
					MediaFileKey,
					brMS.ETLBatchID
			FROM		NexidiaESIDW.TargetMediaSet.dimmapTargetMediaSetBridgeMediaFile			brMF
			INNER JOIN	NexidiaESIDW.TargetMediaSet.dimmapTargetMediaSetBridgeTargetMediaSet	brMS
					ON	brMS.TargetMediaSetBridgeKey	= brMF.TargetMediaSetBridgeKey
			INNER JOIN	NexidiaESIDW.TargetMediaSet.dimTargetMediaSet dtms
					ON	dtms.TargetMediaSetKey			= brMS.TargetMediaSetKey
			where brMS.ETLBatchId				= @CurrentETLBatchID
			order by 3,1,2;
		END

	ELSE  /* pull the current.  no previous exists. */
		BEGIN
			INSERT #CurrentMembership
			SELECT  dtms.TargetMediaSetKey,
					MediaFileKey,
					brMS.ETLBatchID
			FROM		NexidiaESIDW.TargetMediaSet.dimmapTargetMediaSetBridgeMediaFile			brMF
			INNER JOIN	NexidiaESIDW.TargetMediaSet.dimmapTargetMediaSetBridgeTargetMediaSet	brMS
					ON	brMS.TargetMediaSetBridgeKey	= brMF.TargetMediaSetBridgeKey
			INNER JOIN	NexidiaESIDW.TargetMediaSet.dimTargetMediaSet dtms
					ON	dtms.TargetMediaSetKey			= brMS.TargetMediaSetKey
			where brMS.ETLBatchId				= @CurrentETLBatchID
			order by 3,1,2;	
		END

    /* full outer join will show the inserts and deletes  this creates the final multiple exclusion table. */
	INSERT		INTO #ExclusionTable
	SELECT		pm.TargetMediaSetKey,
				pm.MediaFileKey,
				cm.TargetMediaSetKey,
				cm.MediaFileKey
	FROM			#previousmembership			pm
	full OUTER JOIN	#currentmembership			cm
	ON	pm.TargetMediaSetKey		= cm.TargetMediaSetKey
	and pm.MediaFileKey				= cm.MediaFileKey

	/*  here's where I build the final export table.  need to join back to the dims to get the mediafileid and tmsid */
	INSERT  INTO [ExportTracking].[export].[expTMSMembership]
	SELECT	dmf.MediaFileID,
			dtms.TargetMediaSetID
	FROM		#ExclusionTable   eT
	inner join	NexidiaESIDW.dbo.dimTargetMediaSet dtms
	ON		eT.cTargetMediaSetKey	= dtms.TargetMediaSetKey
	inner join	NexidiaESIDW.dbo.dimMediaFile dmf
	ON		eT.cMediaFileKey	= dmf.MediaFileKey
	WHERE	(eT.pTargetMediaSetKey	is null		-- indicates there's a new tmskey and mfkey in current state (insert)
	and		eT.pMediaFileKey		is null)	-- indicates there's a new tmskey and mfkey in current state (insert)	
	
	INSERT  INTO [ExportTracking].[export].[expTMSMembership]
	SELECT	dmf.MediaFileID,
			dtms.TargetMediaSetID
	FROM		#ExclusionTable   eT
	inner join	NexidiaESIDW.dbo.dimTargetMediaSet dtms
	ON		eT.cTargetMediaSetKey	= dtms.TargetMediaSetKey
	inner join	NexidiaESIDW.dbo.dimMediaFile dmf
	ON		eT.cMediaFileKey	= dmf.MediaFileKey
	WHERE	(eT.cTargetMediaSetKey	is null		-- indicates there's a row in previous state that's no longer in current (delete)
	and		eT.cMediaFileKey		is null)	-- indicates there's a row in previous state that's no longer in current (delete)	  
GO