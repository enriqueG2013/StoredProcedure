
/* Enrique Gonzalez - Stored Procedure: Append and Remove New Data */
/* Table names and attribute names changed due to wrok confidentiality */

USE [LocalDB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Append_n_Remove_Delta_Pull]

AS 
SET NOCOUNT ON  

--** STEP 0: Insert latest pull of data from staging table into archive table.

INSERT INTO [dbo].[Archive_Table]
SELECT DISTINCT *
	, GETDATE() AS [BackedUp]
FROM [dbo].[Delta_Pull_Staging_Table] WITH(NOLOCK)

	
--** STEP 1: Append latest delta pull into the main table, and map additional time attributes for reporting.
  
INSERT INTO [Main_Table]

SELECT DISTINCT [Id]
	, [Source]
	, [dbo].[udf_PTtoUTC] ([TimeGenerated_UTC]) AS [TimeGenerated_UTC] --Handling of _UTC timestamp as auto-converted to PT on ADF pipeline.
	, [Computer]
	, [RawData]
	, [ReportName]
	, [User]
	, [Message]
	, [id_g]
	, [UserAlias_s]
	, [dbo].[udf_PTtoUTC] ([SubmitDate_t_UTC]) AS [SubmitDate_t_UTC]  --Handling of _UTC timestamp as auto-converted to PT on ADF pipeline.
	, [dbo].[udf_PTtoUTC] ([RetrieveDate_t_UTC]) AS [RetrieveDate_t_UTC]  --Handling of _UTC timestamp as auto-converted to PT on ADF pipeline.
	, [ReportOutputType_s]
	, [ReportType_s]
	, [ScheduleType_s]
	, [ReportEditType_s]
	, [SourceType_s]
	, [QueryRunType_s]
	, [isRetrieveImmediate_b]
	, [TimeToResolve_d]
	, [TimeToStartExecution_d]
	, [TimeToFormat_d]
	, [TimeToExecute_d]
	, [TotalTimeTaken_d]
	, [TimeToRetrieve_d]
	, [TimeWaitingForServerAssignment_d]
	, [TimePendingToResolve_d]
	, [PrimaryPerspectiveName_s]
	, [SelectedAttributes_s]
	, [AttributeFilters_s]
	, [Statement_s]
	, [NumberOfRows_d]
	, [TableSize_s]
	, [ResultStoreProcedureName_s]
	, [ResultTableName_s]
	, [ResultServerName_s]
	, [ResultDatabaseName_s]
	, [UserAndDomainName_s]
	, [UserSelectedQueryServerName_s]
	, [ReportPacket_s]
	, [SendEmail_b]
	, [Status_s]
	, [StatusTime_t_UTC]
	, [Type]
	, [_ResourceId]
	, [Exported]
	, [TimeGenerated_UTC] AS [TimeGenerated_PT]
	, [SubmitDate_t_UTC] AS [SubmitDate_t_PT]
	, [RetrieveDate_t_UTC] AS [RetrieveDate_t_PT]
	, [StatusTime_t_UTC] AS [StatusTime_t_PT]
	, CONVERT(VARCHAR, [dbo].[udf_PTtoUTC] ([SubmitDate_t_UTC]), 1) AS [DateDay_UTC]
	, DATENAME(dw, [dbo].[udf_PTtoUTC] ([SubmitDate_t_UTC])) AS [DayName_UTC]
	, CONVERT(VARCHAR, fwUTC.FWEndDate, 1) AS [FiscalWeek_UTC]
	, fwUTC.FiscalWeekID AS [FiscalWeekID_UTC]
	, CONVERT(VARCHAR, [SubmitDate_t_UTC], 1) AS [DateDay_PT]
	, DATENAME(dw, ([SubmitDate_t_UTC])) AS [DayName_PT]
	, CONVERT(VARCHAR, fwPT.FWEndDate, 1) AS [FiscalWeek_PT]
	, fwPT.FiscalWeekID AS [FiscalWeekID_PT]
	, [DataSizeActual] = (dbo.[udf_CountChar]([SelectedAttributes_s], ',') - dbo.[udf_CountString]([SelectedAttributes_s], ',1'))
	* CONVERT(BIGINT,[NumberOfRows_d]) --AS [DataSizeActual]
	, [ColumnCount] = 
	dbo.[udf_CountChar]([SelectedAttributes_s], ',') - dbo.[udf_CountString]([SelectedAttributes_s], ',1') --AS [ColumnCount]

FROM [dbo].[Staging_Table] AS la WITH(NOLOCK)
INNER JOIN dbo.FiscalWeek AS fwUTC WITH(NOLOCK)
ON [dbo].[udf_PTtoUTC] (la.[SubmitDate_t_UTC]) BETWEEN fwUTC.FWStartDate AND fwUTC.FWEndDate
INNER JOIN dbo.FiscalWeek AS fwPT WITH(NOLOCK)
ON la.[SubmitDate_t_UTC] BETWEEN fwPT.FWStartDate AND fwPT.FWEndDate

WHERE CONCAT(id_g, CONVERT(VARCHAR(100), TimeGenerated_UTC, 127)) NOT IN			--Append only new queries with new TimeGenerated_UTC.
(SELECT DISTINCT CONCAT(id_g, CONVERT(VARCHAR(100), TimeGenerated_UTC, 127))
FROM Main_Table WITH(NOLOCK)
)



--** STEP 2: Remove duplicate IDs that are updated with new status.

DELETE Main_Table
WHERE CONCAT(CONVERT(VARCHAR(100), StatusTime_t_UTC, 127), id_g)
IN (																	--Detect queries of earlier, non-final Status, which received new stauts on latest pull.
	SELECT CONCAT(CONVERT(VARCHAR(100), MIN(y.StatusTime_t_UTC), 127), y.id_g) FROM
		(SELECT StatusTime_t_UTC, Status_s, id_g 
		FROM Main_Table WITH(NOLOCK)
		WHERE id_g IN (
			SELECT x.id_g 
			FROM (						--, x.CT, x.StatusTime_t 
				SELECT COUNT(id_g) AS CT, id_g		--, StatusTime_t	--Detect queries of duplicate, determined by ID_g.
				FROM Main_Table
				GROUP BY id_g						--, StatusTime_t
			) AS x
			WHERE CT > 1
		)
		) AS y
	GROUP BY y.id_g
)


--** STEP 3: Execute stored procedure to copy Staging table data to Main Table

EXEC [dbo].[Append_n_Remove_Delta_Pull]


GO