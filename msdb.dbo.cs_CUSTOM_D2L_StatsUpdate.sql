CREATE PROCEDURE [dbo].[cs_CUSTOM_D2L_StatsUpdate]

@DBName SYSNAME,
@JobName SYSNAME

AS

SET NOCOUNT ON

------------------start making additional customizations here-------------------

DECLARE @stats_rows_changed_threshold INT	--Number of rows modified threshold to trigger stats update in combination with "@stats_age_threshold_days" of days since last update 
DECLARE @default_stats_sample_size INT		--This is a default sample rate/percentage we use for any stats created on a table with more than @stats_rows_count_threshold " rows changed
DECLARE @stats_age_threshold_days INT			--Number of days since last update to trigger stats update in combination with "@stats_rows_changed_threshold" rows changed
DECLARE @stats_rows_count_threshold INT	--Number of rows to determine if  FULL SAMPLE scan should be used. Less than this value means FULL SAMPLE otherwise SAMPLE = @default_stats_sample_size
DECLARE @stats_HistoryKeepThreshold INT		--Keep stats update history for this many days

IF EXISTS (SELECT * FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_rows_changed_threshold')
BEGIN
	SELECT @stats_rows_changed_threshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_rows_changed_threshold'
END
ELSE
BEGIN
	SELECT @stats_rows_changed_threshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [JobName] = @JobName AND [ConfigName] = 'stats_rows_changed_threshold'
END
-----
IF EXISTS (SELECT * FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'default_stats_sample_size')
BEGIN
	SELECT @default_stats_sample_size = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'default_stats_sample_size'
END
ELSE
BEGIN
	SELECT @default_stats_sample_size = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [JobName] = @JobName AND [ConfigName] = 'default_stats_sample_size'
END
-----
IF EXISTS (SELECT * FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_age_threshold_days')
BEGIN
	SELECT @stats_age_threshold_days = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_age_threshold_days'
END
ELSE
BEGIN
	SELECT @stats_age_threshold_days = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [JobName] = @JobName AND [ConfigName] = 'stats_age_threshold_days'
END
-----
IF EXISTS (SELECT * FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_rows_count_threshold')
BEGIN
	SELECT @stats_rows_count_threshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_rows_count_threshold'
END
ELSE
BEGIN
	SELECT @stats_rows_count_threshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [JobName] = @JobName AND [ConfigName] = 'stats_rows_count_threshold'
END
-----
IF EXISTS (SELECT * FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_HistoryKeepThreshold')
BEGIN
	SELECT @stats_HistoryKeepThreshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [DatabaseName] = @DBName AND [JobName] = @JobName AND [ConfigName] = 'stats_HistoryKeepThreshold'
END
ELSE
BEGIN
	SELECT @stats_HistoryKeepThreshold = CAST(ConfigValue AS INT) FROM [msdb].[dbo].[D2LJobsConfigValues] WHERE [JobName] = @JobName AND [ConfigName] = 'stats_HistoryKeepThreshold'
END
-----

--PRINT @stats_rows_changed_threshold
--PRINT @default_stats_sample_size
--PRINT @stats_age_threshold_days
--PRINT @stats_rows_count_threshold
--PRINT @stats_HistoryKeepThreshold

------------------stop making customizations here---------------------

DECLARE @CollectStatsCmd NVARCHAR(MAX)
DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @CurrentUpdateStatsCmd NVARCHAR(MAX)

--Verify DB exists....
IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = @DBName)
BEGIN
	GOTO ERROR_EXIT_HERE
END

------------------------------------------------------------------------------------
---- Save progress of the completed actions (to MSDN database)
------------------------------------------------------------------------------------


IF (OBJECT_ID('msdb.dbo.D2LStatsUpdateHistory') IS NULL)
BEGIN

	CREATE TABLE msdb.dbo.D2LStatsUpdateHistory
	( 
		[DatabaseName] sysname, 
		[DatabaseId] int, 
		[ObjectId] int,
		[TableName] sysname NULL,
		[TableSchema] sysname NULL, 
		[IndexName] sysname NULL, 
		[StatsId] int, 
		[StatsName] sysname,
		[StatsLastUpdated] DATETIME, 
		[RowsCount] bigint,
		[RowsModifiedCount] bigint,
		[RowsSampledCount] bigint,
		[Action] nvarchar(max) NULL,
		[TimeStarted] DATETIME NULL,
		[TimeCompleted] DATETIME NULL
	) 
END;
--------------Maintenance-------------------------------------
DELETE FROM msdb.dbo.D2LStatsUpdateHistory 
WHERE DatabaseName = @DBName
AND
TimeCompleted < DATEADD(DD, -@stats_HistoryKeepThreshold, GETDATE())
--------------------------------------------------------------

--------------------------------------------------------------
----Load D2LStatsUpdateHistory with Stats Metadata
--------------------------------------------------------------

SET @CollectStatsCmd = 'USE [' + @DBName + ']

INSERT msdb.dbo.D2LStatsUpdateHistory
SELECT DISTINCT
	DB_NAME() AS [Col1],
	DB_ID() AS [Col2],
	i.object_id,
    t.name as tablename,
	SCHEMA_NAME(t.schema_id) AS [Col5],
    i.name as index_name,
	sp.stats_id,
	STS.name,
    sp.last_updated,
	sp.[rows],
	sp.modification_counter,
	sp.rows_sampled,
	NULL AS [Col12],
	NULL AS [Col13],
	NULL AS [Col14]
FROM sys.indexes i
JOIN sys.tables t on t.object_id = i.object_id
CROSS APPLY sys.dm_db_stats_properties(i.object_id, i.index_id) sp
JOIN
sys.stats STS
on i.object_id = STS.object_id
AND
sp.stats_id = STS.stats_id
WHERE sp.last_updated < ''' + CONVERT(NVARCHAR(MAX), (DATEADD(DD, -@stats_age_threshold_days, GETDATE())), 120) + '''
AND
sp.modification_counter >= ' + CAST(@stats_rows_changed_threshold AS NVARCHAR(MAX)) + '
AND
STS.name not in
(
	SELECT [StatsName] FROM msdb.dbo.D2LStatsUpdateHistory
	WHERE [TimeCompleted] IS NULL
	AND [DatabaseName] = '''+ @DBName + '''
)
'

--PRINT (@CollectStatsCmd)
EXEC (@CollectStatsCmd)

-----------------Process D2LStatsUpdateHistory Table------------------
DECLARE @SchemaN SYSNAME
DECLARE @TableN SYSNAME
DECLARE @IndexN SYSNAME
DECLARE @StatsId INT
DECLARE @StatsN SYSNAME
DECLARE @ObjectId INT
DECLARE @RowsC BIGINT
DECLARE @TimeStartedTemp DATETIME

DECLARE cStats CURSOR FOR
SELECT [TableName],	[TableSchema], [ObjectId], [IndexName],	[StatsId], [StatsName], [RowsCount] 
FROM msdb.dbo.D2LStatsUpdateHistory
WHERE DatabaseName = @DBName
AND
TimeCompleted IS NULL
Order BY [StatsLastUpdated] DESC

OPEN cStats
	
FETCH NEXT FROM cStats INTO @TableN, @SchemaN, @ObjectId , @IndexN, @StatsId, @StatsN, @RowsC

WHILE @@FETCH_STATUS = 0
BEGIN


	IF(@RowsC > @stats_rows_count_threshold)
	BEGIN
		SET @CurrentUpdateStatsCmd = 'USE [' + @DBName + ']

		UPDATE STATISTICS [' + @SchemaN + '].[' + @TableN + '](' + @StatsN + ') WITH SAMPLE ' + CAST(@default_stats_sample_size AS NVARCHAR(MAX)) + ' PERCENT'

	END
	ELSE
	BEGIN
		SET @CurrentUpdateStatsCmd = 'USE [' + @DBName + ']

		UPDATE STATISTICS [' + @SchemaN + '].[' + @TableN + '](' + @StatsN + ') WITH SAMPLE 100 PERCENT'
	END

	--Update the start time

	SET @TimeStartedTemp = GETDATE();

	UPDATE msdb.dbo.D2LStatsUpdateHistory
	SET [TimeStarted] = @TimeStartedTemp, [Action] = @CurrentUpdateStatsCmd
	WHERE [TableSchema] = @SchemaN
	AND [TableName] = @TableN
	AND [StatsName] = @StatsN
	AND [DatabaseName] = @DBName
	AND [ObjectId] = @ObjectId;

	--PRINT(@CurrentUpdateStatsCmd);
	EXEC(@CurrentUpdateStatsCmd);

	--Update the time completed

	UPDATE msdb.dbo.D2LStatsUpdateHistory
	SET TimeCompleted = GETDATE()
	WHERE [TableSchema] = @SchemaN
	AND [TableName] = @TableN
	AND [StatsName] = @StatsN
	AND [DatabaseName] = @DBName
	AND [ObjectId] = @ObjectId
	AND [TimeStarted] = @TimeStartedTemp;

	FETCH NEXT FROM cStats INTO @TableN, @SchemaN, @ObjectId , @IndexN, @StatsId, @StatsN, @RowsC;
END
		
CLOSE cStats
DEALLOCATE cStats


GOTO SUCCESS_EXIT_HERE

ERROR_EXIT_HERE:
SELECT @ErrorMessage = 'The DB called "' + @DBName + '" doesn''t exist!'
RAISERROR (@ErrorMessage, 10, 1) WITH LOG, NOWAIT --informational only

SUCCESS_EXIT_HERE:
SELECT @ErrorMessage = 'All done!'
RAISERROR (@ErrorMessage, 10, 1) WITH LOG, NOWAIT --informational only
