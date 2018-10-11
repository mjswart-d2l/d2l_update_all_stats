use msdb;
GO
ALTER PROCEDURE dbo.cs_CUSTOM_D2L_StatsUpdate
  @DBName SYSNAME,
  @JobName SYSNAME = 'IT Update Stats'
AS

SET NOCOUNT ON

--Number of rows modified threshold to trigger stats update in combination with
-- "@stats_age_threshold_days" of days since last update 
DECLARE @stats_rows_changed_threshold INT;
SELECT TOP 1 @stats_rows_changed_threshold = CAST(ConfigValue AS INT)
  FROM msdb.dbo.D2LJobsConfigValues
 WHERE DatabaseName IN (@DBName, 'DefaultValue')
   AND JobName = @JobName 
   AND ConfigName = 'stats_rows_changed_threshold'
 ORDER BY CASE WHEN DatabaseName = @DBName THEN 0 ELSE 1 END ASC;

--Number of days since last update to trigger stats update in combination with
-- @stats_rows_changed_threshold" rows changed
DECLARE @stats_age_threshold_days INT;
SELECT TOP 1 @stats_age_threshold_days = CAST(ConfigValue AS INT)
  FROM msdb.dbo.D2LJobsConfigValues
 WHERE DatabaseName IN (@DBName, 'DefaultValue')
   AND JobName = @JobName 
   AND ConfigName = 'stats_age_threshold_days'
 ORDER BY CASE WHEN DatabaseName = @DBName THEN 0 ELSE 1 END ASC;

--Number of rows to determine if  FULL SAMPLE scan should be used. Less than
-- this value means FULL SAMPLE otherwise let SQL Server determine default 
-- sample rate
DECLARE @stats_rows_count_threshold INT;
SELECT TOP 1 @stats_rows_count_threshold = CAST(ConfigValue AS INT)
  FROM msdb.dbo.D2LJobsConfigValues
 WHERE DatabaseName IN (@DBName, 'DefaultValue')
   AND JobName = @JobName 
   AND ConfigName = 'stats_rows_count_threshold'
 ORDER BY CASE WHEN DatabaseName = @DBName THEN 0 ELSE 1 END ASC;

--Keep stats update history for this many days
DECLARE @stats_HistoryKeepThreshold INT;
SELECT TOP 1 @stats_HistoryKeepThreshold = CAST(ConfigValue AS INT)
  FROM msdb.dbo.D2LJobsConfigValues
 WHERE DatabaseName IN (@DBName, 'DefaultValue')
   AND JobName = @JobName 
   AND ConfigName = 'stats_HistoryKeepThreshold'
 ORDER BY CASE WHEN DatabaseName = @DBName THEN 0 ELSE 1 END ASC;
 
 --This is a default sample rate/percentage we use for any stats created on a
-- table with more than @stats_rows_count_threshold " rows changed
DECLARE @default_stats_sample_size INT;
SELECT TOP 1 @default_stats_sample_size = CAST(ConfigValue AS INT)
  FROM msdb.dbo.D2LJobsConfigValues
 WHERE DatabaseName IN (@DBName, 'DefaultValue')
   AND JobName = @JobName 
   AND ConfigName = 'default_stats_sample_size'
 ORDER BY CASE WHEN DatabaseName = @DBName THEN 0 ELSE 1 END ASC;

DECLARE @CollectStatsCmd NVARCHAR(MAX)
DECLARE @ErrorMessage NVARCHAR(MAX)
DECLARE @CurrentUpdateStatsCmd NVARCHAR(MAX)

-- Verify DB exists....
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
		DatabaseName sysname, 
		DatabaseId int, 
		ObjectId int,
		TableName sysname NULL,
		TableSchema sysname NULL, 
		StatsId int, 
		StatsName sysname,
		StatsLastUpdated DATETIME, 
		RowsCount bigint,
		RowsModifiedCount bigint,
		RowsSampledCount bigint,
		Action nvarchar(max) NULL,
		TimeStarted DATETIME NULL,
		TimeCompleted DATETIME NULL
	) 
END;

--------------Maintenance-------------------------------------
DELETE FROM msdb.dbo.D2LStatsUpdateHistory 
 WHERE DatabaseName = @DBName
   AND TimeCompleted < DATEADD(DD, -@stats_HistoryKeepThreshold, GETDATE())
--------------------------------------------------------------

--------------------------------------------------------------
----Load D2LStatsUpdateHistory with Stats Metadata
--------------------------------------------------------------

SET @CollectStatsCmd = 'USE ' + @DBName + ';
INSERT msdb.dbo.D2LStatsUpdateHistory (
       DatabaseName,
       DatabaseId,
       ObjectId,
       TableName,
       TableSchema,
       StatsId,
       StatsName,
       StatsLastUpdated,
       RowsCount,
       RowsModifiedCount,
       RowsSampledCount )
SELECT @DBName,
	   DB_ID(@DBName),
	   s.object_id,
       OBJECT_NAME(s.object_id),
	   OBJECT_SCHEMA_NAME(s.object_id),
	   s.stats_id,
	   s.name,
       sp.last_updated, 
	   sp.rows,
	   sp.modification_counter,
	   sp.rows_sampled
  FROM sys.stats s
 CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
 WHERE OBJECTPROPERTY(s.object_id,''IsMSShipped'') = 0
   AND sp.last_updated < DATEADD(DD, -@stats_age_threshold_days, GETDATE())
   AND sp.modification_counter >= @stats_rows_changed_threshold
   AND s.name not in (
	     SELECT StatsName 
	       FROM msdb.dbo.D2LStatsUpdateHistory
	      WHERE TimeCompleted IS NULL
	        AND DatabaseName = @DBName
       )';

exec sp_executesql @stmt = @CollectStatsCmd, 
                   @params = N'@DBName sysname, 
				               @stats_age_threshold_days int, 
							   @stats_rows_changed_threshold int',
				   @DBName = @DBName,
				   @stats_age_threshold_days = @stats_age_threshold_days,
				   @stats_rows_changed_threshold = @stats_rows_changed_threshold;

-----------------Process D2LStatsUpdateHistory Table------------------
DECLARE @SchemaN SYSNAME
DECLARE @TableN SYSNAME
DECLARE @StatsId INT
DECLARE @StatsN SYSNAME
DECLARE @ObjectId INT
DECLARE @RowsC BIGINT
DECLARE @TimeStartedTemp DATETIME
DECLARE @IsIndexStat BIT
DECLARE @CurrentGetIsSystemStatCommand NVARCHAR(max)
DECLARE @ParmDefinition NVARCHAR(500);

DECLARE cStats CURSOR FOR
	SELECT TableName,	
	       TableSchema, 
		   ObjectId, 
		   StatsId, 
		   StatsName, 
		   RowsCount 
      FROM msdb.dbo.D2LStatsUpdateHistory
     WHERE DatabaseName = @DBName
       AND TimeCompleted IS NULL
  ORDER BY StatsLastUpdated DESC;

OPEN cStats;
	
FETCH NEXT FROM cStats 
	INTO @TableN, 
		 @SchemaN, 
		 @ObjectId , 
		 @StatsId, 
		 @StatsN, 
		 @RowsC;

WHILE ( @@FETCH_STATUS = 0 )
BEGIN
	--Determine if current stat is based on an index
	SET @CurrentGetIsSystemStatCommand = 'USE ' + @DBName + ';
		IF EXISTS (SELECT 1 FROM sys.Indexes WHERE name = '''
		+ @StatsN + ''')
		BEGIN
			SET @RetVal = 1;
		END
		ELSE
		BEGIN
			SET @RetVal = 0
		END'
	SET @ParmDefinition = N'@RetVal BIT OUTPUT';
	EXEC sp_executesql @CurrentGetIsSystemStatCommand, @ParmDefinition, @RetVal=@IsIndexStat OUTPUT;

    IF (@RowsC > @stats_rows_count_threshold AND @IsIndexStat = 0)
    BEGIN
      SET @CurrentUpdateStatsCmd = 'USE ' + @DBName + '; 
            UPDATE STATISTICS ' 
            + QUOTENAME(@SchemaN) + '.' 
            + QUOTENAME(@TableN) 
            + '(' + QUOTENAME(@StatsN) + ');';
    END
    ELSE IF (@RowsC > @stats_rows_count_threshold AND @IsIndexStat = 1)
    BEGIN
      SET @CurrentUpdateStatsCmd = 'USE ' + @DBName + ';
            UPDATE STATISTICS '
            + QUOTENAME(@SchemaN) + '.'
            + QUOTENAME(@TableN)
            + '(' + QUOTENAME(@StatsN) + ') 
            WITH SAMPLE ' + CAST(@default_stats_sample_size AS NVARCHAR(5)) + ' PERCENT;';
    END
    ELSE
    BEGIN
      SET @CurrentUpdateStatsCmd = 'USE ' + @DBName + '; 
            UPDATE STATISTICS ' 
            + QUOTENAME(@SchemaN) + '.' 
            + QUOTENAME(@TableN) 
            + '(' + QUOTENAME(@StatsN) + ')
            WITH SAMPLE 100 PERCENT;';
    END

	--Update the start time
	UPDATE msdb.dbo.D2LStatsUpdateHistory
	   SET TimeStarted = GETDATE(), 
	       Action = @CurrentUpdateStatsCmd
	 WHERE TableSchema = @SchemaN
	   AND TableName = @TableN
	   AND StatsName = @StatsN
	   AND DatabaseName = @DBName
	   AND ObjectId = @ObjectId;

	EXEC sp_executesql @CurrentUpdateStatsCmd;

	--Update the time completed
	UPDATE msdb.dbo.D2LStatsUpdateHistory
	   SET TimeCompleted = GETDATE()
	 WHERE TableSchema = @SchemaN
	   AND TableName = @TableN
	   AND StatsName = @StatsN
	   AND DatabaseName = @DBName
	   AND ObjectId = @ObjectId;

	FETCH NEXT FROM cStats 
	INTO @TableN, 
		 @SchemaN, 
		 @ObjectId , 
		 @StatsId, 
		 @StatsN, 
		 @RowsC;
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
