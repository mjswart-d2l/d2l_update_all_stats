SET NOCOUNT ON

DECLARE @command NVARCHAR(MAX)
DECLARE @command_temp AS NVARCHAR(MAX)
DECLARE @selectDB AS NVARCHAR(128) = ''

USE master

IF (OBJECT_ID('tempdb..#ALL_DBS') IS NOT NULL)
DROP TABLE #ALL_DBS

SELECT [name] INTO #ALL_DBS
FROM master.sys.databases WHERE database_id > 4 AND is_read_only = 0
AND [state_desc] = 'ONLINE'
AND [name] not like '%[_]%'
AND
-----------
name in
(select DISTINCT ADC.database_name 
from 
sys.availability_databases_cluster ADC 
inner join 
sys.dm_hadr_availability_replica_states HARS 
on ADC.group_id = HARS.group_id 
inner join sys.availability_group_listeners AGL 
on HARS.group_id = AGL.group_id 
where is_local = 'TRUE' 
and HARS.role_desc = 'PRIMARY' 
UNION
select DISTINCT name 
from master.sys.databases 
where name not in
(
select database_name from
sys.availability_databases_cluster ADC 
)
)
ORDER BY [name]

USE msdb

WHILE (SELECT COUNT(*) FROM #ALL_DBS) > 0
BEGIN
	SELECT @selectdb = (SELECT TOP 1 * FROM #ALL_DBS)

	SELECT @command_temp = 'USE msdb
	EXEC [dbo].[cs_CUSTOM_D2L_StatsUpdate] @DBName = ''' +  @selectdb + ''', @JobName = ''IT Update Stats'''

	EXEC(@command_temp)
	--PRINT @command_temp
	DELETE FROM #ALL_DBS WHERE [name] = @selectdb
END


DROP TABLE #ALL_DBS