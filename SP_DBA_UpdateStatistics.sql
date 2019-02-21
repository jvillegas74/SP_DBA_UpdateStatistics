USE [master]
GO
IF EXISTS (select 1 from sys.procedures WHERE name ='SP_DBA_UpdateStatistics')
	DROP PROCEDURE dbo.SP_DBA_UpdateStatistics
GO
CREATE PROCEDURE dbo.SP_DBA_UpdateStatistics
AS
------------------------------------------------------------------------------------
--
-- dbo.SP_DBA_UpdateStatistics
-- 2019-01-09
-- Javier Villegas
-- Rev:1.1
-- Performs Update Statistics for all objects 
--
------------------------------------------------------------------------------------
SET NOCOUNT ON
DECLARE     
 @DaysBack int = 4,  --<--Can Adjust this Value    
 @PercModif int = 10, --<--Can Adjust this Value    
 @email varchar(100)= 'recipient@gmail.com',
 @options varchar(300)='WITH FULLSCAN,MAXDOP=0;',
 @db sysname,    
 @cmd varchar(4000),    
 @rowcountTotal int,    
 @currentCount int=1    

 DECLARE @Databases TABLE (id int identity,name sysname) 

IF (object_id( 'tempdb..#DBA_STATISTICS_STATUS_V3' ) IS NOT NULL) DROP TABLE #DBA_STATISTICS_STATUS_V3 ; 
    
CREATE TABLE #DBA_STATISTICS_STATUS_V3(    
id int identity primary key,    
database_name sysname,    
table_schema sysname,    
table_name sysname,   
auto_create bit,
stats_name sysname, 
stats_cols varchar(2000), 
stats_id int,
filter_definition nvarchar(max),
is_temporary bit,
no_recompute int, 
last_update datetime2,
modification_counter bigint,
[rows] bigint,
rows_sampled bigint,
[% Changed] bigint NULL, 
[Statement] varchar(max),
Processed datetime
)    
 
 ;WITH CTE
AS
(SELECT 
min(database_id) as database_id,
    DB_NAME( dbid ) AS DatabaseName, 
	 --SUM( size ) * 8,
    CAST( ( SUM( cast (size as bigint) ) * 8 ) / ( 1024.0 * 1024.0 ) AS decimal( 10, 2 ) ) AS DbSizeGb,
	min(recovery_Model_desc) as recovery_model_desc
	 
FROM 
    sys.sysaltfiles  f
	inner join sys.databases d on f.dbid=d.database_id
GROUP BY 
    DB_NAME( dbid )
)
INSERT INTO @DATABASES
select DatabaseName from CTE 
WHERE DatabaseName not in ('model','tempdb')
ORDER BY recovery_model_desc,DbSizeGb desc
   
DECLARE Cursor_Statistics CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR   
SELECT name from @DATABASES order BY ID 
    
OPEN Cursor_Statistics    
    
FETCH NEXT FROM Cursor_Statistics    
INTO @db    
    
WHILE @@FETCH_STATUS = 0    
BEGIN    
 SELECT @cmd = 'USE ['+@db+'];    
 INSERT INTO #DBA_STATISTICS_STATUS_V3
 SELECT 
	db_name() as DatabaseName,
	schema_name(so.schema_id) as SchemaName,
	object_name(stat.object_id) as ObjactName,	
		stat.auto_created,
		stat.name as stats_name,
		STUFF((SELECT '', '' + cols.name
			FROM sys.stats_columns AS statcols
			JOIN sys.columns AS cols ON
				statcols.column_id=cols.column_id
				AND statcols.object_id=cols.object_id
			WHERE statcols.stats_id = stat.stats_id and
				statcols.object_id=stat.object_id
			ORDER BY statcols.stats_column_id
			FOR XML PATH(''''), TYPE
		).value(''.'', ''NVARCHAR(MAX)''), 1, 2, '''')  as stat_cols,
		stat.stats_id,
		stat.filter_definition,
		stat.is_temporary,
		stat.no_recompute,
		sp.last_updated,
		sp.modification_counter,
		sp.rows,
		sp.rows_sampled,
		modification_counter*100/rows as [% Changed]
		,''USE [''+db_name()+'']; UPDATE STATISTICS [''+schema_name(so.schema_id)+''].[''+object_name(stat.object_id)+''] [''+	stat.name +''] '+@options+''' as [Statement]
		,NULL


	from sys.stats (nolock) as stat
	CROSS APPLY sys.dm_db_stats_properties (stat.object_id, stat.stats_id) AS sp
	JOIN sys.objects (nolock) as so on stat.object_id=so.object_id
	JOIN sys.schemas (nolock) as sc on so.schema_id=sc.schema_id

	--WHERE  [rows] > 2000 and (datediff(dd,last_updated,getdate()) > 3 or (modification_counter*100/rows) > 10) 
	ORDER BY [rows]  desc;'
 
 --PRINT @cmd    
 BEGIN TRY 
 --PRINT @cmd    
  EXEC (@cmd)    
 END TRY    
 BEGIN CATCH    
  SELECT     
  @cmd as [Statement],    
        ERROR_NUMBER() AS ErrorNumber,    
        ERROR_SEVERITY() AS ErrorSeverity,    
        ERROR_STATE() as ErrorState,    
        ERROR_PROCEDURE() as ErrorProcedure,    
        ERROR_LINE() as ErrorLine,    
        ERROR_MESSAGE() as ErrorMessage;    
       
 END CATCH;    
    
 FETCH NEXT FROM Cursor_Statistics    
INTO @db    
END    
    
CLOSE Cursor_Statistics    
DEALLOCATE Cursor_Statistics    


select database_name,count(*)
 from #DBA_STATISTICS_STATUS_V3  (NOLOCK) 

   WHERE  [rows] > 2000 and (datediff(dd,last_update,getdate()) > @DaysBack or [% Changed] > @PercModif) 
   GROUP BY database_name

select * from #DBA_STATISTICS_STATUS_V3
    WHERE  [rows] > 2000 and (datediff(dd,last_update,getdate()) > @DaysBack or [% Changed] > @PercModif) 

--------------------------------------------------------------------------------------------------------------------------------------------------------------------
--DECLARE  @DaysBack int = 4, @PercModif int = 10 , @db sysname,  @cmd varchar(4000)   
DECLARE @id int

DECLARE Cursor_Stmt CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR	
	select ID,[Statement] from #DBA_STATISTICS_STATUS_V3
    WHERE  [rows] > 2000 and (datediff(dd,last_update,getdate()) > @DaysBack or [% Changed] > @PercModif) 
	and Processed IS NULL
	ORDER BY ID

OPEN Cursor_Stmt

FETCH NEXT FROM Cursor_Stmt
INTO @id,@cmd

WHILE @@FETCH_STATUS = 0
BEGIN
	
	--IF datepart(dw,getdate()) in (7,1) /*Saturday or Sunday*/or ((datepart(dw,getdate())=6 and datepart(hh,getdate()) >=19)) /*Firday after 7 PM*/or ((datepart(dw,getdate())=2 and datepart(hh,getdate()) < 7))/*Monday before 7 AM*/
	BEGIN
	BEGIN TRY 
		PRINT @cmd
		EXEC (@cmd)
		UPDATE #DBA_STATISTICS_STATUS_V3
		SET Processed=getdate()
		WHERE ID=@id   
	  
	END TRY    
	BEGIN CATCH    
		SELECT     
		@cmd as [Statement],    
			ERROR_NUMBER() AS ErrorNumber,    
			ERROR_SEVERITY() AS ErrorSeverity,    
			ERROR_STATE() as ErrorState,    
			ERROR_PROCEDURE() as ErrorProcedure,    
			ERROR_LINE() as ErrorLine,    
			ERROR_MESSAGE() as ErrorMessage;    
       
	END CATCH;  

	FETCH NEXT FROM Cursor_Stmt
	INTO @id,@cmd
	END
	--ELSE
	--BEGIN
	--	SELECT 'Exiting due timestamp '+@@servername+' '+cast(getdate() as varchar(30))
	--	BREAK
	--END
END

CLOSE Cursor_Stmt
DEALLOCATE Cursor_Stmt
DECLARE @email_info varchar(100)
SELECT @email_info=@@servername+' Update Stats - COMPLETED' 
EXEC msdb.dbo.sp_send_dbmail 
  @recipients=@email, 	
  @subject = @email_info ,  
  @body = @email_info,  
  @importance = 'HIGH',  
  @body_format = 'TEXT' ; 
  GO


