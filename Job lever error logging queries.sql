  
  SELECT *
  FROM [msdb].[dbo].[sysjobhistory]
 
 SELECT *
 FROM msdb.dbo.sysjobhistory JOIN msdb.dbo.sysjobs ON sysjobhistory.job_id = sysjobs.job_id


  SELECT *
  FROM [msdb].[dbo].[sysjobs]

  SELECT * FROM msdb.dbo.sysjobsteps


--select the job name , failed steps according to the job name
DECLARE @jobNames VARCHAR(100) = 'ErrorLoggingEmail,ETL'; -- set the job names which want to be tracked comma seperated
DECLARE @backDate INT = 1; --set the number of days to track back

SELECT job_history.instance_id ,job_history.server ,job.name AS job_name , job_history.step_name AS step_name ,job_history.message ,msdb.dbo.agent_datetime(job_history.run_date,job_history.run_time) AS run_date, job_history.step_id ,
	CASE 
		WHEN job_history.run_status = 0 THEN 'Failed'
		WHEN job_history.run_status = 1 THEN 'Success'
		WHEN job_history.run_status = 2 THEN 'Retry'
		WHEN job_history.run_status = 3 THEN 'Cancelled'
		WHEN job_history.run_status = 4 THEN 'Progress'
		ELSE 'Undefined'
	END AS step_status -- step status defined according to microsoft documentation
	,(SELECT value
	FROM STRING_SPLIT((SELECT value FROM STRING_SPLIT(steps.command ,'/') WHERE value LIKE '%.dtsx%'),'\' )
	WHERE value LIKE '%dtsx') AS package_name 
	,SUBSTRING(job_history.message, NULLIF(CHARINDEX('Execution ID: ', job_history.message),0)+14 ,PATINDEX('%[^0-9]%',SUBSTRING(job_history.message, NULLIF(CHARINDEX('Execution ID: ', job_history.message),0)+14 ,20))-1) AS execution_id
	, execution_events.message AS execution_message
	, executions.status AS execution_status
	, execution_events.event_name AS execution_event_name
	, execution_events.message_source_name AS failed_component
	, execution_events.execution_path AS execution_path


FROM msdb.dbo.sysjobhistory AS job_history
	  LEFT JOIN msdb.dbo.sysjobs AS job ON job.job_id = job_history.job_id
	  LEFT JOIN msdb.dbo.sysjobsteps AS steps ON   steps.job_id = job_history.job_id
	  LEFT JOIN SSISDB.catalog.executions AS executions ON SUBSTRING(job_history.message, NULLIF(CHARINDEX('Execution ID: ', job_history.message),0)+14 ,PATINDEX('%[^0-9]%',SUBSTRING(job_history.message, NULLIF(CHARINDEX('Execution ID: ', job_history.message),0)+14 ,20))-1) = executions.execution_id
	  LEFT JOIN SSISDB.catalog.event_messages AS execution_events ON executions.execution_id = execution_events.operation_id

WHERE job.name IN ( SELECT value AS PackgeName
					FROM STRING_SPLIT(@jobNames ,',' )) 
AND job_history.run_date >= cast(convert(varchar(8),getdate(),112) as int)-@backDate -- how many  days before
AND job_history.run_status <> 1
 --get only failed executions
 AND ( execution_events.event_name = 'OnError' OR  execution_id IS NULL)
--AND steps.subsystem ='SSIS' 
--ORDER BY run_date--,package_name

 ;







