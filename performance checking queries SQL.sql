--performance checking queries
--Finding the TOP N queries
SELECT TOP 5 query_stats.query_hash AS "Query Hash",   
    SUM(query_stats.total_worker_time) / SUM(query_stats.execution_count) AS "Avg CPU Time",  
    MIN(query_stats.statement_text) AS "Statement Text"  
FROM   
    (SELECT QS.*,   
    SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,  
    ((CASE statement_end_offset   
        WHEN -1 THEN DATALENGTH(ST.text)  
        ELSE QS.statement_end_offset END   
            - QS.statement_start_offset)/2) + 1) AS statement_text  
     FROM sys.dm_exec_query_stats AS QS  
     CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST) as query_stats  
GROUP BY query_stats.query_hash  
ORDER BY 2 DESC;



--checking latency
SELECT
GETDATE() AS collection_time,
d.name AS [Database],
f.physical_name AS [File],
(fs.num_of_bytes_read / 1024.0 / 1024.0) [Total MB Read],
(fs.num_of_bytes_written / 1024.0 / 1024.0) AS [Total MB Written],
(fs.num_of_reads + fs.num_of_writes) AS [Total I/O Count],
fs.io_stall AS [Total I/O Wait Time (ms)],
fs.size_on_disk_bytes / 1024 / 1024 AS [Size (MB)]
INTO #past_coll_time
FROM sys.dm_io_virtual_file_stats(default, default) AS fs
INNER JOIN sys.master_files f 
ON fs.database_id = f.database_id 
AND fs.file_id = f.file_id
INNER JOIN sys.databases d 
ON d.database_id = fs.database_id;

WAITFOR DELAY '00:05:00';

SELECT
GETDATE() AS collection_time,
d.name AS [Database],
f.physical_name AS [File],
(fs.num_of_bytes_read / 1024.0 / 1024.0) [Total MB Read],
(fs.num_of_bytes_written / 1024.0 / 1024.0) AS [Total MB Written],
(fs.num_of_reads + fs.num_of_writes) AS [Total I/O Count],
fs.io_stall AS [Total I/O Wait Time (ms)],
fs.size_on_disk_bytes / 1024 / 1024 AS [Size (MB)]
INTO #curr_coll_time
FROM sys.dm_io_virtual_file_stats(default, default) AS fs
INNER JOIN sys.master_files f 
ON fs.database_id = f.database_id 
AND fs.file_id = f.file_id
INNER JOIN sys.databases d 
ON d.database_id = fs.database_id;

SELECT
cur.[Database],
cur.[File] AS [File Name],
CONVERT (numeric(28,1), (cur.[Total MB Read] - prev.[Total MB Read]) 
* 1000 /DATEDIFF (millisecond, prev.collection_time, cur.collection_time)) 
AS [MB/sec Read],
CONVERT (numeric(28,1), (cur.[Total MB Written] - prev.[Total MB Written]) 
* 1000 / DATEDIFF (millisecond, prev.collection_time, cur.collection_time)) 
AS [MB/sec Written],
CONVERT (numeric(28,1), (cur.[Total I/O Wait Time (ms)] 
- prev.[Total I/O Wait Time (ms)]) ) AS [Total IO Stall (ms)],

-- protect from div-by-zero

CASE
WHEN (cur.[Total I/O Count] - prev.[Total I/O Count]) = 0 THEN 0
ELSE
(cur.[Total I/O Wait Time (ms)] - prev.[Total I/O Wait Time (ms)])
/ (cur.[Total I/O Count] - prev.[Total I/O Count])
END AS [Response Time (ms per transaction)]
INTO #response_time_cal
FROM #curr_coll_time AS cur
INNER JOIN #past_coll_time AS prev 
ON prev.[Database] = cur.[Database] 
AND prev.[File] = cur.[File];

SELECT *
FROM #response_time_cal
WHERE [Response Time (ms per transaction)] > 0;

DROP table #response_time_cal;

DROP TABLE #curr_coll_time;

DROP TABLE #past_coll_time;


--disk wait stats
select * from sys.dm_os_wait_stats dows
order by dows.wait_time_ms desc

select * from sys.dm_os_waiting_tasks dowt
where dowt.wait_type like 'PAGEIO%'

SELECT * FROM sys.dm_io_virtual_file_stats(null,null) a
order by a.io_stall desc


--buffer checking


SELECT object_name, counter_name, cntr_value
FROM sys.dm_os_performance_counters
WHERE [object_name] LIKE '%Buffer Manager%'
AND [counter_name] = 'Page writes/sec'

 --memory check
SELECT
	physical_memory_kb,
	virtual_memory_kb,
	committed_kb,
	committed_target_kb
FROM sys.dm_os_sys_info;

--buffer cache
SELECT
    databases.name AS database_name,
    COUNT(*) * 8 / 1024 AS mb_used
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.databases
ON databases.database_id = dm_os_buffer_descriptors.database_id
GROUP BY databases.name
ORDER BY COUNT(*) DESC;

--page life expectancy
SELECT *
FROM sys.dm_os_performance_counters  
WHERE counter_name = 'Page life expectancy'
AND OBJECT_NAME = 'SQLServer:Buffer Manager'

--tables and dbuffer use
SELECT
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY objects.name,
		 objects.type_desc
ORDER BY COUNT(*) DESC;

--tables and dbuffer use details
SELECT
	indexes.name AS index_name,
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
INNER JOIN sys.indexes
ON objects.object_id = indexes.object_id
AND partitions.index_id = indexes.index_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY indexes.name,
		 objects.name,
		 objects.type_desc
ORDER BY COUNT(*) DESC;
 


--- table buffer use further details
WITH CTE_BUFFER_CACHE AS (
	SELECT
		objects.name AS object_name,
		objects.type_desc AS object_type_description,
		objects.object_id,
		COUNT(*) AS buffer_cache_pages,
		COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
	FROM sys.dm_os_buffer_descriptors
	INNER JOIN sys.allocation_units
	ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
	INNER JOIN sys.partitions
	ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
	OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
	INNER JOIN sys.objects
	ON partitions.object_id = objects.object_id
	WHERE allocation_units.type IN (1,2,3)
	AND objects.is_ms_shipped = 0
	AND dm_os_buffer_descriptors.database_id = DB_ID()
	GROUP BY objects.name,
			 objects.type_desc,
			 objects.object_id)
SELECT
	PARTITION_STATS.name,
	CTE_BUFFER_CACHE.object_type_description,
	CTE_BUFFER_CACHE.buffer_cache_pages,
	CTE_BUFFER_CACHE.buffer_cache_used_MB,
	PARTITION_STATS.total_number_of_used_pages,
	PARTITION_STATS.total_number_of_used_pages * 8 / 1024 AS total_mb_used_by_object,
	CAST((CAST(CTE_BUFFER_CACHE.buffer_cache_pages AS DECIMAL) / CAST(PARTITION_STATS.total_number_of_used_pages AS DECIMAL) * 100) AS DECIMAL(5,2)) AS percent_of_pages_in_memory
FROM CTE_BUFFER_CACHE
INNER JOIN (
	SELECT 
		objects.name,
		objects.object_id,
		SUM(used_page_count) AS total_number_of_used_pages
	FROM sys.dm_db_partition_stats
	INNER JOIN sys.objects
	ON objects.object_id = dm_db_partition_stats.object_id
	WHERE objects.is_ms_shipped = 0
	GROUP BY objects.name, objects.object_id) PARTITION_STATS
ON PARTITION_STATS.object_id = CTE_BUFFER_CACHE.object_id
ORDER BY CAST(CTE_BUFFER_CACHE.buffer_cache_pages AS DECIMAL) / CAST(PARTITION_STATS.total_number_of_used_pages AS DECIMAL) DESC;


--buffer free space check
WITH CTE_BUFFER_CACHE AS
( SELECT
  databases.name AS database_name,
  COUNT(*) AS total_number_of_used_pages,
  CAST(COUNT(*) * 8 AS DECIMAL) / 1024 AS buffer_cache_total_MB,
  CAST(CAST(SUM(CAST(dm_os_buffer_descriptors.free_space_in_bytes AS BIGINT)) AS DECIMAL) / (1024 * 1024) AS DECIMAL(20,2))  AS buffer_cache_free_space_in_MB
 FROM sys.dm_os_buffer_descriptors
 INNER JOIN sys.databases
 ON databases.database_id = dm_os_buffer_descriptors.database_id
 GROUP BY databases.name)
SELECT
 *,
 CAST((buffer_cache_free_space_in_MB / NULLIF(buffer_cache_total_MB, 0)) * 100 AS DECIMAL(5,2)) AS buffer_cache_percent_free_space
FROM CTE_BUFFER_CACHE
ORDER BY buffer_cache_free_space_in_MB / NULLIF(buffer_cache_total_MB, 0) DESC

 --percent of IO by database
WITH AggregateIOStatistics
AS
(SELECT DB_NAME(database_id) AS [DB Name],
CAST(SUM(num_of_bytes_read + num_of_bytes_written)/1048576 AS DECIMAL(12, 2)) AS io_in_mb
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS [DM_IO_STATS]
GROUP BY database_id)
SELECT ROW_NUMBER() OVER(ORDER BY io_in_mb DESC) AS [I/O Rank], [DB Name], io_in_mb AS [Total I/O (MB)],
       CAST(io_in_mb/ SUM(io_in_mb) OVER() * 100.0 AS DECIMAL(5,2)) AS [I/O Percent]
FROM AggregateIOStatistics
ORDER BY [I/O Rank] 



---read/writes of queries

SELECT TOP 25 execution_count, plan_generation_num, last_execution_time,

        total_worker_time, last_worker_time, min_worker_time, max_worker_time,

        total_logical_reads, last_logical_reads, min_logical_reads,  max_logical_reads,

        total_physical_reads, last_physical_reads, min_physical_reads,  max_physical_reads,

        total_logical_writes, last_logical_writes, min_logical_writes, max_logical_writes,

        total_elapsed_time, last_elapsed_time, min_elapsed_time, max_elapsed_time,

        (SUBSTRING(s2.text,  statement_start_offset / 2, ( (CASE WHEN statement_end_offset = -1 THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2) ELSE statement_end_offset END)  - statement_start_offset) / 2)  )  AS sql_statement,

        text, p.query_plan

FROM sys.dm_exec_query_stats qs

     CROSS APPLY sys.dm_exec_sql_text(qs.plan_handle) s2

     CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) P

ORDER BY total_physical_reads DESC