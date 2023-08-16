SELECT user
    ,query_id
    ,formatReadableTimeDelta(elapsed) as elapsed_time 
    ,read_rows
    ,formatReadableSize(read_bytes) as read_memory
    ,formatReadableSize(memory_usage) as memory_use
    ,formatReadableSize(peak_memory_usage) as peak_memory 
    ,query
FROM system.processes
WHERE 1=1
--and user= 'airflow'
and query not like '%system.processes%'
order by elapsed desc