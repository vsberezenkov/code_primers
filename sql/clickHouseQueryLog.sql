SELECT type
,event_date
,event_time
,round (query_duration_ms/1000/60, 2) as query_duration_min
,substring (toString (toDateTime (query_duration_ms/1000, 'Etc/UTC' )),12,8) query_duration 
,read_rows
,formatReadableSize(read_bytes) as read_Mb
,formatReadableSize(memory_usage) as RAM_usage_Mb
,result rows
,formatReadableSize(result_bytes) as result
,current_database
,query
,query_kind
,tables
,'user'
,os_user
FROM system.query_log
WHERE 1=1
 --and query not like '%query_log%' 
 and query not like '%system%'
 and user = 'airflow'
and type = 'QueryFinish'
order by event time desc
;