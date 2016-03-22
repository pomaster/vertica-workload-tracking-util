#!/bin/bash
#### Po Hong, HP Big Data Presales
#### Created: August 18, 2015
#### Updated: Feb 17, 2015

if [ $# -ne 2 ]; then
  echo "usage: enter start_time in the form of yyyy-mm-dd hh24:mi:ss...."
  echo "usage: enter end_time in the form of yyyy-mm-dd hh24:mi:ss...."
  exit 1
fi

start_time=$1
end_time=$2

STIME=`date +%s`

echo "Start job at `date`...." 

vsql -U ? -w ? <<EOF

\timing

set search_path='vertica_stats';

\! echo "Query to find the total/average/max runtime for each job category:"

select request_type, count(*), MIN(start_timestamp), MAX(start_timestamp), SUM(request_duration_ms)//1000 As Total_Runtime, 
AVG(request_duration_ms)//1000 As AVG_Runtime, MIN(request_duration_ms)//1000 As MIN_Runtime, MAX(request_duration_ms)//1000 As MAX_Runtime
from query_requests 
where success='t'
and start_timestamp BETWEEN '${start_time}' AND '${end_time}'
group by 1 order by 1;

\! echo ''

\! echo "Resource pool query latency:"


SELECT pool_name, count(*) As Query_Count, MIN(START_TIME) As MIN_StartTime, MAX(START_TIME) As MAX_StartTime, SUM(Runtime_Sec)::INT As Total_Runtime, 
AVG(Runtime_Sec)::INT As Avg_Runtime, SUM(Waiting_sec)::INT As Total_Wait_Time, (((SUM(Waiting_sec)/NULLIFZERO(SUM(Runtime_Sec)))::DEC(4,2))*100)::INT As Wait_Percent
FROM ( 
select DATE_TRUNC('SECOND',start_timestamp)::TIMESTAMP(0) As START_TIME, B.pool_name, 
(A.request_duration_ms/1000) As Runtime_Sec, A.memory_acquired_mb
, DATEDIFF('SECOND', B.Resource_Start_Time, B.Resource_Grant_Time ) as Waiting_sec 
from query_requests A
     INNER JOIN
     ( select          transaction_id,
                       statement_id,
                       min(start_time) As Resource_Start_Time,
                       max ("time") As Resource_Grant_Time,
                       (max(memory_kb)/1024)::INT as memory_mb,
                       max(pool_name) as pool_name
                from dc_resource_acquisitions
                where result = 'Granted'
                and request_type <> 'AcquireAdditional'
                group by 1,2 ) B
 ON ( A.transaction_id=B.transaction_id and A.statement_id=B.statement_id )
where success='t'
and A.request_type IN ('LOAD','QUERY', 'DDL')
and B.pool_name ILIKE '%'
and A.request_duration_ms > 0 ) C
WHERE START_TIME BETWEEN '${start_time}' AND '${end_time}'
Group By 1
Order By 1;

\! echo ''

\! echo "Calculate min, avg and max runtime for queries in different memory categories:"  


select request_type, ( CASE When memory_acquired_mb BETWEEN 0 and 500 Then 'SMALL'
              When memory_acquired_mb BETWEEN 500.01 and 2000 Then 'MEDIUM' 
              When memory_acquired_mb BETWEEN 2000.01 and 16000 Then 'LARGE' 
              When memory_acquired_mb > 16000 Then 'HUGE' ELSE NULL END) As QUERY_TYPE,
            count(*), MIN(start_timestamp), MAX(start_timestamp), (MIN(request_duration_ms)//1000)::INT As MIN_Runtime_Sec, 
            (AVG(request_duration_ms)//1000)::INT As AVG_Runtime_Sec,
            (MAX(request_duration_ms)//1000)::INT As MAX_Runtime_Sec
from query_requests 
where success='t'
and start_timestamp BETWEEN '${start_time}' AND '${end_time}'
and request_type IN ('LOAD','QUERY', 'DDL')
and memory_acquired_mb > 0
and request_duration_ms > 0
group by 1,2
order by 1,2;


\! echo ''

\! echo "Use the following to do runtime and memory bucket/histogram analysis:"

WITH temp_detail AS (
  SELECT
    transaction_id
    ,statement_id
    ,MAX(memory_acquired_mb::INT) As mem_mb_per_node
    ,MAX(request_duration_ms)/1000::INT As quey_runtime_sec
  FROM query_requests
  WHERE START_TIMESTAMP BETWEEN '${start_time}' AND '${end_time}'
  AND SUCCESS='t' and REQUEST_TYPE IN ('DDL', 'LOAD', 'QUERY')
  AND memory_acquired_mb > 0
  AND request_duration_ms > 0
  GROUP BY 1,2
)
SELECT
  MAX(mem_mb_per_node)::INT AS max_mem_mb,
  AVG(mem_mb_per_node)::INT AS avg_mem_mb,
  MIN(mem_mb_per_node)::INT AS min_mem_mb,
  COUNT(1),
  WIDTH_BUCKET (
    mem_mb_per_node
    ,0
    ,4*1024
    ,10
  ) AS memory_bucket ,
 MAX(quey_runtime_sec)::INT As Max_quey_runtime_sec,
 AVG(quey_runtime_sec)::INT As Avg_quey_runtime_sec,
 MIN(quey_runtime_sec)::INT As Min_quey_runtime_sec,
 WIDTH_BUCKET (
    quey_runtime_sec
    ,0
    ,100
    ,10
  ) AS runtime_bucket
FROM temp_detail
GROUP BY memory_bucket,runtime_bucket
ORDER BY memory_bucket,runtime_bucket
;

\! echo ''

\! echo "Resource usage high-pole analysis:"  

select description, max(assigned_parallelism) as max_thread, sum(assigned_memory_bytes*assigned_parallelism) mem
from dc_plan_resources pr join dc_plan_parallel_zones ppz  on ( pr.plan_id=ppz.plan_id and pr.parallel_zone_id = ppz.parallel_zone_id
and pr.transaction_id = ppz.transaction_id and pr.statement_id = ppz.statement_id )
--where pr."time" BETWEEN '${start_time}' AND '${end_time}'
group by description
order by mem DESC, description
LIMIT 10;


\! echo ''

\! echo "Use the following to check in-memory Vertica catalog size:"  


Select MIN(catalog_size) As MIN_Cat_Size, AVG(catalog_size) As AVG_Cat_Size, MAX(catalog_size) as MAX_Cat_Size, NOW() As Query_Time
from ( SELECT
  a.node_name
  , b.time
  , SUM( a.total_memory - a.free_memory ) AS catalog_size
 FROM dc_allocation_pool_statistics a INNER JOIN (
  SELECT
    node_name
    , date_trunc('SECOND', max( time )) AS time
  FROM dc_allocation_pool_statistics
  GROUP BY 1
) b
ON a.node_name = b.node_name
  AND date_trunc('SECOND', a.time) = b.time GROUP BY 1,2 ) A;

\! echo ''

\! echo "Check query retries:"

select reason_for_retry, count(*) from dc_requests_retried 
where  "time" BETWEEN '${start_time}' AND '${end_time}'
group by 1 order by 1;


\! echo ''

\! echo "Cascading pool movement:"


select source_pool_name, target_pool_name, RESULT_REASON, count( DISTINCT (100*transaction_id+statement_id)), MAX("time")
from dc_resource_pool_move
where "time" BETWEEN '${start_time}' AND '${end_time}'
group by 1,2,3
order by 1,2,3;

\! echo ''

\! echo "Interesting events in the cluster:"

Select event_code_description, event_severity, MAX(substr(TRIM(event_problem_description),1,100)) As Event_Desc, 
MIN(event_posted_timestamp) As Min_Event_Time, MAX(event_posted_timestamp) As Max_Event_Time, count(*)
From  MONITORING_EVENTS
Where  event_posted_timestamp BETWEEN '${start_time}' AND '${end_time}'
Group By 1,2
Order By count(*) DESC
LIMIT 10
;

\! echo ''

\! echo "Things that should be fast but slow:"

Select event_description, count(*), MAX(threshold_us/1000)::INT As Max_threshold_ms, MAX(duration_us/1000)::INT As Max_duration_ms
From dc_slow_events
Where "time" BETWEEN '${start_time}' AND '${end_time}'
Group By 1
Order By count(*) DESC
LIMIT 5;

\! echo ''

\! echo "Total exec time for different users:"


WITH resource_detail AS (
  SELECT user_name
    ,transaction_id
    ,statement_id
    ,Count(*)
  FROM query_requests
  WHERE START_TIMESTAMP BETWEEN '${start_time}' AND '${end_time}'
  AND SUCCESS='t' and REQUEST_TYPE IN ('DDL', 'LOAD', 'QUERY')
  GROUP BY 1,2,3
)
Select m.user_name, (SUM(counter_value)/1000/1000)::INT As Total_Exec_Time_Sec
From resource_detail m INNER JOIN execution_engine_profiles eep
   ON (m.transaction_id=eep.transaction_id and m.statement_id=eep.statement_id)
Where ( eep.node_name ILIKE '%0001' or eep.node_name ILIKE '%0016' )
And eep.counter_name='execution time (us)'
Group By 1
Order By 1
;

\! echo ''

\! echo "How is memory allocated in resource pools:"

Select pool_name,
       request_type,
       MAX(memory_kb)//1000 as memory_MB,
       count(DISTINCT (10000*transaction_id + statement_id)) As Query_Cnt
From dc_resource_acquisitions
Where "time" BETWEEN '${start_time}' AND '${end_time}'
Group By 1,2
Order By 1,2;

\! echo ''


\! echo "Find the cpu/io/disk usage metrics:"

Select *
From vertica_stats.cpu_io_disk
Where start_time BETWEEN '${start_time}' AND '${end_time}'
Order By start_time ASC;


\! echo ''


\! echo "Find the total Vertica concurrency and memory usage:"

Select *
From vertica_stats.concurrency_memory
Where Measure_time BETWEEN '${start_time}' AND '${end_time}'
Order By Measure_time ASC;


\! echo ''


\! echo "Find the metrics for cpu/io/concurrency/memory:"


Select A.*, B.*
From vertica_stats.cpu_io_disk A INNER JOIN vertica_stats.concurrency_memory B
  ON (date_trunc('MINUTE',A.start_time)=date_trunc('MINUTE',B.Measure_time) + INTERVAL '1 MINUTE')
Where A.start_time BETWEEN '${start_time}' AND '${end_time}'
And B.Measure_time BETWEEN '${start_time}' AND '${end_time}'
Order By 1;


\! echo ''


\! echo "Total Vertica memory usage and running query count:"

Select *
From vertica_stats.resource_pool_status_hist
Where Measure_time BETWEEN '${start_time}' AND '${end_time}'
Order By Measure_time ASC;


\! echo "Find the GCL(X) lock time info:"


WITH temp_detail AS (
  select transaction_id, DATEDIFF('SECOND',MIN(time), MAX(grant_time) ) As Duration_Sec
from dc_lock_releases 
where object_name ilike '%global%catalog%' and mode='X' 
and "time" BETWEEN '${start_time}' AND '${end_time}'
group by 1
Having DATEDIFF('SECOND',MIN(time), MAX(grant_time) ) >= 0
)
SELECT
 WIDTH_BUCKET (
    Duration_Sec
    ,0
    ,100
    ,20
  ) AS runtime_bucket,
 MIN(Duration_Sec)::INT As Min_Duration_Sec,
 MAX(Duration_Sec)::INT As Max_Duration_Sec,
 AVG(Duration_Sec)::INT As Avg_Duration_Sec,
 COUNT(*) As JOB_COUNT
FROM temp_detail
GROUP BY runtime_bucket
ORDER BY runtime_bucket
;


\! echo "Query queue length...."


select pool_name, count( DISTINCT (100*transaction_id+statement_id)) As Number_Q_Queue, MAX(MEMORY_REQUESTED_KB) As MAX_Mem_Requested
, MIN(QUEUE_ENTRY_TIMESTAMP)::TIMESTAMP(0) As MIN_Queue_Time, MAX(QUEUE_ENTRY_TIMESTAMP)::TIMESTAMP(0) As MAX_Queue_Time, MAX(CURRENT_TIMESTAMP)::TIMESTAMP(0) As Current_Time
from resource_queues
where QUEUE_ENTRY_TIMESTAMP BETWEEN '${start_time}' AND '${end_time}'
group by 1
order by 1
;

\! echo "Show the error messages...."

select substr(message,1,50) as msg, count(*), MIN(event_timestamp), MAX(event_timestamp) 
from error_messages 
where event_timestamp BETWEEN '${start_time}' AND '${end_time}'
 group by 1 order by 1;

\!

\q

EOF

ETIME=`date +%s`
RT=`expr $ETIME - $STIME`
echo "End job at `date`...." 
echo "Total time taken is $RT seconds."  


exit