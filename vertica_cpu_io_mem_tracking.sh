#!/bin/bash

WDIR=$(pwd)

STIME=`date +%s`
LOGDATE=`date +%Y%m%d`

#LOGFILE=${WDIR}/vertica_cpu_io_mem_tracking_${LOGDATE}.log

#rm -f ${LOGFILE} 2>/dev/null

echo "Start the job at `date`...."

# Change the following param to a small value when testing

MAXSEQ=1000000

for i in `seq 0 ${MAXSEQ}`
do

N=$(($i+1))

echo "Start tracking job $N...." 


vsql -U ? -w ??? <<!

set search_path to vertica_stats;

insert into cpu_io_disk 
Select DATE_TRUNC('MINUTE',B.start_time)::TIMESTAMP(0) As START_TIME, MAX(C.avg_latency_ms) As Disk_Latency_ms, MAX(B.IO_Wait_Percent) As IO_Wait_Percent,
MAX(B.average_cpu_usage_percent) As Avg_CPU_Percent,
MAX((round(case when C.perc>=100 then 100 else C.perc end, 1.0))) as Disk_Utilization_Percent
From ( Select node_name,
   timestamp_trunc(start_time, 'MI') as start_time,
   timestamp_trunc(end_time, 'MI') as end_time,
   round((CAST(Actual_IO_Wait_Time*1.00/Total_CPU_Time As DEC(4,2))*100),1.0) As IO_Wait_Percent,
   round((CAST(Actual_Idle_CPU_Time*1.00/Total_CPU_Time As DEC(4,2))*100),1.0) As Idle_CPU_Percent,
   round((100 - CAST(Actual_Idle_CPU_Time*1.00/Total_CPU_Time As DEC(4,2))*100),1.0) As average_cpu_usage_percent
From (
SELECT
   node_name,
   start_time,
   end_time,
   (io_wait_microseconds_end_value - io_wait_microseconds_start_value )//(1000*1000) As Actual_IO_Wait_Time,
   (idle_microseconds_end_value - idle_microseconds_start_value)//(1000*1000) As Actual_Idle_CPU_Time,
   (user_microseconds_end_value + nice_microseconds_end_value + system_microseconds_end_value
             + idle_microseconds_end_value + io_wait_microseconds_end_value + irq_microseconds_end_value
             + soft_irq_microseconds_end_value + steal_microseconds_end_value + guest_microseconds_end_value
             - user_microseconds_start_value - nice_microseconds_start_value - system_microseconds_start_value
             - idle_microseconds_start_value - io_wait_microseconds_start_value - irq_microseconds_start_value
             - soft_irq_microseconds_start_value - steal_microseconds_start_value - guest_microseconds_start_value)//( 1000*1000) As Total_CPU_Time
FROM v_internal.dc_cpu_aggregate_by_minute ) As A ) As B
  INNER JOIN
( select timestamp_trunc(start_time,'MI') as start_time,
                          node_name ,
        max((( total_read_mills_end_value-total_read_mills_start_value + total_written_mills_end_value-total_written_mills_start_value )/NULLIFZERO(total_reads_end_value+total_writes_end_value-total_reads_start_value-total_writes_start_value))::DEC(8,2)) As avg_latency_ms ,
        max((total_ios_mills_end_value-total_ios_mills_start_value)/((extract('epoch' from end_time)-extract('epoch' from start_time))*1000))*100 as perc
from dc_io_info_by_minute
group by timestamp_trunc(start_time, 'MI'), node_name ) As C
ON ( B.node_name = C.node_name
 AND B.start_time = C.start_time )
where B.start_time BETWEEN  CLOCK_TIMESTAMP() - INTERVAL '5 MINUTES' AND CLOCK_TIMESTAMP()
GROUP BY 1
ORDER BY 1
;
commit;

\! echo ''


insert into concurrency_memory
SELECT A.Meaure_Time, A.Concurrency_Level, ((CASE When Memory_In_Use_KB_General > 0 Then (Memory_In_Use_KB_General+Memory_In_Use_KB_Other)
ELSE (Memory_Borrowed_KB_Other+Memory_In_Use_KB_Other) END)/1024/1024)::INT As Total_Memory_In_Use_GB
FROM (
select CLOCK_TIMESTAMP()::TIMESTAMP(0) as Meaure_Time, SUM(running_query_count) As Concurrency_Level,
SUM(CASE When pool_name='general' then memory_inuse_kb Else 0 END) As Memory_In_Use_KB_General,
SUM(CASE When pool_name <> 'general' then memory_inuse_kb Else 0 END) As Memory_In_Use_KB_Other,
SUM(CASE When pool_name <> 'general' then general_memory_borrowed_kb Else 0 END) As Memory_Borrowed_KB_Other,
SUM(CASE When pool_name='tm' Then running_query_count Else 0 END) As Total_TM_Job
from resource_pool_status
where running_query_count > 0
and (node_name ilike '%0001')
group by 1 ) As A
order by 1
;
commit;


\! echo ''

Insert Into resource_pool_status_hist
select CLOCK_TIMESTAMP()::TIMESTAMP(0) As MEASURE_Time, pool_name, node_name, 
 (memory_inuse_kb+general_memory_borrowed_kb) As Used_Memory_KB,  running_query_count
from resource_pool_status 
Where running_query_count > 0
;
commit;

!


echo "End tracking job $N...." 

sleep 300

done



ETIME=`date +%s`
RT=`expr $ETIME - $STIME`
echo
echo "Job is done in $RT seconds."
echo

exit 0

