#!/bin/bash

WDIR=$(pwd)

STIME=`date +%s`

echo "Start the job at `date`...."

# Change the following param to a small value when testing

MAXSEQ=12

for i in `seq 1 ${MAXSEQ}`
do

N=$(($i+1))
LOGDATE=`date +%Y%m%d`
LOGDIR=${WDIR}/${LOGDATE}

if [ ! -d ${LOGDIR} ]; then
mkdir ${LOGDIR}
fi

echo "Start tracking job $N...." 

ENDTIME=$(date +"%Y-%m-%d %H:%M:%S")
STARTTIME=$(date --date='-'3600' seconds' +"%Y-%m-%d %H:%M:%S")
echo "Start Time is: " ${STARTTIME}
echo "End Time is: " ${ENDTIME}

LOGSEQ=$(date +"%Y%m%d%H%M")
sh ${WDIR}/vertica_workload_metric_collect_71.sh "${STARTTIME}" "${ENDTIME}" > ${LOGDIR}/vertica_workload_metric_collect_${LOGSEQ}.log 2>&1

echo "End tracking job $N...." 

# Wake up after two hours

sleep 7200

done



ETIME=`date +%s`
RT=`expr $ETIME - $STIME`
echo
echo "Job is done in $RT seconds."
echo

exit 0
