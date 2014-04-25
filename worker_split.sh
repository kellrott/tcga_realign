#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
STOP_FLAGFILE="$BASEDIR/stop_split.flag"

while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi

	echo "Scanning"
	UUID=`$SYN_MONITOR getAssignmentForWork ucsc_biofarm downloaded`
	if [ $? != 0 ]; then 
		echo "Done, sleeping"
		#exit 0
		sleep 60
	else
		echo Splitting $UUID
    	if [ -e /pancanfs*/input/$UUID ]; then
    		BAM_FILE=`ls /pancanfs*/input/$UUID/*.bam`
    		BAM_DIR=`dirname $BAM_FILE`
    		INPUT_DIR=`dirname $BAM_DIR`
    		WORK_VOLUME=`dirname $INPUT_DIR`
			$BASEDIR/job_split.sh $WORK_VOLUME $UUID
    		if [ $? != 0 ]; then 
				$SYN_MONITOR errorAssignment $UUID "splitting error"
			else
				$SYN_MONITOR returnAssignment $UUID		
			fi
		fi
	fi
done
