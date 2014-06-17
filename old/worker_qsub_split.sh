#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

. $BASEDIR/align.conf

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
STOP_FLAGFILE="$BASEDIR/stop_split.flag"

while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi

	echo "Scanning"
	UUID=`$SYN_MONITOR getAssignmentForWork $ASSIGNEE downloaded`
	if [ $? != 0 ]; then 
		echo "Done, sleeping"
		#exit 0
		sleep 60
	else
		echo Splitting $UUID
		BAM_DIR=`$BASEDIR/find_dir.sh input $UUID`
    	if [ $? == 0 ]; then
    		INPUT_DIR=`dirname $BAM_DIR`
    		WORK_VOLUME=`dirname $INPUT_DIR`
    		pushd $BASEDIR
    		CMD="qsub qsub_split.sh $WORK_VOLUME $UUID"
    		echo "Running" $CMD
			$CMD
			popd    		
		else
			echo $SYN_MONITOR errorAssignment $UUID "can't find input files"
		fi 
	fi
done
