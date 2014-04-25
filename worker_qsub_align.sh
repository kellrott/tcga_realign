#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

. $BASEDIR/align.conf

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
STOP_FLAGFILE="$BASEDIR/stop_align.flag"

while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi

	echo "Scanning"
	UUID=`$SYN_MONITOR getAssignmentForWork $ASSIGNEE split`
	if [ $? != 0 ]; then 
		echo "Done, sleeping"
		#exit 0
		sleep 60
	else
		echo Splitting $UUID
		BAM_DIR=`$BASEDIR/find_dir.sh splits $UUID`
    	if [ $? == 0 ]; then
    		INPUT_DIR=`dirname $BAM_DIR`
    		WORK_VOLUME=`dirname $INPUT_DIR`
    		pushd $BASEDIR
			CMD="qsub qsub_align.sh $WORK_VOLUME $UUID"
			echo Running $CMD
			$CMD
			popd    		
		else
			echo "Can't find split $UUID" $BAM_DIR 
		fi
	fi
done
