#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

. $BASEDIR/align.conf

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
STOP_FLAGFILE="$BASEDIR/stop_download.flag"

while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi
	JOB_COUNT=`$SYN_MONITOR getAssignments $ASSIGNEE | grep -w downloading | grep -v error | wc -l`

	if [[ $JOB_COUNT < $MAX_DOWNLOAD ]]; then
		echo "Scanning"
		UUID=`$SYN_MONITOR getAssignmentForWork $ASSIGNEE todownload`
		if [ $? != 0 ]; then 
			echo "Done, sleeping"
			#exit 0
			sleep 60
		else
			echo Downloading $UUID
			BAM_DIR=`$BASEDIR/find_dir.sh input $UUID`
			if [ $? != 0 ]; then 
				VOLUME=`$BASEDIR/min_volume.sh`
				echo "Howdy" $VOLUME
			else
				INPUT_DIR=`dirname $BAM_DIR`
				VOLUME=`dirname $INPUT_DIR`
			fi
			
			pushd $BASEDIR
			CMD="qsub qsub_download.sh $VOLUME $UUID"
			echo "Running" $CMD
			$CMD
			popd
		fi
	else
		echo "Too many downloads, waiting"
		sleep 600
	fi
done
