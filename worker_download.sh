#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
STOP_FLAGFILE="$BASEDIR/stop_download.flag"

. $BASEDIR/align.conf


while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi

	echo "Scanning"
	UUID=`$SYN_MONITOR getAssignmentForWork $ASSIGNEE todownload`
	if [ $? != 0 ]; then 
		echo "Done, sleeping"
		#exit 0
		sleep 60
	else
		echo Downloading $UUID
    	echo Downloading $UUID
		BAM_DIR=`$BASEDIR/find_dir.sh input $UUID`
		if [ $? != 0 ]; then 
			VOLUME=`$BASEDIR/min_volume.sh`
			echo "Using" $VOLUME
			BAM_DIR=$VOLUME/input/$UUID
		else
			INPUT_DIR=`dirname $BAM_DIR`
			VOLUME=`dirname $INPUT_DIR`
		fi
		
		$BASEDIR/job_download.sh $VOLUME $UUID
		
		if [ $? != 0 ]; then 
			$SYN_MONITOR errorAssignment $UUID "gtdownload error"
		else
			if [ $(ls $BAM_DIR/*.bam | wc -l) = 1 ]; then
				BAM_FILE=`ls $BAM_DIR/*.bam`
				$SYN_MONITOR addBamGroups $UUID $BAM_FILE
				$SYN_MONITOR returnAssignment $UUID
			else
				 $SYN_MONITOR errorAssignment $UUID "File not found"
			fi		
		fi
	fi
done
