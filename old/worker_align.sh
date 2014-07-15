#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
SYN_MONITOR="$BASEDIR/synapseICGCMonitor"
ALIGN_SCRIPT="$BASEDIR/job_align.sh"
STOP_FLAGFILE="$BASEDIR/stop_align.flag"

while :
do
	if [ -e $STOP_FLAGFILE ]; then
	    echo "Flagfile found. Exiting"
	    exit 0
	fi

	echo "Scanning"
	UUID=`$SYN_MONITOR getAssignmentForWork ucsc_biofarm split`
	if [ $? != 0 ]; then 
		#echo "Done, exiting"
		#exit 0
	    echo "Done, sleeping"
	    sleep 10;
	    return
	fi
		echo Aligning $UUID
    	if [ -e /pancanfs*/splits/$UUID ]; then
			echo "$ALIGN_SCRIPT $UUID"
			$ALIGN_SCRIPT $UUID

		if [ $? != 0 ]; then 
			$SYN_MONITOR errorAssignment $UUID "aligning error"
		else
			$SYN_MONITOR returnAssignment $UUID		
		fi
	else
		echo $SYN_MONITOR errorAssignment $UUID "Split not found"

		$SYN_MONITOR errorAssignment $UUID "Split not found"
	fi

done
