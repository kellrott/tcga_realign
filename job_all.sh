#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
. $BASEDIR/align.conf

VOLUME=$1
UUID=$2

#LOCAL=`mktemp -d -p $WORK_DIR tcga_realign_XXXXXXXXXXXX`

space=`df $WORK_DIR | tail -n1  | awk '{print $4}'`
echo "Aligning" $UUID on `hostname`
echo SPACE $space
if [[ $space >  838860800 ]]; then
	exit 0
fi

LOCAL=$WORK_DIR/tcga_realign_$UUID

mkdir $LOCAL.pid
if [ $? != 0 ]; then
	echo "Failed to get PID"
fi
echo $$ > $LOCAL.pid/pid
hostname > $BASEDIR/logs/$UUID.host

#from now on, WORK_DIR is LOCAL
export WORK_DIR=$LOCAL

for a in $LOCAL $LOCAL/input $LOCAL/splits $LOCAL/output $LOCAL/submit; do 
	if [ ! -e $a ]; then
		mkdir $a
	fi
done

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

if [ ! -e $LOCAL/input/$UUID ]; then
	echo Downloading
	$SYN_MONITOR resetStatus --status=downloading $UUID
	$BASEDIR/job_download.sh $LOCAL $UUID  2> $BASEDIR/logs/$UUID.download.err > $BASEDIR/logs/$UUID.download.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "File not found"
		rm -rf $LOCAL.pid
		touch $LOCAL.error
		exit 1
	fi
fi

if [ ! -e $LOCAL/splits/$UUID ]; then
	echo Splitting
	$SYN_MONITOR resetStatus --status=splitting $UUID 
	$BASEDIR/job_split.sh $LOCAL $UUID 2> $BASEDIR/logs/$UUID.split.err > $BASEDIR/logs/$UUID.split.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "Split Failure"
		rm -rf $LOCAL.pid
		touch $LOCAL.error
		exit 1
	fi
fi

if [ ! -e $LOCAL/output/$UUID ]; then
	echo Aligning
	$SYN_MONITOR resetStatus --status=aligning $UUID
	$BASEDIR/job_align.sh $LOCAL $UUID 2> $BASEDIR/logs/$UUID.align.err > $BASEDIR/logs/$UUID.align.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "Align Failure"
		rm -rf $LOCAL.pid
		touch $LOCAL.error
		exit 1
	fi
fi

#now do checking, submitting, and uploading
if [ ! -e $LOCAL/submit/$UUID ]; then
	echo Submitting
	$SYN_MONITOR resetStatus --status=submitting $UUID
	$BASEDIR/job_upload.sh $LOCAL $UUID 2> $BASEDIR/logs/$UUID.submit.err > $BASEDIR/logs/$UUID.submit.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "Submit Failure"
		rm -rf $LOCAL.pid
		touch $LOCAL.error
		exit 1
	fi
	$SYN_MONITOR resetStatus --status=uploaded $UUID
	touch $LOCAL.complete
	#rm -rf $LOCAL
fi

rm -rf $LOCAL.pid
