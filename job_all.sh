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

if [ ! -e $LOCAL ]; then
	mkdir $LOCAL
	mkdir $LOCAL/input
	mkdir $LOCAL/splits
	mkdir $LOCAL/output
	mkdir $LOCAL/submit
fi

echo $$ > $LOCAL.pid

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

if [ ! -e $LOCAL/input/$UUID ]; then
	echo Downloading
	$SYN_MONITOR resetStatus --status=downloading $UUID
	$BASEDIR/job_download.sh $LOCAL $UUID  2> $BASEDIR/logs/$UUID.download.err > $BASEDIR/logs/$UUID.download.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "File not found"
		rm $LOCAL.pid
		exit 1
	fi
fi

if [ ! -e $LOCAL/splits/$UUID ]; then
	echo Splitting
	$SYN_MONITOR resetStatus --status=splitting $UUID 
	$BASEDIR/job_split.sh $LOCAL $UUID 2> $BASEDIR/logs/$UUID.split.err > $BASEDIR/logs/$UUID.split.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "Split Failure"
		rm $LOCAL.pid
		exit 1
	fi
fi

if [ ! -e $LOCAL/outputs/$UUID ]; then
	echo Aligning
	$SYN_MONITOR resetStatus --status=aligning $UUID
	$BASEDIR/job_align.sh $LOCAL $UUID 2> $BASEDIR/logs/$UUID.align.err > $BASEDIR/logs/$UUID.align.out
	if [ $? != 0 ]; then
		$SYN_MONITOR errorAssignment $UUID "Align Failure"
		rm $LOCAL.pid
		exit 1
	fi
fi

rsync -av $LOCAL/output/$UUID $VOLUME/output/
rm -rf $LOCAL
rm $LOCAL.pid
