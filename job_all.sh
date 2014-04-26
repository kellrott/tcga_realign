#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
. $BASEDIR/align.conf

VOLUME=$1
UUID=$2

#LOCAL=`mktemp -d -p $WORK_DIR tcga_realign_XXXXXXXXXXXX`

space=`df $WORK_DIR | tail -n1  | awk '{print $4}'`
hostname
echo SPACE $space
if [[ $space >  838860800 ]]; then
	exit 0
fi

LOCAL=$WORK_DIR/tcga_realign_$UUID

mkdir $LOCAL
mkdir $LOCAL/input
mkdir $LOCAL/splits
mkdir $LOCAL/output
mkdir $LOCAL/submit

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

$SYN_MONITOR resetStatus --status=downloading $UUID
$BASEDIR/job_download.sh $LOCAL $UUID  2> $BASEDIR/logs/$UUID.download.err > $BASEDIR/logs/$UUID.download.out
if [ $? != 0 ]; then
	$SYN_MONITOR errorAssignment $UUID "File not found"
	exit 1
fi

$SYN_MONITOR resetStatus --status=splitting $UUID 2> $BASEDIR/logs/$UUID.split.err > $BASEDIR/logs/$UUID.split.out
$BASEDIR/job_split.sh $LOCAL $UUID
if [ $? != 0 ]; then
	$SYN_MONITOR errorAssignment $UUID "Split Failure"
	exit 1
fi

$SYN_MONITOR resetStatus --status=aligning $UUID 2> $BASEDIR/logs/$UUID.align.err > $BASEDIR/logs/$UUID.align.out
$BASEDIR/job_align.sh $LOCAL $UUID
if [ $? != 0 ]; then
	$SYN_MONITOR errorAssignment $UUID "Align Failure"
	exit 1
fi

rsync -av $LOCAL/output/$UUID $VOLUME/output/
rm -rf $LOCAL
