#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
. $BASEDIR/align.conf

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

if [ $(ls -d $WORK_DIR/tcga_realign_* | wc -l) > 0 ]; then
	for a in `ls -d $WORK_DIR/tcga_realign_* | grep -v '.pid$'`; do
		if [ ! -e $a.pid ]; then
			basename $a | sed 's/^tcga_realign_//'
			exit 0
		fi
	done
fi

$SYN_MONITOR getAssignmentForWork $ASSIGNEE todownload
