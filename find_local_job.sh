#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"
. $BASEDIR/align.conf

SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

if [ -e $WORK_DIR/tcga_realign_* ]; then
	for a in $WORK_DIR/tcga_realign_*; do
		if [ ! -e $a.pid ]; then
			basename $a | sed 's/^tcga_realign_//'
			exit 0
		fi
	done
fi

$SYN_MONITOR getAssignmentForWork $ASSIGNEE todownload
