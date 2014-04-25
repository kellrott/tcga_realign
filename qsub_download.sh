#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 16

#this script assumes that it will be qsub'ed in the pancancer working directory

VOLUME=$1
UUID=$2

SYN_MONITOR="./synapseICGCMonitor"

./job_download.sh $VOLUME $UUID 2> logs/$UUID.download.err > logs/$UUID.download.out
if [ $? != 0 ]; then 
	$SYN_MONITOR errorAssignment $UUID "gtdownload error"
else
	if [ $(ls $VOLUME/input/$UUID/*.bam | wc -l) = 1 ]; then
		BAM_FILE=`ls $VOLUME/input/$UUID/*.bam`
		$SYN_MONITOR addBamGroups $UUID $BAM_FILE
		$SYN_MONITOR returnAssignment $UUID
	else
		 $SYN_MONITOR errorAssignment $UUID "File not found"
	fi		
fi
