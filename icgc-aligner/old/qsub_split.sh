#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 16

#this script assumes that it will be qsub'ed in the pancancer working directory

VOLUME=$1
UUID=$2

SYN_MONITOR="./synapseICGCMonitor" 

./job_split.sh $VOLUME $UUID 2> logs/$UUID.split.err > logs/$UUID.split.out
if [ $? != 0 ]; then 
	$SYN_MONITOR errorAssignment $UUID "splitting error"
else
	$SYN_MONITOR returnAssignment $UUID		
fi
