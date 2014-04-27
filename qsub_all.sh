#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 16

#this script assumes that it will be qsub'ed in the pancancer working directory

VOLUME=$1

UUID=`./find_local_job.sh`

if [ -z "$UUID" ]; then 
	exit 0
fi

./job_all.sh $VOLUME $UUID
