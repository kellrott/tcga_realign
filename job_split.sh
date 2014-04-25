#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

. $BASEDIR/align.conf

SPLIT_CODE="$BASEDIR/pcap_split/pcap_split.py"
SYN_MONITOR="$BASEDIR/synapseICGCMonitor"

VOLUME=$1
UUID=$2

INDIR=$VOLUME/input
OUTDIR=$VOLUME/splits

if [ -z $USE_DOCKER ]; then 
	CMD_PREFIX="" 
else
	CMD_PREFIX="sudo docker run -v $BASEDIR:$BASEDIR -v $VOLUME:$VOLUME -v /mnt:/mnt icgc-aligner"
fi

BAM_FILE=$(ls $INDIR/$UUID/*.bam)

SAMPLE_TYPE=`$SYN_MONITOR getInfo $UUID --type`

if [[ "$SAMPLE_TYPE" == "Primary Solid Tumor" ]]; then 
	NORMAL_ID=`$SYN_MONITOR getInfo $UUID --get-normal`
	CMD="$CMD_PREFIX $SPLIT_CODE --bam_path $BAM_FILE --output_dir $VOLUME/splits/$UUID --work_dir $WORK_DIR/$UUID --tumor_id $UUID --normal_id $NORMAL_ID"
	echo "Running $CMD"
	$CMD
elif [[ "$SAMPLE_TYPE" == "Blood Derived Normal" || "$SAMPLE_TYPE" == "Solid Tissue Normal" ]]; then
	$CMD_PREFIX $SPLIT_CODE --bam_path $BAM_FILE --output_dir $VOLUME/splits/$UUID --work_dir $WORK_DIR/$UUID --normal_id $UUID
else
	exit 1
fi
