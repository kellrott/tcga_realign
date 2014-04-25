#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

VOLUME=$1
UUID=$2

. $BASEDIR/align.conf

BAM_DIR=$VOLUME/output/$UUID
BAM_FILE=$(ls $BAM_DIR/*.bam)

SUB_DIR=$VOLUME/submit/$UUID

if [ ! -e $SUB_DIR ]; then
	mkdir $SUB_DIR
else 
	rm -rf $SUB_DIR
	mkdir $SUB_DIR
fi

if [ ! -e $SUB_DIR/PAWG.$UUID.bam ]; then
	ln -s $BAM_FILE $SUB_DIR/PAWG.$UUID.bam
fi

NEW_UUID=`uuidgen`

pushd $SUB_DIR
python $BASEDIR/pancan_pawg_metadata_generator/create_pawg_metadata.py -u $UUID -f PAWG.$UUID.bam -c `cat $BAM_FILE.md5` -p $SUB_DIR -i $BAM_FILE.info -n $NEW_UUID
popd

pushd $SUB_DIR
$BASEDIR/pancan_pawg_metadata_generator/cgsubmit --validate-only -u $NEW_UUID
popd
