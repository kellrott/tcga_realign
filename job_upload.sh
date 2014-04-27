#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

VOLUME=$1
UUID=$2

. $BASEDIR/align.conf


BAM_DIR=$VOLUME/output/$UUID
#the realigned BAM file
BAM_FILE=$(ls $BAM_DIR/*.bam)
#the BAM file downloaded from CGHub
ORIG_FILE=$(ls $VOLUME/input/$UUID/*.bam)

#the submission directory
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

#put metric compareing $ORIG_FILE and $BAM_FILE and save stats to $SUB_DIR
/pod/home/cwilks/pawgpy/bin/python $BASEDIR/cghub_metadata_generator/realigned_bam_check.py -o $ORIG_FILE -n $BAM_FILE -p $SUB_DIR

NEW_UUID=`uuidgen`
NEW_NORMAL_UUID=`uuidgen`

pushd $SUB_DIR
/pod/home/cwilks/pawgpy/bin/python $BASEDIR/cghub_metadata_generator/create_pawg_metadata.py -u $UUID -f PAWG.$UUID.bam -c `cat $BAM_FILE.md5` -p $SUB_DIR -t $NEW_NORMAL_UUID -n $NEW_UUID
popd

pushd $SUB_DIR
/pod/home/cwilks/pawgpy/bin/python $BASEDIR/cghub_metadata_generator/cgsubmit --validate-only -u $NEW_UUID
#for real
#/pod/home/cwilks/pawgpy/bin/python $BASEDIR/cghub_metadata_generator/cgsubmit -c /pod/home/cwilks/UCSC_PAWG.key -u $NEW_UUID
#/usr/bin/gtupload -c /pod/home/cwilks/UCSC_PAWG.key -u $NEW_UUID/manifest.xml -vv 2>$SUB_DIR/upload.stderr.log
popd
