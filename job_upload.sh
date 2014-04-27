#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

PYTHON=/pod/home/cwilks/pawgpy/bin/python

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
$PYTHON $BASEDIR/cghub_metadata_generator/realigned_bam_check -o $ORIG_FILE -n $BAM_FILE -p $SUB_DIR
if [ $? != 0 ]; then
	echo "Realignment Check error"
	exit 1
fi

#NEW_UUID=`uuidgen`


NEW_NORMAL_UUID=`$PYTHON $BASEDIR/synapseICGCMonitor getInfo $UUID --get-normal`
`$PYTHON $BASEDIR/synapseICGCMonitor getResultID $UUID 2> $SUB_DIR/new_uuid`
NEW_UUID=`cat $SUB_DIR/new_uuid`

#create cghub validating metadata with ICGC specific metadata added to it
pushd $SUB_DIR
$PYTHON $BASEDIR/cghub_metadata_generator/create_pawg_metadata -u $UUID -f PAWG.$UUID.bam -c `cat $BAM_FILE.md5` -p $SUB_DIR -t $NEW_NORMAL_UUID -n $NEW_UUID 
if [ $? != 0 ]; then
	echo "CGHub metadata creation error"
	exit 1
fi
popd

#submit to cghub (or just validate)
pushd $SUB_DIR
$PYTHON $BASEDIR/cghub_metadata_generator/cgsubmit --validate-only -u $NEW_UUID
#uncomment to run for real, changes CGHub production!
#$PYTHON $BASEDIR/cghub_metadata_generator/cgsubmit -c /pod/home/cwilks/UCSC_PAWG.key -u $NEW_UUID
#if [ $? != 0 ]; then
#	echo "CGHub metadata submission error"
#	exit 1
#fi
#upload data to cghub
#/usr/bin/gtupload -c /pod/home/cwilks/UCSC_PAWG.key -u $NEW_UUID/manifest.xml -vv 2>$SUB_DIR/upload.stderr.log
#if [ $? != 0 ]; then
#	echo "CGHub file upload error, check error log $SUB_DIR/upload.stderr.log"
#	exit 1
#fi
popd
