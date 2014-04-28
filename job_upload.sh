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
SUB_DIR=$VOLUME/submit/$UUID.partial
FIN_DIR=$VOLUME/submit/$UUID

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
$PYTHON $BASEDIR/pyscripts/realigned_bam_check -o $ORIG_FILE -n $BAM_FILE -p $SUB_DIR
if [ $? != 0 ]; then
	echo "Realignment Check error"
	exit 1
fi


NORMAL_UUID=`$PYTHON $BASEDIR/synapseICGCMonitor getInfo $UUID --get-normal`
if [ $? != 0 ]; then
	echo "Failed to get normal uuid from Synapse error"
	exit 1
fi

NEW_NORMAL_UUID=`$PYTHON $BASEDIR/synapseICGCMonitor getResultID $NORMAL_UUID`
if [ $? != 0 ]; then
	echo "Failed to get new normal uuid from Synapse error"
	exit 1
fi

NEW_UUID=`$PYTHON $BASEDIR/synapseICGCMonitor getResultID $UUID`
if [ $? != 0 ]; then
	echo "Failed to get new uuid from Synapse error"
	exit 1
fi

#create cghub validating metadata with ICGC specific metadata added to it
pushd $SUB_DIR
$PYTHON $BASEDIR/pyscripts/create_pawg_metadata -u $UUID -f PAWG.$UUID.bam -c `cat $BAM_FILE.md5` -p $SUB_DIR -t $NEW_NORMAL_UUID -n $NEW_UUID 
if [ $? != 0 ]; then
	echo "CGHub metadata creation error"
	exit 1
fi
popd

#submit to cghub (or just validate)
pushd $SUB_DIR
#$PYTHON $BASEDIR/pyscripts/cgsubmit --validate-only -u $NEW_UUID
#if [ $? != 0 ]; then
#	echo "CGHub metadata validation error"
#	exit 1
#fi

#uncomment to run for real, changes CGHub production!
$PYTHON $BASEDIR/pyscripts/cgsubmit -c $UPLOAD_KEY -u $NEW_UUID
if [ $? != 0 ]; then
	echo "CGHub metadata submission error"
	exit 1
fi
#upload data to cghub
gtupload -c $UPLOAD_KEY -u $NEW_UUID/manifest.xml -vv 2>$SUB_DIR/upload.stderr.log
if [ $? != 0 ]; then
	echo "CGHub file upload error, check error log $SUB_DIR/upload.stderr.log"
	exit 1
fi
popd

mv $SUB_DIR $FIN_DIR
