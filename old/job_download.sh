#!/bin/bash


BASEDIR="$(cd `dirname $0`; pwd)"
. $BASEDIR/align.conf

VOLUME=$1
UUID=$2

echo DOWNLOADING $UUID into $VOLUME
OUTDIR=$VOLUME/input

##echo "$DOWNLOAD_SCRIPT --uuid $UUID --outputdir $OUTDIR --keyfile $KEYFILE"
##$DOWNLOAD_SCRIPT --uuid $UUID --outputdir $OUTDIR --keyfile $KEYFILE
gtdownload -c $KEYFILE -p $VOLUME/input/ -v -d $UUID
if [ $? != 0 ]; then
	echo "Download error"
	exit 1
fi

if [ ! -e $VOLUME/input/$UUID ]; then 
	echo "Download error"
	exit 1
fi

