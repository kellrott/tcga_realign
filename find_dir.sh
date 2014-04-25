#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

PHASE=$1
UUID=$2

OUT=""
for VOLUME in `cat $BASEDIR/volume.list | sed '/^#/ d'`; do
	DIR=$VOLUME/$PHASE/$UUID 
	if [ -e $DIR ]; then 
		OUT=$DIR
	fi
done

if [ -z "$OUT" ]; then
	echo "Not found"
	exit 1
else
	echo $OUT
fi
