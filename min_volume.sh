#!/bin/bash

BASEDIR="$(cd `dirname $0`; pwd)"

for a in `cat $BASEDIR/volume.list | sed '/^#/ d'`; do
	echo -n $a " " 
	df $a | tail -n 1
done | sort -n -k 5 -r | awk '{print $1}' | head -n 1
