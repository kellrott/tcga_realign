#!/bin/bash

for a in `./synapseICGCMonitor getAssignments ucsc_pod | cut -f 1`; do 
	b=`./find_dir.sh output $a`; 
	if [ -e "$b/$a.bam.md5" ]; then 
		echo $a; 
		./synapseICGCMonitor resetStatus --status=aligned $a
	fi ; 
done
