#!/usr/bin/env python
#does one or more checks on the realigned bam in comparison to the original bam to make sure new bam looks sane
#this requires that both bams have associated .bai files in the same directory as the bam
#the current check that has been implemented as of 4/26/2014 is a pearson correlation coef for the sum of mapped + unmapped
#across all chromosomes/contigs in both files.  this will at least tell us if truncation has occurred (a few contigs/chromosomes have 0 read counts)

import ConfigParser
from optparse import OptionParser
import os
import sys
import re
import subprocess
import math
from subprocess import Popen

PEARSON_CORR_MIN=0.99

parser=OptionParser()
parser.add_option("-o", action="store",type='string',dest="original_bam",help="REQUIRED: full path andfilename of original BAM")
parser.add_option("-n", action="store",type='string',dest="new_bam",help="REQUIRED: full path andfilename of new realigned BAM")
parser.add_option("-p", action="store",type='string',dest="output",help="REQUIRED: output directory for BAI files")

(options,args) = parser.parse_args()
ORIGINAL_BAM=options.original_bam
NEW_BAM=options.new_bam
OUTPUT_DIR=options.output

if NEW_BAM is None or ORIGINAL_BAM is None or OUTPUT_DIR is None:
    sys.stderr.write("MUST submit: the original TCGA source BAM and the re-aligned new BAM\n")
    sys.exit(-1)

def run_command(command=str):
    print "Running: %s" % (command)
    run=Popen(["-c",command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (stdout,stderr)=run.communicate()
    if run.returncode != 0:
        for line in stderr:
            print "ERROR:\t"+line.rstrip()
        sys.exit(-1)
    return (stdout,stderr)

def run_flagstat(bam_file):
	(fs_output,stderr)=run_command("samtools flagstat %s" % bam_file)
	return process_flagstat(fs_output)

def process_flagstat(fs_output):
#68629600 + 6562518 in total (QC-passed reads + QC-failed reads)
#10054468 + 1517557 duplicates
#67842739 + 5593779 mapped (98.85%:85.24%)
#68629600 + 6562518 paired in sequencing
#34314800 + 3281259 read1
#34314800 + 3281259 read2
#66854380 + 5314442 properly paired (97.41%:80.98%)
#67196156 + 5353240 with itself and mate mapped
#646583 + 240539 singletons (0.94%:3.67%)
#301196 + 35316 with mate mapped to a different chr
#260127 + 29485 with mate mapped to a different chr (mapQ>=5)
	#track each field in each line of the fs output in a map, keyed by the first english word in the description (after the numbers)
	fs_map={}
	name_ctr=2
	#fs_output=fs_output.replace("\\n","\n")
	for line in fs_output.split("\n"):
		line=line.rstrip()
		if len(line) < 1:
			continue
		f=line.split()
		#print line
		if f[3] not in fs_map:
			fs_map[f[3]]=[f[0],f[2]]
		else:
			fs_map["%s%s" % (f[3],name_ctr)]=[f[0],f[2]]
	return fs_map

def run_idxstats(bam_file,output_path,do_truncation):
	(sam_output,stderr)=run_command("samtools idxstats %s" % bam_file)
	return process_idxstats(sam_output,output_path,do_truncation)

def process_idxstats(sam_output,output_path,do_truncation):
	sam_map={}
	sam_map["*"]=0
	total_mapped=0
	total_unmapped=0
	total=0
	TRUNCATE=None
	with open(output_path,"w") as outf:
		for line in sam_output.split("\n"):
			outf.write("%s\n" % (line))	
			line=line.rstrip()
			if len(line) < 1:
				continue
			#chr,chr len,mapped,unmapped
			f=line.split()
			chr = str(f[0]).upper()
			mapped=int(f[2])
			unmapped=int(f[3])
			
			if chr == 'N' and do_truncation:
				TRUNCATE=1
			if TRUNCATE:
				mapped=0
				unmapped=0		
			#adjust for the missing chromosomes in the original, but present in the newer one, 
			#add their mapped + unmapped to the unmapped catchall chr="*"
			if chr == "NC_007605" or chr == "HS37D5" or chr == "*":
				sam_map["*"]=sam_map["*"]+mapped+unmapped
			else:
				sam_map[chr]=mapped+unmapped

			total_mapped=total_mapped+mapped
			total_unmapped=total_unmapped+unmapped
			total=total+mapped+unmapped

	return (total,total_mapped,total_unmapped,sam_map)


#from
#http://stackoverflow.com/questions/3949226/calculating-pearson-correlation-and-significance-in-python/7939259#7939259
def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)

def pearson_corr(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff

    return diffprod / math.sqrt(xdiff2 * ydiff2)


def main():
    #orig_fs_map=run_flagstat(ORIGINAL_BAM)
    #(fs_output,stderr)=run_command("cat %s" % ORIGINAL_BAM)
    #orig_fs_map = process_flagstat(fs_output) 
    #(fs_output,stderr)=run_command("cat %s" % NEW_BAM)
    #new_fs_map = process_flagstat(fs_output) 
    (total1,total_mapped1,total_unmapped1,orig_idxs) = run_idxstats(ORIGINAL_BAM,"%s/old_bam.idxstats"%(OUTPUT_DIR),False)
    (total2,total_mapped2,total_unmapped2,new_idxs) = run_idxstats(NEW_BAM,"%s/new_bam.idxstats"%(OUTPUT_DIR),False)
  
    sys.stdout.write("old=%d %d %d\n" % (total1,total_mapped1,total_unmapped1)) 
    sys.stdout.write("new=%d %d %d\n" % (total2,total_mapped2,total_unmapped2)) 

    all_counts1=[]
    all_counts2=[]

    for (chr,counts1) in orig_idxs.iteritems():
        #(mapped1,unmapped1)=values
        #(mapped2,unmapped2)=new_idxs[chr]
        counts2=new_idxs[chr]
        all_counts1.append(int(counts1))	
        all_counts2.append(int(counts2))	
        #sys.stdout.write("%s + %s %s|||%s %s\n" % (chr,mapped1,unmapped1,mapped2,unmapped2))
        #sys.stdout.write("%s %d %d\n" % (chr,counts1,counts2))

    pearson_corr_coef=pearson_corr(all_counts1,all_counts2)
    sys.stdout.write("pearson=%s\n" % (str(pearson_corr_coef)))
    if pearson_corr_coef < PEARSON_CORR_MIN:
	sys.stderr.write("Pearson Correlation Coefficient too low: %s < %s\n" % (str(pearson_corr_coef),str(PEARSON_CORR_MIN)))
	sys.exit(-1)			

if __name__ == '__main__':
    main()
