#!/usr/bin/env python

import os
import re
import shutil
import argparse
import subprocess
from multiprocessing import Pool

def run_fastqtobam(args):
	print "Doing run_fastqtobam"

	cmd = [
		"fastqtobam",
		"I=%s" % (args['fq1']),
		"I=%s" % (args['fq2']),
		"threads=5",
		"gz=1",
		"level=1"
	]
	for fid, fval in args['rg_info'].items():
		cmd.append( "RG%s=%s" % (fid, fval) )
	out = open(args['bam'], "w")
	a = subprocess.Popen(cmd, stdout=out)
	a.communicate()
		
if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("bamfile")
	parser.add_argument("splitdir")
	parser.add_argument("--resume", action="store_true", default=False)
	parser.add_argument("--no-clean", action="store_true", default=False)
	parser.add_argument("--workdir", default=None)
	parser.add_argument("--procs", type=int, default=2)
	
	args = parser.parse_args()
	
	if args.workdir is None:
		args.workdir = args.splitdir
	
	id_map = {}
	a = subprocess.Popen("/agua/apps/samtools/0.1.19/samtools view -H %s | grep '^@RG'" % (args.bamfile), stdout=subprocess.PIPE, shell=True)

#	a = subprocess.Popen("samtools view -H %s | grep '^@RG'" % (args.bamfile), stdout=subprocess.PIPE, shell=True)
	stdout, stderr = a.communicate()

	for line in stdout.split("\n"):
		info = line.split("\t")[1:]
		rg_id = None
		for b in info:
			if b.startswith("ID:"):
				rg_id = b[3:]
				id_map[rg_id] = line
	
	missing = False
	for gr in id_map:
		fq1 = os.path.join( args.workdir, gr + "_1.fq" )
		fq2 = os.path.join( args.workdir, gr + "_2.fq" )
		print fq1
		print fq2
		if not os.path.exists(fq1) or not os.path.exists(fq2):
			missing = True
	
	if not os.path.exists(args.workdir):
		os.mkdir(args.workdir)

	if not os.path.exists(args.splitdir):
		os.mkdir(args.splitdir)
	
	print "Missing Splits:", missing
	
	if not args.resume or missing:
		cmd = [
#			"bamtofastq", 
			"/agua/apps/biobambam/0.0.129/src/bamtofastq", 
			"filename=%s" % (args.bamfile),
			"outputperreadgroup=1",
			"gz=1",
			"level=1",
			"exclude=QCFAIL,SECONDARY,SUPPLEMENTARY",
			"outputdir=%s" % (args.workdir) 
		]
		print "Doing split"
		a = subprocess.call(cmd)

	
	proc_list = []
	for gr in id_map:
		fq1 = os.path.join( args.workdir, gr + "_1.fq" )
		fq2 = os.path.join( args.workdir, gr + "_2.fq" )
		bam = os.path.join( args.splitdir, gr + ".bam" )
		
		rg_info = {}
		for field in id_map[gr].split("\t")[1:]:
			fid, fval = re.search(r'^(..):(.*)$', field).groups()
			rg_info[fid] = fval
		
		proc_list.append( {"fq1" : fq1, "fq2" : fq2, "bam" : bam, "rg_info" : rg_info} )
	p = Pool(args.procs)
	p.map(run_fastqtobam, proc_list)
		
	if not args.no_clean and os.path.abspath(args.workdir) != os.path.abspath(args.splitdir):
		print "Removing workdir"
		shutil.rmtree(args.workdir)

