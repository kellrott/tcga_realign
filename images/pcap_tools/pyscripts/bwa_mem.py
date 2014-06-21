#!/usr/bin/env python

import logging
import subprocess
import argparse
import sys
import tempfile
import string
import shutil


def run_command(command=str):
    logging.info("Running: %s" % (command))
    run=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (stdout,stderr)=run.communicate()
    if run.returncode != 0:
		raise Exception("Tool fail")
    return (stdout,stderr)


def get_readgroup(bamfile):
	out, err = run_command("samtools view -H %s" % (bamfile))	
	rgs = []
	for line in out.split("\n"):
		if line.startswith("@RG\t"):
			rgs.append(line.rstrip("\n\r"))
	if len(rgs) != 1:
		raise Exception("Wrong number of readgroups in BAM file")
	return rgs[0]

def get_rgid(rgline):
	for i in rgline.split("\t"):
		if i.startswith("ID:"):
			return i[3:]


def main(args):
	rgline = get_readgroup(args.inbam)
	rgid = get_rgid(rgline)
	
	work_dir = tempfile.mkdtemp(dir=args.workdir, prefix="bwa_mem_")
	
	template = "bamtofastq T=${tmpdir}/bamtofastq_tmp S=${tmpdir}/single.fq O=${tmpdir}/unmatched_1.fq O2=${tmpdir}/unmatched_2.fq exclude=QCFAIL,SECONDARY,SUPPLEMENTARY collate=1 filename=${inbam} | \
bwa mem -t 8 -p -T 0 -R '${rgline}' ${refseq} - | \
bamsort inputformat=sam level=1 inputthreads=2 outputthreads=2 calmdnm=1 calmdnmrecompindetonly=1 calmdnmreference=${refseq} tmpfile=${tmpdir}/out.sorttmp O=${tmpdir}/out.bam"
	
	cmd = string.Template(template).substitute({
		"inbam" : args.inbam,
		"rgline" : rgline,
		"refseq" : args.refseq,
		"tmpdir" : work_dir
	})
	
	run_command(cmd)
	shutil.move( "%s/out.bam" % (work_dir), args.outbam)
	shutil.rmtree(work_dir)
	

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	
	parser.add_argument("-r", "--refseq", required=True)
	#parser.add_argument("-it", "--input-threads", default="2" )
	#parser.add_argument("-ot", "--output-threads", default="2" )
	parser.add_argument("-i", "--inbam", required=True)
	parser.add_argument("-o", "--outbam", required=True)
	parser.add_argument("-w", "--workdir", default="./")
	
	args = parser.parse_args()
	sys.exit(main(args))

