


import subprocess
import string
from glob import glob
import os

MARKDUP="bammarkduplicates O=${OUTFILE} M={METRICS_FILE} markthreads=8 rewritebam=1 rewritebamlevel=1 index=1 md5=1 tmpfile=${TMPBASE}"

def run_normal(params):
	cmd = string.Template(MARKDUP).substitute(dict(params, OUTFILE="PAWG.%s.bam" % (params['normal_id']), METRICS_FILE="normal.metric", TMPBASE="tmp_normal"))
	for i in params['unaligned_normal_bams']:
		cmd += " I=%s" % (params[i+":aligned_bam"])
	subprocess.check_call(cmd, shell=True)

def run_tumor(params):
	cmd = string.Template(MARKDUP).substitute(dict(params, OUTFILE="PAWG.%s.bam" % (params['normal_id']), METRICS_FILE="tumor.metric", TMPBASE="tmp_tumor"))
	for i in params['unaligned_tumor_bams']:
		cmd += " I=%s" % (params[i+":aligned_bam"])
	subprocess.check_call(cmd, shell=True)


STEPS=[run_normal, run_tumor]
RESUME=False
STORE=False
IMAGE="pcap_tools"
