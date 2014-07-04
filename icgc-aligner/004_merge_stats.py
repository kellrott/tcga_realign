


import subprocess
import string
from glob import glob
import os

MARKDUP="bammarkduplicates O=${OUTFILE} M=${METRICS_FILE} markthreads=8 rewritebam=1 rewritebamlevel=1 index=1 md5=1 tmpfile=${TMPBASE}"

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


class BAMStats:
	def __init__(self, input_name, mode):
		self.input_name = input_name
		self.mode = mode

	def run(self, params):
		input_path = params[self.input_name + ":aligned_bam"]
		output_path =  os.path.join(self.mode, "%s.stats" % (self.input_name))

		cmd = "bam_stats.pl -i %s -o %s" % (
			input_path,
			output_path
		)
		print "calling", cmd
		subprocess.check_call(cmd, shell=True)
		yield (self.input_name + ":aligned_stats", output_path)

def merge_steps(params):
	yield run_normal
	yield run_tumor

	if not os.path.exists("tumor"):
		os.mkdir("tumor")
	for rg in params['unaligned_tumor_bams']:
		o = BAMStats(rg, "tumor")
		yield o.run

	if not os.path.exists("normal"):
		os.mkdir("normal")
	for rg in params['unaligned_normal_bams']:
		o = BAMStats(rg, "normal")
		yield o.run

STEPS=merge_steps
RESUME=False
STORE=False
IMAGE="pcap_tools"
