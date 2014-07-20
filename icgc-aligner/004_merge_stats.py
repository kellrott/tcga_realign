


import subprocess
import string
from glob import glob
import os

MARKDUP="bammarkduplicates O=${OUTFILE} M=${METRICS_FILE} markthreads=8 rewritebam=1 rewritebamlevel=1 index=1 md5=1 tmpfile=${TMPBASE}"

def run_command(command=str):
	run=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	(stdout,stderr)=run.communicate()
	if run.returncode != 0:
		raise Exception("command %s failure %s\n%s" % (command,stdout,stderr))
	return (stdout,stderr)

def timing(command_name):
    date_cmd = r'date +%s'
    run_command("%s >> %s_timing.txt" % (date_cmd,command_name))

def run_normal(params):
	cmd = string.Template(MARKDUP).substitute(dict(params, OUTFILE="PAWG.%s.bam" % (params['normal_id']), METRICS_FILE="%s.markdup.metrics" % (params['normal_id']), TMPBASE="tmp_normal"))
	for i in params['unaligned_normal_bams']:
		cmd += " I=%s" % (params[i+":aligned_bam"])
	timing("PAWG.%s.bam_merge" % params['normal_id'])
	subprocess.check_call(cmd, shell=True)
	timing("PAWG.%s.bam_merge" % params['normal_id'])
	yield ("normal_merged", "PAWG.%s.bam" % (params['normal_id']))
	yield ("normal_merged_metrics", "%s.markdup.metrics" % (params['normal_id']))
        yield ("normal_merged_timing", "PAWG.%s.bam_merge_timing.txt" % params['normal_id'])

def run_tumor(params):
	cmd = string.Template(MARKDUP).substitute(dict(params, OUTFILE="PAWG.%s.bam" % (params['tumor_id']), METRICS_FILE="%s.markdup.metrics" % (params['tumor_id']), TMPBASE="tmp_tumor"))
	for i in params['unaligned_tumor_bams']:
		cmd += " I=%s" % (params[i+":aligned_bam"])
	timing("PAWG.%s.bam_merge" % params['tumor_id'])
	subprocess.check_call(cmd, shell=True)
	timing("PAWG.%s.bam_merge" % params['tumor_id'])
	yield ("tumor_merged", "PAWG.%s.bam" % (params['tumor_id']))
	yield ("tumor_merged_metrics", "%s.markdup.metrics" % (params['tumor_id']))
        yield ("tumor_merged_timing", "PAWG.%s.bam_merge_timing.txt" % params['tumor_id'])


class BAMStatsAndVerify:
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
		
		input_path = params[self.input_name + ":file"]
		header_path = "%s.header.txt" % input_path
		stats_path = output_path
		counts_path =  "%s.count.txt" % input_path
		
		#verify_read_groups.pl --header-file bam_header." + i + ".txt" + " --bas-file out" + i + ".bam.stats.txt" + " --input-read-count-file inputbam" + i + ".count.txt"
		cmd = "verify_read_groups.pl --header-file %s --bas-file %s --input-read-count-file %s" % (
			header_path,
			stats_path,
			counts_path	
		)
		print "calling", cmd
		#subprocess.check_call(cmd, shell=True)
		#this will die (and then raise an exception) if the verification fails at any point
		(stdout,stderr)=run_command(cmd)
	
		yield (self.input_name + ":aligned_stats", output_path)


def merge_steps(params):
	yield run_normal
	yield run_tumor

	if not os.path.exists("tumor"):
		os.mkdir("tumor")
	for rg in params['unaligned_tumor_bams']:
		input_path = params[rg + ":aligned_bam"]
		timing("%s_qc" % input_path)
		o = BAMStatsAndVerify(rg, "tumor")
		yield o.run
		timing("%s_qc" % input_path)
        yield ("tumor:stats_dir","tumor")

	if not os.path.exists("normal"):
		os.mkdir("normal")
	for rg in params['unaligned_normal_bams']:
		input_path = params[rg + ":aligned_bam"]
		timing("%s_qc" % input_path)
		o = BAMStatsAndVerify(rg, "normal")
		yield o.run
		timing("%s_qc" % input_path)
        yield ("normal:stats_dir","normal")

STEPS=merge_steps
RESUME=False
STORE=False
IMAGE="pcap_tools"
