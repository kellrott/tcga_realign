


import subprocess
import string
from glob import glob
import os

def run_command(command=str):
	run=subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	(stdout,stderr)=run.communicate()
	if run.returncode != 0:
		raise Exception("command %s failure %s\n%s" % (command,stdout,stderr))
	return (stdout,stderr)

def timing(command_name):
    date_cmd = r'date +%s'
    run_command("%s >> %s_timing.txt" % (date_cmd,command_name))

class Aligner:
	def __init__(self, input_name, mode):
		self.input_name = input_name
		self.mode = mode

	def run(self, params):
		input_path = params[self.input_name + ":file"]
		output_path =  os.path.join(self.mode, "out_%s.bam" % (self.input_name))
		cmd = "/opt/pyscripts/bwa_mem.py -r %s -i %s -o %s" % (
			params['refseq'],
			input_path, 
			output_path			
		)
		print "calling", cmd
		timing("%s_bwa" % output_path)
		subprocess.check_call(cmd, shell=True)
		timing("%s_bwa" % output_path)
		yield (self.input_name + ":aligned_bam", output_path)


def bwa_steps(params):
	if not os.path.exists("tumor"):
		os.mkdir("tumor")
	for rg in params['unaligned_tumor_bams']:
		o = Aligner(rg, "tumor")
		yield o.run
        yield ("tumor:aligned_bam_dir","tumor")
	
	if not os.path.exists("normal"):
		os.mkdir("normal")
	for rg in params['unaligned_normal_bams']:
		o = Aligner(rg, "normal")
		yield o.run
        yield ("normal:aligned_bam_dir","normal")


STEPS=bwa_steps
RESUME=False
STORE=False
IMAGE="pcap_tools"
