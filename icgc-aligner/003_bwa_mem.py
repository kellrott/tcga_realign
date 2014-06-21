


import subprocess
import string
from glob import glob
import os

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
		subprocess.check_call(cmd, shell=True)
		yield (self.input_name + ":aligned_bam", output_path)


def bwa_steps(params):
	if not os.path.exists("tumor"):
		os.mkdir("tumor")
	for rg in params['unaligned_tumor_bams']:
		o = Aligner(rg, "tumor")
		yield o.run
	
	if not os.path.exists("normal"):
		os.mkdir("normal")	
	for rg in params['unaligned_normal_bams']:
		o = Aligner(rg, "normal")
		yield o.run


STEPS=bwa_steps
RESUME=False
STORE=False
IMAGE="pcap_tools"
