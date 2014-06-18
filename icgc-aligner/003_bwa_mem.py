


import subprocess
import string
from glob import glob
import os

class Aligner:
	def __init__(self, input_name):
		self.input_name = input_name

	def run(params):
		cmd = "bwa mem -p -T 0 -R '%s' %s %s | \
bamsort inputformat=sam level=1 inputthreads=2 outputthreads=2 calmdnm=1 calmdnmrecompindetonly=1 calmdnmreference=%s tmpfile=out_%s.sorttmp O=out_%s.bam" % (
			params[self.input_name + ":header"], 
			params['reference_file'], 
			params[self.input_name + ":input_file"], 
			params[self.input_name + ":input_file"],
			self.input_name, 
			self.input_name
)
		subprocess.check_call(cmd, shell=True)
		yield (self.input_name + ":aligned_bam", "out_%s.bam" % (self.input_name))


def bwa_steps(params):
	for rg in params['read_groups']:
		o = Aligner(rg)
		yield o.run


STEPS=bwa_steps
RESUME=False
STORE=False
IMAGE="icgc-aligner"