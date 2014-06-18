
import subprocess
import string
from glob import glob
import os

def gtdownload(params):
	cmd = string.Template("gtdownload -c ${keyfile} -p ./ -v -d ${tumor_id}").substitute(params)
	subprocess.check_call(cmd, shell=True)
	cmd = string.Template("gtdownload -c ${keyfile} -p ./ -v -d ${normal_id}").substitute(params)
	subprocess.check_call(cmd, shell=True)

	tumor_bam = None
	for a in glob(os.path.join(params['tumor_id'], "*.bam")):
		tumor_bam = a

	normal_bam = None
	for a in glob(os.path.join(params['normal_id'], "*.bam")):
		normal_bam = a

	yield ("tumor_bam", tumor_bam)
	yield ("normal_bam", normal_bam)



STEPS=[gtdownload]
RESUME=True
STORE=False
IMAGE="gtdownload"