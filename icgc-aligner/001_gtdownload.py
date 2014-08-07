
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

def gtdownload(params):
	cmd = string.Template("gtdownload -c ${keyfile} -p ./ -v -d ${tumor_id}").substitute(params)
	timing("%s_download" % params['tumor_id'])
	subprocess.check_call(cmd, shell=True)
	timing("%s_download" % params['tumor_id'])
	cmd = string.Template("gtdownload -c ${keyfile} -p ./ -v -d ${normal_id}").substitute(params)
	timing("%s_download" % params['normal_id'])
	subprocess.check_call(cmd, shell=True)
	timing("%s_download" % params['normal_id'])

	tumor_bam = None
	for a in glob(os.path.join(params['tumor_id'], "*.bam")):
		tumor_bam = a

	normal_bam = None
	for a in glob(os.path.join(params['normal_id'], "*.bam")):
		normal_bam = a

	yield ("tumor_bam", tumor_bam)
	yield ("tumor_download_timing","%s_download_timing.txt" % params['tumor_id'])
	yield ("normal_bam", normal_bam)
	yield ("normal_download_timing","%s_download_timing.txt" % params['normal_id'])



STEPS=[gtdownload]
RESUME=True
STORE=False
IMAGE="gtdownload"
CLUSTER_MAX=2
