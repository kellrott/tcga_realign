

import re
import subprocess
import string
from glob import glob
import os

SPLIT_CODE="/opt/pyscripts/pcap_split.py"

def do_normal_split(params):
	template = "$SPLIT_CODE --bam_path ${normal_bam} --output_dir ./normal --work_dir ./normal_work --normal_id ${normal_id}"
	cmd = string.Template(template).substitute(dict(params, SPLIT_CODE=SPLIT_CODE))
	subprocess.check_call(cmd, shell=True)
	names = []
	for bam in glob("normal/*.bam"):
		base = os.path.basename(bam)
		#the naming pattern is <uuid>_<readgroup>.cleaned.bam
		n = "normal_" + re.sub(r'........-....-....-....-............_(.*).cleaned.bam', r'\1', base)
		if n is None or len(n) == 0:
			n = base
		yield (n + ":file", bam)
		names.append(n)
	yield ('unaligned_normal_bams', names)


def do_tumor_split(params):
	template = "$SPLIT_CODE --bam_path ${tumor_bam} --output_dir ./tumor --work_dir ./tumor_work --tumor_id ${tumor_id} --normal_id ${normal_id}"
	cmd = string.Template(template).substitute(dict(params, SPLIT_CODE=SPLIT_CODE))
	subprocess.check_call(cmd, shell=True)
	names = []
	for bam in glob("tumor/*.bam"):
		base = os.path.basename(bam)
		#the naming pattern is <uuid>_<readgroup>.cleaned.bam
		n = "tumor_" + re.sub(r'........-....-....-....-............_(.*).cleaned.bam', r'\1', base)
		if n is None or len(n) == 0:
			n = base
		yield (n + ":file", bam)
		names.append(n)
	yield ('unaligned_tumor_bams', names)


STEPS=[do_normal_split, do_tumor_split]
RESUME=False
STORE=False
IMAGE="pcap_tools"
