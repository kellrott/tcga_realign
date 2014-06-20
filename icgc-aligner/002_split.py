

import subprocess
import string
from glob import glob
import os

SPLIT_CODE="/opt/pyscripts/pcap_split.py"

def do_normal_split(params):
	template = "$SPLIT_CODE --bam_path ${normal_bam} --output_dir ./ --work_dir ./ --normal_id ${normal_id}"
	cmd = string.Template(template).substitute(dict(params, SPLIT_CODE=SPLIT_CODE))
	subprocess.check_call(cmd, shell=True)
	#start yielding the results


def do_tumor_split(params):
	template = "$SPLIT_CODE --bam_path ${tumor_bam} --output_dir ./ --work_dir ./ --tumor_id ${tumor_id} --normal_id ${normal_id}"
	cmd = string.Template(template).substitute(dict(params, SPLIT_CODE=SPLIT_CODE))
	subprocess.check_call(cmd, shell=True)
	#start yielding the results


STEPS=[do_normal_split, do_tumor_split]
RESUME=False
STORE=False
IMAGE="pcap_tools"
