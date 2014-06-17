

import subprocess

def copy_out(params):
	if "mutect_vcf" in params:
		subprocess.check_call("gzip -c %s > %s.mutect.vcf.gz" % (params["mutect_vcf"], params["id"]), shell=True)
	if "muse_txt" in params:
		subprocess.check_call("gzip -c %s > %s.muse.txt.gz" % (params["muse_txt"], params["id"]), shell=True)
	return []
	
STEPS = [copy_out]
RESUME=False
STORE=True
