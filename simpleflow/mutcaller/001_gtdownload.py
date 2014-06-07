
import subprocess
import string


def gtdownload(params):
	cmd = string.Template("gtdownload -c ${keyfile} -p ${outdir} -v -d ${tumor_id}").substitute(params)
	subprocess.check_call(cmd, shell=True)

STEPS=[gtdownload]
