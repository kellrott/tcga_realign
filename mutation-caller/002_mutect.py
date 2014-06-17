
import subprocess
import string
from glob import glob
import os


def call_mutect(params):
    template = """/pod/home/kellrott/mutcall_wrapper/mutcall_wrapper.py --refseq /pod/podstore/projects/PAWG/reference/genome.fa \
--tumor ${tumor_bam} \
--normal ${normal_bam} \
--method mutect \
--cosmic-vcf /pod/podstore/projects/PAWG/reference/b37_cosmic_v54_120711.vcf \
--dbsnp /pod/podstore/projects/PAWG/reference/dbsnp_132_b37.leftAligned.vcf \
--out mutect_out \
--cpus ${ncpus}"""

    cmd = string.Template(template).substitute(params)
    subprocess.check_call(cmd, shell=True)
    yield ("mutect_vcf", "mutect_out/out.MuTect.vcf")


STEPS = [call_mutect]
STORE = False
RESUME= False
IMAGE = "mutect"
FAIL = "soft"