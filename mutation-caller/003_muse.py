
import subprocess
import string
from glob import glob
import os


def call_muse(params):
    template = """/pod/home/kellrott/mutcall_wrapper/mutcall_wrapper.py --refseq /pod/podstore/projects/PAWG/reference/genome.fa \
--tumor ${tumor_bam} \
--normal ${normal_bam} \
--method muse \
--cosmic-vcf /pod/podstore/projects/PAWG/reference/b37_cosmic_v54_120711.vcf \
--dbsnp /pod/podstore/projects/PAWG/reference/dbsnp_132_b37.leftAligned.vcf \
--out muse_out \
--cpus ${ncpus}"""

    cmd = string.Template(template).substitute(params)
    subprocess.check_call(cmd, shell=True)
    yield ("muse_txt", "muse_out/out.MuSE.txt")


STEPS = [call_muse]
STORE = False
RESUME= False
IMAGE="muse"