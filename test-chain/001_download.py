
import subprocess

def infodownload(params):
    subprocess.check_call("/cgquery analysis_id=%s > normal_data" % (params['normal_id']), shell=True)
    subprocess.check_call("/cgquery analysis_id=%s > tumor_data" % (params['tumor_id']), shell=True)

    yield ('normal_info', 'normal_data')
    yield ('tumor_info', 'tumor_data')


STEPS=[infodownload]
RESUME=True
STORE=False
IMAGE="test-chain-image"
CLUSTER_MAX=2
