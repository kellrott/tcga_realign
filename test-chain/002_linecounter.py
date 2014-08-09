
import time

def do_normal_count(params):
    with open(params['normal_info']) as handle:
        count = 0
        for line in handle:
            count += 1
    time.sleep(30)
    return []

def do_tumor_count(params):
    with open(params['tumor_info']) as handle:
        count = 0
        for line in handle:
            count += 1
    time.sleep(3)
    return []


STEPS=[do_normal_count, do_tumor_count]
RESUME=False
STORE=False
IMAGE="test-chain-image"
