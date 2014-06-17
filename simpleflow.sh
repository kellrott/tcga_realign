#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 32


PIPELINE="$1"
REQUESTS="$2"
WORK_ID="$3"
WORK_DIR="$4"

./simpleflow.py run --work $WORK_DIR $PIPELINE $REQUESTS $WORK_ID --ncpus 32