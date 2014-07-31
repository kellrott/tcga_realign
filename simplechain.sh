#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 32


PIPELINE="$1"
PARAMS="$2"

hostname
./simplechain.py install

./simplechain.py run --work /scratch/tcga_realign \
--out /pod/pstore/projects/ICGC/realign_output \
$PIPELINE $PARAMS \
--ncpus 16 --docker --data /pod/home/kellrott/keys/:/keys \
--data /pod/podstore/projects/PAWG/reference/:/data 

