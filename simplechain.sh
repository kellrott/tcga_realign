#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 32
#$ -l scratch=4000g

PIPELINE="$1"

hostname
./simplechain.py $PIPELINE install
./simplechain.py $PIPELINE client --clean --workdir /scratch/tcga_realign --z podk.local:2181

