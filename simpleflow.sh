#!/bin/bash
#$ -S /bin/bash
#$ -cwd
#$ -pe smp 32

./simpleflow.py run --work /scratch/simple mutcaller test.request $1 --ncpus 32