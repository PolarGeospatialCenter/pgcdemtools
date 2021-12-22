#!/bin/bash

#PBS -l walltime=40:00:00,nodes=1:ppn=8,mem=32gb
#PBS -m n
#PBS -k oe
#PBS -j oe
#PBS -q new

cd $PBS_O_WORKDIR

echo $PBS_JOBID
echo $PBS_O_HOST
echo $PBS_NODEFILE
echo $a1

source ~/.bashrc
conda activate pgc

echo $p1
python $p1
