#!/bin/bash
#SBATCH --output=./logs/Out-%j.out 
#SBATCH --error=./logs/Err-%j.err 
#SBATCH -N 1
#SBATCH -n 20
#SBATCH -p long
#SBATCH --mem 128G
source ../setup.sh
source ../cred.sh
python3 NovelPipeline.py
