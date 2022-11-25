#!/bin/bash
#SBATCH --output=simple-%j.out 
#SBATCH --error=simple-%j.err 
#SBATCH -N 1
#SBATCH -n 48
#SBATCH -p long
#SBATCH --mem 256G
. setup.sh
python3 NaiveKeyIdentifier.py
