#!/bin/bash
#SBATCH --output=./logs/Out-%j.out 
#SBATCH --error=./logs/Err-%j.err 
#SBATCH -N 1
#SBATCH -c 16
#SBATCH -p short
spark-submit --master $SPARK_CLUSTER CleanTokens.py
