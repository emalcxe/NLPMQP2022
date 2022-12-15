#!/bin/bash
#SBATCH --output=./logs/Out-%j.out 
#SBATCH --error=./logs/Err-%j.err 
#SBATCH -N 1
#SBATCH -c 16
#SBATCH -p short
source ./venv/bin/activate
pip3 install sparknlp==$PUBLICVERSION pyspark=3.1.2 
pip3 install scikit-learn scipy numpy
spark-submit --master $SPARK_CLUSTER NovelCTFIDF.py

