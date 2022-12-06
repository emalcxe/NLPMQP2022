#!/bin/bash
#SBATCH --output=./logs/latest.out 
#SBATCH --error=./logs/latest.err 
#SBATCH -N 1
#SBATCH -c 24
#SBATCH -p short
source ~/startup.sh
if [ ! -d "./venv" ] 
then
	python3 -m venv venv
fi
source ./venv/bin/activate
pip3 install pyspark=3.1.2 textblob vaderSentiment
spark-submit --master $SPARK_CLUSTER Sentiment.py

