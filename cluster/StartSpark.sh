#!/bin/bash
#SBATCH -o ./logs/spark.out
#SBATCH -e ./logs/spark.err
#SBATCH --ntasks-per-node 1
pip3 install --user spark-nlp==4.2.1 pyspark==3.1.2
srun -l --multi-prog startCluster.conf
