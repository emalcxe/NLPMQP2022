import json
import concurrent.futures
import math
import os
import numpy as np

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import min,max,col

import glob
import re


def main():
    spark = SparkSession.builder \
        .appName("NLP CTF-IDF by keyword") \
        .config("spark.driver.memory","460g") \
        .config("spark.executor.memory","128g") \
        .getOrCreate()
    parqs = glob.glob('../Data/processed/text*')
    halfParqs = len(parqs)//2
    df_content = None
    for parq in parqs[:halfParqs]:
        print(f'Loading and working on parquet {parq}')
        temp  = spark.read.parquet(parq) \
            .select('Content_Index','cleanTokens').rdd \
            .filter(lambda x: len(x[1]) > 10) \
            .map(lambda x: (x[0]," ".join([word['result'] for word in x[1]]))) \
            .toDF(["Content_Index","cleanTokens"])
        if df_content  is not None:
            df_content = df_content.union(temp)
        else:
            df_content = temp
        print(df_content.tail(5))
        print(f'Currently operating on {df_content.count()} rows')
    print('Loading Complete')
    df_content.write.parquet('../Data/processed/text/complete1.parquet')


if __name__ == '__main__':
    main()
