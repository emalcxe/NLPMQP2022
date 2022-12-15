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
    df_content = spark.read.parquet('../Data/processed/tokens/*.parquet').join(spark.reqad.parquet('../Data/df_content3.parquet').select('Content_Index','TrimmedLink_ID'))
    print(df_content.show()) 
    #df_content.write.parquet('../Data/processed/text/complete1.parquet')


if __name__ == '__main__':
    main()
