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
    print('Loading Index')
    df_index = spark.read.parquet('../Data/df_index2.parquet')
    print('Loading Links')
    df_links = spark.read.parquet('../Data/df_links2.parquet')
    print('Loading Tokens')
    df_content = spark.read.parquet('../Data/processed/text*').select('Content_Index','cleanTokens')
    print('Loading Complete')
    Link2Keyword = df_index.rdd.map(lambda x: (x['Link_ID'], x['Keyword'])).toDF(['Link_ID', 'Keyword'])
    Link2Trim = df_links.select('Link_ID', 'TrimmedLink_ID')
    Link2Content = df_content.select('TrimmedLink_ID', 'Content_Index')
    Keys2Content = Link2Keyword.join(Link2Trim, 'Link_ID').select('Keyword', 'TrimmedLink_ID').groupby(
        'TrimmedLink_ID').agg(collect_set('Keyword').alias('Keyword')).join(Link2Content, 'TrimmedLink_ID').select(
        'Keyword', 'Content_Index')
    KeysExpanded = Keys2Content.rdd.flatMap(
        lambda x: [(x['Content_Index'], searchLabeler.transform([keyword])[0]) for keyword in x['Keyword']])


if __name__ == '__main__':
    main()
