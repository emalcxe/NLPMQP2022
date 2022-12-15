import concurrent.futures
import math
import os
import numpy as np

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import min, max, col, collect_set, row_number
from sklearn import preprocessing
import glob
import re

def vectorize(df,x, labeler):
    vec = np.empty(labeler.n_features_in_)
    for token in set(x[1].split()):
        idx = labeler.transform(token)
        vec[idx] = df.select(f'TFIDF where Cluster == {x[0]} and Token == {token}').collect()[0].TFIDF
    return vec

def main():
    spark = SparkSession.builder \
        .appName("NLP CTF-IDF by keyword") \
        .config("spark.driver.memory", "460g") \
        .config("spark.executor.memory", "128g") \
        .getOrCreate()
    print('Loading Index')
    df_index = spark.read.parquet('../Data/df_index2.parquet')
    print('Loading Links')
    df_links = spark.read.parquet('../Data/df_links2.parquet')
    print('Loading Tokens')
    df_content = spark.read.parquet('../Data/processed/tokens/*.parquet').select('Content_Index', 'cleanTokens')
    df_trimmed = spark.read.parquet('../Data/df_content3.parquet').select('Content_Index', 'TrimmedLink_ID')
    df_content = df_content.join(df_trimmed, 'Content_Index')
    print('Loading Complete')
    # Create Keyword Links
    Link2Keyword = df_index.rdd.map(lambda x: (x['Link_ID'], x['Keyword'])).toDF(['Link_ID', 'Keyword'])
    Link2Trim = df_links.select('Link_ID', 'TrimmedLink_ID')
    Link2Content = df_content.select('TrimmedLink_ID', 'Content_Index')
    Keys2Content = Link2Keyword.join(Link2Trim, 'Link_ID').select('Keyword', 'TrimmedLink_ID').groupby(
        'TrimmedLink_ID').agg(collect_set('Keyword').alias('Keyword')).join(Link2Content, 'TrimmedLink_ID').select(
        'Keyword', 'Content_Index')
    KeysExpanded = Keys2Content.rdd.flatMap(
        lambda x: [(keyword, x['Content_Index']) for keyword in x['Keyword']]).toDF(['Key', 'Content_Index'])

    # TF-IDF
    K = 113
    tokens = KeysExpanded.join(df_content, 'Content_Index').select('Key', 'cleanTokens')
    term2one = tokens.rdd \
        .flatMap(lambda x: [((x[0], word), 1) for word in x[1].split()])  # ((doc,word),1) * N
    termPerDoc = term2one \
        .map(lambda x: (x[0][0], 1)) \
        .reduceByKey(lambda x, y: x + y)  # (doc, 1) * K -> (doc, K)
    termReduced = term2one \
        .reduceByKey(lambda x, y: x + y)  # ((doc,word),N)
    termDivide = termReduced \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .join(termPerDoc) \
        .map(lambda x: (
    (x[0], x[1][0][0]), x[1][0][1] / x[1][1]))  # ((doc,word),N)->(doc,(word,N))->(doc,((word,N),K)->((doc,word),N/K)
    tf = termDivide \
        .map(lambda x: (x[0][1], (x[0][0], x[1])))  # ((doc,word),N/K) -> (word, (doc, n))
    presence = termReduced \
        .map(lambda x: (x[0][1], (x[0][0], x[1], 1))) \
        .map(lambda x: (x[0], x[1][2]))  # ((doc,word),N) -> (word, 1)
    presenceReduced = presence \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: (K * 0.95) > x[1] > (K * 0.05))  # (word, 1) -> (word, j)
    idf = presenceReduced \
        .map(lambda x: (x[0], float(np.log10(K / x[1])))) # (word, j) -> (word, log(K/j))
    tfidf = tf.join(idf) \
        .map(lambda x: (x[1][0][0], x[0], x[1][0][1], x[1][1], x[1][0][1] * x[1][1])) \
        .toDF(['Cluster', 'Token', 'TF', 'IDF', 'TFIDF']) \
        .sort(col('TFIDF').desc()).cache()

    DistinctKeys = [row[0] for row in tfidf.select('Cluster').distinct().collect()]
    Rows = Window.partitionBy("Cluster").orderBy(col('TFIDF').desc())
    out = tfidf.withColumn("row",row_number().over(Rows)).filter(col("row")<=50).drop("row")
    unique = [token.Token for token in out.select('Token').distinct().collect()]
    intersect = [token for token in out.rdd.map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] == 113).map(lambda x: x[0]).collect()]

    #classLabel = preprocessing.LabelBinarizer().fit(DistinctKeys)
    #tokenLabel = preprocessing.LabelBinarizer().fit(tfidf.select('Token').distinct().collect())
    #clusterVecs = tfidf.select(["Cluster","Token","TFIDF"]).filter(f"Cluster == {DistinctKeys[1]}").rdd.map(lambda x: (x[0],(tokenLabel.transform([x[1]])*x[2]).tolist())).reduceByKey(lambda x,y: x+y).toDF(['Cluster','clusVec'])
    #docVecs = tokens.rdd.map(lambda x: (x[0],(np.max(tokenLabel.transform([x[1]]))).tolist())).toDF(['Cluster','docVec'])
    #vec = clusterVecs.join(docVecs,'Cluster')
    print(clusterVecs.show())

if __name__ == '__main__':
    main()

