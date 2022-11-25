import concurrent.futures
import math
import os
import numpy as np

import sparknlp_jsl
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from sparknlp.annotator import *
from sparknlp_jsl.annotator import MedicalNerModel
from pyspark.sql.functions import min,max

from tqdm import trange
import glob
import re



class NovelPipeline:
    def __init__(self, spark=None, params=None):
        self._spark = spark
        document_assembler = DocumentAssembler() \
            .setInputCol("Content") \
            .setOutputCol("document")

        sentence_detector = SentenceDetectorDLModel.pretrained("sentence_detector_dl_healthcare", "en",
                                                               "clinical/models") \
            .setInputCols(["document"]) \
            .setOutputCol("sentence")

        tokenizer = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token")

        spellModel = ContextSpellCheckerModel \
            .pretrained("spellcheck_dl", "en") \
            .setInputCols("token") \
            .setOutputCol("checked")

        lemmatizer = LemmatizerModel.pretrained() \
            .setInputCols(["checked"]) \
            .setOutputCol("lemma")

        normalizer = Normalizer() \
            .setInputCols(["lemma"]) \
            .setOutputCol("normal")

        stop_words = StopWordsCleaner.pretrained("stopwords_en", "en") \
            .setInputCols(["normal"]) \
            .setOutputCol("cleanTokens")

        word_embeddings = WordEmbeddingsModel.pretrained("embeddings_clinical", "en", "clinical/models") \
            .setInputCols(["sentence", "cleanTokens"]) \
            .setOutputCol("embeddings")

        ner_oncology_wip = MedicalNerModel.pretrained("ner_oncology_wip", "en", "clinical/models") \
            .setInputCols(["sentence", "cleanTokens", "embeddings"]) \
            .setOutputCol("ner_oncology_wip")

        ner_oncology_wip_converter = NerConverter() \
            .setInputCols(["sentence", "cleanTokens", "ner_oncology_wip"]) \
            .setOutputCol("ner_oncology_wip_chunk")

        self._pipeline = Pipeline(stages=[document_assembler,
                                          sentence_detector,
                                          tokenizer,
                                          spellModel,
                                          lemmatizer,
                                          normalizer,
                                          stop_words,
                                          word_embeddings,
                                          ner_oncology_wip,
                                          ner_oncology_wip_converter])

    def getPipeline(self):
        return self._pipeline

    def trainFiltered(self, docs, index, low, high):
        docs = docs.filter((low <= col(index)) & (col(index) < high))
        return self._pipeline.fit(docs).transform(docs)

    # def train(self, docs):
    #     return self._pipeline.fit(docs).transform(docs)
    #
    # def _trainHelp(self, args):
    #     return self.train(*args)
    #
    # def batchTrain(self, documents, index='Index', N=None, batch_size=10000):
    #     if not N:
    #         N = documents.count()
    #     firstDone = False
    #     df = None
    #     for i in range(math.ceil(N / batch_size)):
    #         res = self.trainFiltered(documents, index, i * batch_size, (i + 1) * batch_size)
    #         if not firstDone:
    #             df = res
    #             firstDone = True
    #         else:
    #             df = df.union(res)
    #     return df
    @property
    def pipeline(self):
        return self._pipeline


def main():
    spark = sparknlp_jsl.start(os.environ.get('SECRET'), params={
        'spark.driver.memory': '24g',
        'spark.executor.memory': '36g',
        'spark.driver.cores': '2',
        'spark.executor.cores': '16'
    })
    content = spark.read.parquet('../Data/df_content3.parquet')
    print('Finding Range')
    RANGE = content.select(min('Content_Index'), max('Content_Index')).collect()[0]
    LOW, HIGH = RANGE[0], RANGE[1]
    print('Finding starting point')
    parsed = glob.glob('../Data/ProcessedText/*.parquet')
    if parsed:
        print(parsed)
        rgx = re.compile(r'([0-9]+)\.parquet')
        print(rgx.search(parsed[0]))
        result = [int(m.group(1)) for m in (rgx.search(line) for line in parsed) if m]
        print(result)
        LOW = np.max(result)

    cursor = LOW
    N = HIGH - LOW
    batch_size = 5000
    print('Training Pipeline')
    pipe = NovelPipeline(spark).getPipeline().fit(content)
    for batch in trange((N // batch_size) + 1):
        docs = content.filter((cursor <= col('Content_Index')) & (col('Content_Index') < cursor+batch_size)).select('Content')
        result = pipe.transform(docs)
        result.write.parquet(f'../Data/ProcessedText/Processed{cursor+batch_size}.parquet')
        cursor += batch_size


if __name__ == '__main__':
    main()
