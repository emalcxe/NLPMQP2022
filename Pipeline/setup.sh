#!/bin/bash
export JSL_VERSION=4.2.1
export PUBLIC_VERSION=4.2.1
source ./venv/bin/activate
pip3 install sklearn numpy tqdm pyarrow textblob vaderSentiment 
pip3 install --upgrade -q pyspark==3.1.2 spark-nlp==$PUBLIC_VERSION 
pip3 install --upgrade -q spark-nlp-jsl==$JSL_VERSION  --extra-index-url https://pypi.johnsnowlabs.com/$SECRET 
