#!/usr/bin/env python
import os
from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

example_file_path=os.path.join(os.environ['SPARK_HOME'],"data/mllib/sample_fpgrowth.txt")
data = sc.textFile("file://"+example_file_path)
transactions = data.map(lambda line: line.strip().split(' '))
model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
result = model.freqItemsets().collect()
for fi in result:
    print(fi)
