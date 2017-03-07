#!/usr/bin/env python

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)

input_text_file=sys.argv[1]
output_text_file=sys.argv[2]

counts=sc.textFile(input_text_file).\
        flatMap(lambda x: x.split()).\
        map(lambda x: (x,1)).\
        reduceByKey(lambda x,y: x+y)

counts.saveAsTextFile(output_text_file)
