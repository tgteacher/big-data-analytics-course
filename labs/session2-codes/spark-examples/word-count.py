import os
import sys
from pyspark import SparkContext, SparkConf

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"

# Created by TeamZero on 23/01/20.
# input: inputs/word-count-input
# output: spark-output/word-count-out/
# local -> file:/home/arezou/Desktop/spark-examples/inputs/word-count-input
# hadoop -> hdfs://namenode:port/[file address]
# use same pattern for output


def main(argv):
    conf = SparkConf().setMaster('local').setAppName('word count')
    sc = SparkContext(conf=conf)
    files = sc.textFile(sys.argv[1])
    data = files.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
