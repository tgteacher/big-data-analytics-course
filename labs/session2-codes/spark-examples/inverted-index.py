import os
import sys
from pyspark import SparkContext, SparkConf
from os.path import basename

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"


def pair_creator(pair):
    file_path, text = pair
    return [line.split(",") + [basename(file_path)] for line in text.splitlines()]


def main(argv):
    """
    Created by TeamZero on 23/01/22.
    input: inputs/word-count-input
    output: spark-output/inverted-index-out/
    local -> file:/home/mojtaba/Desktop/spark-examples/inputs/word-count-input
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    :param argv: first is input and second is output address
    """
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    files = sc.wholeTextFiles(sys.argv[1])
    data = files.flatMap(pair_creator).map(lambda x: (x[1], x[0])).flatMapValues(lambda x: x.split())\
        .map(lambda x: (x[1], x[0])).distinct().reduceByKey(lambda x, y: str(x) + ', ' + str(y))
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
