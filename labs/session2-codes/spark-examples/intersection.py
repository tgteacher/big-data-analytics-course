import os
import sys
from pyspark import SparkContext, SparkConf
from os.path import basename

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"


def pair_creator(pair):
    file_path, text = pair
    return [line.split("\n") + [basename(file_path)] for line in text.splitlines()]


def main(argv):
    """
    Created by TeamZero on 23/01/23.
    input: inputs/ralational-algebra-op-input/
    output: spark-output/ralational-algebra-op-out/intersection/
    local -> file:/home/mojtaba/Desktop/spark-examples/inputs/ralational-algebra-op-input/
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    this program compute the intersection based on year attribute between all documents or tables
    which is accessible with input argument
    :param argv: first is input, second is output, and third is the number of
                 table which you have in input address(we have "3" tables in our input folder)
    """
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    tables_count = int(argv[3])
    files = sc.wholeTextFiles(sys.argv[1])
    data = files.flatMap(pair_creator).map(lambda x: (x[1], x[0])).mapValues(lambda x: x.split(",")[0]) \
        .map(lambda x: (x[1], x[0])).distinct().groupByKey().map(lambda x: (x[0], list(x[1]))) \
        .filter(lambda x: len(x[1]) == tables_count)
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
