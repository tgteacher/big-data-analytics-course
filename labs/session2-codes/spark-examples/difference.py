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
    output: spark-output/ralational-algebra-op-out/difference/
    local -> file:/home/mojtaba/Desktop/spark-examples/inputs/ralational-algebra-op-input/
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    this program compute the difference based on year attribute from
    selected table which is defined as third argument of main method
    SELECT year from employee WHERE year NOT IN (SELECT year from other_tables)
    :param argv: input address, output address, and selected table name(we used "employee" for testing)
    """
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    table_name = argv[3]
    files = sc.wholeTextFiles(sys.argv[1])
    data = files.flatMap(pair_creator).map(lambda x: (x[1], x[0])).mapValues(lambda x: x.split(",")[0])\
        .map(lambda x: (x[1], x[0])).distinct().groupByKey().map(lambda x: (x[0], list(x[1])))\
        .filter(lambda x: len(x[1]) == 1 and table_name in x[1])
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
