import os
import sys
from pyspark import SparkContext, SparkConf

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"


def main(argv):
    """
    Created by TeamZero on 23/01/20.
    input: inputs/ralational-algebra-op-input/
    output: spark-output/ralational-algebra-op-out/union
    local -> file:/home/arezou/Desktop/spark-examples/inputs/ralational-algebra-op-input/
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    :param argv: first is input and second is output address
    """
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    files = sc.textFile(sys.argv[1])
    data = files.map(lambda x: (x, x)).distinct()
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
