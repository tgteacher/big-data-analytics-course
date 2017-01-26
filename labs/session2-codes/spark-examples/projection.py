import os
import sys
from pyspark import SparkContext, SparkConf

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"

# Created by TeamZero on 23/01/22.
# input: inputs/ralational-algebra-op-input/users
# output: spark-output/ralational-algebra-op-out/projection/
# local -> file:/home/arz/Desktop/spark-examples/inputs/ralational-algebra-op-input/users
# hadoop -> hdfs://namenode:port/[file address]
# use same pattern for output


def main(argv):
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    files = sc.textFile(sys.argv[1])
    data = files.map(lambda x: x.split(",")[1]).distinct().map(lambda x: (x, x))
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
