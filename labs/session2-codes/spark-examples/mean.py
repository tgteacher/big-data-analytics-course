import os
import sys
from pyspark import SparkContext, SparkConf

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"

# Created by TeamZero on 23/01/23.
# input: inputs/mean-input/
# output: spark-output/mean-out/
# local -> file:/home/mojtaba/Desktop/spark-examples/inputs/mean-input/
# hadoop -> hdfs://namenode:port/[file address]
# use same pattern for output


def main(argv):
    conf = SparkConf().setMaster('local').setAppName('word count')
    sc = SparkContext(conf=conf)
    files = sc.textFile(sys.argv[1])
    data = files.map(lambda x: (x.split()[0], x.split()[1])).groupByKey().map(lambda x: (x[0], list(x[1])))\
        .mapValues(lambda x: sum(int(i) for i in x)/len(x))
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
