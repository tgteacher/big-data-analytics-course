import os
import sys
from pyspark import SparkContext, SparkConf

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"


def main(argv):
    """
    Created by TeamZero on 23/01/22.
    input: inputs/ralational-algebra-op-input/users
    output: spark-output/ralational-algebra-op-out/selection/
    local -> file:/home/arezou/Desktop/spark-examples/inputs/ralational-algebra-op-input/users
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    this program compute the selection based on the year which is equal or greater than the condition
    SELECT * from tableName WHERE year >= base_limit(for testing we used 1975 as a base limit)
    :param argv: first is input. second is output address,
                 and the third is the year which you want to use as a base limit(for example 1975)
    """
    conf = SparkConf().setMaster('local').setAppName('inverted index')
    sc = SparkContext(conf=conf)
    base_limit = int(argv[3])
    files = sc.textFile(sys.argv[1])
    data = files.map(lambda x: (x.split(",")[0], x)).filter(lambda x: int(x[0]) >= base_limit)
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()

if __name__ == "__main__":
    main(sys.argv)
