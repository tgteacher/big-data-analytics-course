#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
import os

print "Initialize Spark session"
spark = SparkSession \
    .builder \
    .appName("Lab session") \
    .getOrCreate()

print "Read dataset"
example_file_path=os.path.join(os.environ['SPARK_HOME'],"data/mllib/sample_kmeans_data.txt")
dataset = spark.read.format("libsvm").load("file://"+example_file_path)

print "Fit kmeans model to dataset"
kmeans = KMeans(k=2)
model=kmeans.fit(dataset)

print "Show centroids"
centers = model.clusterCenters()
print centers

print "Apply model to data and show result"
model.transform(dataset).show()

spark.stop()
