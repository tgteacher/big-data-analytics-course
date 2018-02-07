#!/usr/bin/env python
from pyspark.mllib.clustering import KMeans, KMeansModel
import sys
import pyspark
import os
from math import sqrt

def write_centroids(centroids, file_name):
    with open(file_name, 'w') as f:
        for c in centroids:
            f.write("{0} {1}\n".format(str(c[0]), str(c[1])))

# Spark initialization
conf = pyspark.SparkConf().setAppName("kmeans").setMaster("local")
sc = pyspark.SparkContext(conf=conf)

# Arguments parsing
file_name=sys.argv[1]
k=int(sys.argv[2])
output_file_name=sys.argv[3]

# Initialization
points=sc.textFile(file_name).map(lambda x: x.split(" ")).\
        map(lambda (x,y): (float(x), float(y)))

clusters = KMeans.train(points, k, maxIterations=100, initializationMode="kmeans||")

points.map(lambda x: "{0} {1} {2}".format(clusters.predict(x), x[0], x[1])).\
    saveAsTextFile(output_file_name)

write_centroids(clusters.centers, os.path.join(output_file_name,"centroids_final.txt"))

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

wsse = points.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Final cost: " + str(wsse))
