#!/usr/bin/env python
import sys
import pyspark
import os
import random
import time

# Helper functions
def distance2(x, y):
    return (x[0]-y[0])**2+(x[1]-y[1])**2

def distance2_set(x, C):
    min_dist = distance2(x, C[0])
    for c in C:
        d = distance2(x, c)
        if d < min_dist:
            min_dist = d
    return min_dist

def phi(X, C):
    sum = 0
    for x in X:
        sum += distance2_set(x, C)
    return sum

def closest_centroid(x, C):
    current = 0
    min_dist = distance2(x, C[current])
    for index, c in enumerate(C):
        d = distance2(x, c)
        if d < min_dist:
            current = index
            min_dist = d
    return current

def addpairs((x, a), (y, b)):
    z = [x[0]+y[0], x[1]+y[1]]
    c = a + b
    return (z, c)

def write_centroids(centroids, file_name):
    with open(file_name, 'w') as f:
        for c in centroids:
            f.write("{0} {1}\n".format(str(c[0]), str(c[1])))

def log(message):
    print("[ {0} ] {1}".format(round(time.time(),3)-START_TIME, message))
            
# Init functions

def init_kmeans(points, k):
    return points.sample(False, 0.1).take(k)

def init_kmeans_plus_plus(points, k):
    # take a random point
    point_list = points.collect()
    centroids = random.sample(point_list, 1)
    phi_value = phi(point_list, centroids)
    while len(centroids) < k:
        for x in point_list:
            proba = distance2_set(x, centroids)/phi_value
            if random.random() < proba:
                centroids.append(x)
                if len(centroids) == k:
                    break
    return centroids

### PROGRAM starts HERE

# Spark initialization
conf = pyspark.SparkConf().setAppName("kmeans").setMaster("local")
sc = pyspark.SparkContext(conf=conf)

# Arguments parsing
file_name=sys.argv[1]
k=int(sys.argv[2])
output_file_name=sys.argv[3]

# Initialization
START_TIME=time.time()
points=sc.textFile(file_name).map(lambda x: x.split(" ")).\
        map(lambda (x,y): (float(x), float(y)))
centroids=init_kmeans_plus_plus(points, k)
init_centroids=centroids

# kmeans
max_it=100
old_centroids=[]
for i in range(0, max_it):
    log("kmeans iteration "+str(i))
    # Assign points to centroids (in parallel)
    # and recompute centroids 
    points_assigned = points.map(
        lambda x: (closest_centroid(x, centroids), (x, 1))
    )
    centroid_pairs=points_assigned.reduceByKey(
        lambda x,y: addpairs(x,y)
    )
    # Finish centroid computation
    centroids = []
    for pair in centroid_pairs.collect():
        _, (c, count) = pair
        centroids.append((c[0]/count, c[1]/count))
    # Check convergence
    if sorted(centroids) == sorted(old_centroids):
        break
    old_centroids = centroids
        
points_assigned.map(lambda x: "{0} {1} {2}".format(x[0], x[1][0][0], x[1][0][1])).\
    saveAsTextFile(output_file_name)

write_centroids(centroids, os.path.join(output_file_name,"centroids_final.txt"))
write_centroids(init_centroids, os.path.join(output_file_name,"centroids_init.txt"))

print("Final cost: {0}".format(phi(points.collect(), centroids)))
