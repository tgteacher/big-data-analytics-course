#!/usr/bin/env python

import argparse
import random
import math
import matplotlib.pyplot as plt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates 2D points aggregated in clusters")
    parser.add_argument("n_clusters", type=int, help="Number of clusters")
    parser.add_argument("n_points", type=int, help="Number of points")
    parser.add_argument("inter_cluster",type=float, help="Minimum distance between cluster centroids")
    parser.add_argument("intra_cluster",type=float, help="Mean distance between centroid and cluster elements")
    parser.add_argument("-d", action="store_true")
    args = parser.parse_args()

    n_clusters    = args.n_clusters
    inter_cluster = args.inter_cluster
    intra_cluster = args.intra_cluster
    n_points      = args.n_points
    display       = args.d
    
    # Puts the centroids on a 'spiral'
    clusters = [[0,0]]
    sign=1
    mod=2
    while len(clusters) < n_clusters:
        for i in range(0,mod):
            if len(clusters) >= n_clusters:
                break
            c = clusters[-1]
            if i < mod/2 :
                clusters.append([c[0]+inter_cluster*sign,c[1]])
            else:
                clusters.append([c[0],c[1]+inter_cluster*sign])
        sign=-sign
        mod+=2
        
    # Generate the points
    x = []
    y = []
    points_clusters = []
    for i in range(0,n_points):
        # pick up a cluster
        cluster = random.randint(0,n_clusters-1)
        max_dist = intra_cluster / math.sqrt(2)         
        # pick up  x and y deltas
        x_delta = random.uniform(-max_dist,max_dist)
        y_delta = random.uniform(-max_dist,max_dist)
        x.append(clusters[cluster][0]+x_delta)
        y.append(clusters[cluster][1]+y_delta)
        points_clusters.append(cluster)
        print x[-1],y[-1]
        
    if display:
        plt.plot(x, y, 'ro')
        plt.show()
