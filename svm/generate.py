#!/usr/bin/env python

import argparse
import random
import math
import matplotlib.pyplot as plt

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates 2D points in two classes separated by a line")
    parser.add_argument("n_points", type=int, help="Number of points")
    parser.add_argument("a",type=float, help="as in y=a*x+b")
    parser.add_argument("b",type=float, help="as in y=a*x+b")
    args = parser.parse_args()

    n_points      = args.n_points
    a             = args.a
    b             = args.b

    for i in range(0,n_points):
        # pick up  x and y deltas
        x = random.uniform(0,10)
        y = random.uniform(0,10)
        if y > a*x+b:
          klass=1
        else:
          klass=0
        print klass,x,y
        
