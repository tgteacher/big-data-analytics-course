#!/usr/bin/env python

import matplotlib.pyplot as plt
import math
import argparse

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Displays k-means results")
  parser.add_argument("data_file", type=argparse.FileType('r'), help="The file containing data points");
  args = parser.parse_args()
  color=[]
  x=[]
  y=[]
  area=[]
  for line in args.data_file:
    aa,xx,yy=str.split(line)
    color.append(aa)
    area.append(math.pi*6**2)
    x.append(xx)
    y.append(yy)
  plt.scatter(x,y,c=color,s=area)
 
  plt.show()

