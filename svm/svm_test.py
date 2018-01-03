#!/usr/bin/env python

import sys, os, math
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt


# Initialize Spark context
conf = SparkConf().setAppName("svm").setMaster("local")
sc = SparkContext(conf=conf)

# argument parsing
input_text_file=sys.argv[1]
input_url="file://"+os.path.join(os.getenv("PWD"),input_text_file)

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile(input_url)
parsedData = data.map(parsePoint)

# Build the model
model = SVMWithSGD.train(parsedData, iterations=10000, intercept=True)
print "Model: ",model

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))

def f(x):
    u=model.weights[0]
    v=model.weights[1]
    b=model.intercept
    return -(u*x+b)/v

color=[]
x=[]
y=[]
area=[]

for line in open(input_text_file,'r'):
    aa,xx,yy=str.split(line)
    color.append(aa)
    area.append(math.pi*6**2)
    x.append(xx)
    y.append(yy)
    
plt.scatter(x,y,c=color,s=area)
plt.plot([0,10],[f(0),f(10)])
plt.show()

