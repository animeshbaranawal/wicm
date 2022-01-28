import sys
import random

from pyspark.sql import SparkSession

def getReverseEdges(x):
    edges = []
    l = x.split()
    source = l[0]

    for eIndex in range(3,len(l),3):
        edges.append((l[eIndex], source+"|"+l[eIndex+1]+"|"+l[eIndex+2]))
    return edges

def getReverseString(x):
    return (x[0], "|".join(x[1]))

def getString(x):
    l = x.split()
    return (l[0], l[1]+","+l[2]+","+"|".join(l[3:]))

def extend(a,b):
    return (min(a[0],b[0]), max(a[1],b[1]))

def createVertexLine(x):
    source = x[0]
    if x[1][1] == None:
        return source+","+x[1][0]+","
    else:
        return source+","+x[1][0]+","+x[1][1]

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("SCC")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    original = lines.map(lambda l: getString(l))
    
    reverseEdges = lines.flatMap(lambda l: getReverseEdges(l))
    reverseAdjList = reverseEdges.groupByKey().map(lambda r: getReverseString(r))
    SCCGraph = original.leftOuterJoin(reverseAdjList).map(lambda l : createVertexLine(l))

    SCCGraph.saveAsTextFile(sys.argv[2])

    spark.stop()
