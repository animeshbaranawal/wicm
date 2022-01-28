import sys
import random

from pyspark.sql import SparkSession

def createVertexInterval(x):
    originalInterval = x[1]

    random.seed(hash(x[0]))
    start = 0
    toAdd = (random.uniform(0,1) < 0.72)
    while (not toAdd) and start < originalInterval[0]:
        start += 1
        toAdd = (random.uniform(0,1) < 0.72)

    end = originalInterval[1]
    toDelete = (random.uniform(0,1) < 0.04)
    while (not toDelete) and end < 30:
        end += 1
        toDelete = (random.uniform(0,1) < 0.04)

    return (x[0], (start, end))

def createEdgeInterval(x):
    edgeStart = 0
    random.seed(hash(x[0])+hash(x[1]))
    toAdd = (random.uniform(0,1) < 0.75)
    while (not toAdd) and edgeStart < 29:
        edgeStart += 1
        toAdd = (random.uniform(0,1) < 0.75)

    edgeEnd = edgeStart + 1
    toDelete = (random.uniform(0,1) < 0.12)
    while (not toDelete) and edgeEnd < 30:
        edgeEnd += 1
        toDelete = (random.uniform(0,1) < 0.12)
    
    return (x[0], (x[1], (edgeStart, edgeEnd)))

def extend(a,b):
    return (min(a[0],b[0]), max(a[1],b[1]))

def createVertexLine(x):
    source = x[0]
    sourceInterval = x[1][0]
    baseString = str(source)+" "+str(sourceInterval[0])+" "+str(sourceInterval[1])
    if x[1][1] == None:
        return baseString
    else:
        for e in x[1][1]:
            baseString = baseString + " " + str(e[0]) + " "+str(e[1][0])+" "+str(e[1][1])
        return baseString

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("Twitter")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    edges = lines.map(lambda r: (int(r.split(" ")[0]), int(r.split(" ")[1]))).map(lambda r: createEdgeInterval(r))
    vertices = edges.flatMap(lambda r: [(r[0], r[1][1]), (r[1][0], r[1][1])]).reduceByKey(lambda a,b: extend(a,b)).map(lambda x: createVertexInterval(x))
    graph = vertices.leftOuterJoin(edges.groupByKey()).map(lambda r: createVertexLine(r))
    graph.saveAsTextFile("TwitterTemporalGraph.txt")

    spark.stop()
