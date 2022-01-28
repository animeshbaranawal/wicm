import sys
import random

from pyspark.sql import SparkSession

def getVertexId(x):
    return x.split(" ")[0]

def getEdgeList(x):
    edges = []
    l = x.split(" ")
    source = l[0]
    for eIndex in range(3,len(l),3):
        edges.append((source, l[eIndex]))
    return edges

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("SCC")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    vertexList = lines.map(lambda l: getVertexId(l)).map(lambda l : str(l))
    edgeList = lines.flatMap(lambda l: getEdgeList(l)).map(lambda l : str(l[0])+" "+str(l[1]))
    
    vertexList.saveAsTextFile(sys.argv[2])
    edgeList.saveAsTextFile(sys.argv[3])

    spark.stop()
