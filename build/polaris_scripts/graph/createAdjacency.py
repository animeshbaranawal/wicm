import sys
import random
import pickle

from pyspark.sql import SparkSession

def createEdge(x):
    l = x.split(" ")
    start = l[1]
    end = l[2]
    return (start,end)

def getLine(x):
    vId = x[0]
    edges = x[1]
    line = vId
    line = line + " ".join(edges)
    return line

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphStats")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    edges = lines.map(lambda l: createEdge(l))
    adjacency = edges.groupByKey().mapValues(list).map(lambda x: getLine(x))

    adjacency.saveAsTextFile('rmat')

    spark.stop()

