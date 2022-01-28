import sys
import random
import pickle

from pyspark.sql import SparkSession

def expandGraph(x, factor):
    l = x.split(" ")
    l[1] = str(int(l[1]) * factor)
    l[2] = str(int(l[2]) * factor)
    for eIndex in range(3, len(l), 3):
        l[eIndex+1] = str(int(l[eIndex+1]) * factor)
        l[eIndex+2] = str(int(l[eIndex+2]) * factor)
    return " ".join(l)

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphStats")\
        .getOrCreate()

    factor = 2
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    newLines = lines.map(lambda l: expandGraph(l, factor))
    newLines.saveAsTextFile(sys.argv[2])
    spark.stop()
