import sys
import random
import pickle

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphStats")\
        .getOrCreate()

    start = 0
    end = 10

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    incorrect_lines = lines.filter(lambda l: len(l.split(",")) < 5)
    incorrect_lines.saveAsTextFile("WebUK_dbg")
    
    spark.stop()
