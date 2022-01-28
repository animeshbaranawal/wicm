import sys
import random
import pickle

#from pyspark.sql import SparkSession

def getSnap(x, snapshot):
    l = x.split(" ")
    vid = l[0]
    start = int(l[1])
    end = int(l[2])
    output = ""
    if start <= snapshot and snapshot < end:
        output = output + vid
    
    for eIndex in range(3, len(l), 3):
        eId = l[eIndex]
        eStart = int(l[eIndex+1])
        eEnd = int(l[eIndex+2])
        if eStart <= snapshot and snapshot < eEnd:
            output = output + " " + eId
    
    return output

if __name__ == "__main__":
    '''
    spark = SparkSession\
        .builder\
        .appName("GraphStats")\
        .getOrCreate()

    snapshot = 73
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    newLines = lines.map(lambda l: getSnap(l, snapshot))
    newLines.saveAsTextFile("snapshot")
    spark.stop()
    '''

    snapshot = 0
    count = 0

    inputHandler = open(sys.argv[1], 'r')
    outputHandler = open(sys.argv[2], 'w')
    for l in inputHandler:
        ol = getSnap(l, snapshot)
        if ol != "":
            outputHandler.write(ol+"\n")
        
        count += 1
        if count % 100000 == 0:
            print(count)
    outputHandler.close()
    inputHandler.close()
