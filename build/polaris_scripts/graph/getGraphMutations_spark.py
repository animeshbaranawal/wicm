import sys
import random
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

def getMutations(line, bwindows, workers):
    windows = bwindows.value
    x = line.strip().split(" ")
    print(x)
    vid = x[0]
    vStart = x[1]
    vEnd = x[2]

    mutations = []
    vertexCreated = None
    for i in range(1, len(windows)):
        sameLine = False
        separator = ""

        wStart = windows[i-1]
        wEnd = windows[i]

        mutation = ""
        if wStart <= int(vStart) < wEnd:
            if i != 1:
                mutation = mutation + "ADD "
            mutation = mutation + vid + " " + vStart
            vertexCreated = i
            sameLine = True

        if wStart < int(vEnd) <= wEnd:
            if i == vertexCreated:
                mutation = mutation + "/" + vEnd
                sameLine = True
            else:
                mutation = "TRUNCATE "+vid+" "+vEnd
                sameLine = False
                separator = "\n"
                if i != (len(windows)-1):
                    mutations.append((str(i+1)+"-"+str(random.randint(0, workers)), "REMOVE "+vid))

        for eIndex in range(3, len(x), 3):
            eId = x[eIndex]
            eStart = int(x[eIndex+1])
            eEnd = int(x[eIndex+2])

            if eStart < wEnd and eEnd > wStart:
                s = max(wStart, eStart)
                e = min(wEnd, eEnd)
                if sameLine:
                    mutation = mutation + " " + eId + " " + str(s) + " " + str(e)
                else:
                    mutation = mutation + separator + "EDGE " + vid + " " + eId + " " + str(s) + " " + str(e)
                    sameLine = True

        if mutation != "":
            mutations.append((str(i)+"-"+str(random.randint(0, workers)), mutation))

    return mutations


### Native
# windows = [0, 5, 10, 15]
# workers = 1
# fileHandlers = {}
# for i in range(1, len(windows)):
#     if i == 1:
#         fileHandlers[i] = open('window-'+str(i-1)+".txt", 'w')
#     else:
#         fileHandlers[i] = open('window-' + str(i-1) + "-0", 'w')
#
# fileName = open(sys.argv[1])
# for line in fileName:
#     mutations = getMutations(line, windows, 0)
#     for m in mutations:
#         fileHandlers[m[0][0]].write(m[1]+"\n")
# fileName.close()
#
# for k in fileHandlers:
#     fileHandlers[k].close()


### SPARK
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphMutations")\
        .getOrCreate()

    sc = spark.sparkContext
    window_broadcast = sc.broadcast([0, 10, 20, 36])
    workers = 1

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    mutations = lines.flatMap(lambda r: getMutations(r, window_broadcast, workers-1))
    dummy = sc.parallelize([("1-0","-1 0")])
    mutations = mutations.union(dummy)
    mutationDF = mutations.toDF(["key", "text"])

    mutationDF.write.partitionBy("key").text("text")
    # x = mutationDF.collect()
    # print(x)
