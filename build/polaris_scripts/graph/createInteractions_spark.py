import sys
import random
import pickle

from pyspark.sql import SparkSession

def getSourceLines(x, sources):
    l = x.split(",")
    sid = int(l[0])
    if sid in sources:
        return True
    return False

def getInteraction(x):
    l = x.split(",")
    localArray = dict()
    endTime = 4*int(l[2])
    edges = l[4]
    if edges != "":
        edgeArray = edges.split("|")
        for eIndex in range(0,len(edgeArray),3):
            eStart = 4*int(edgeArray[eIndex+1])
            eEnd = 4*int(edgeArray[eIndex+2])
            for interactionIndex in range(eStart,eEnd):
                destinationTimepoint = interactionIndex + 1
                for x in range(destinationTimepoint,endTime):
                        if (interactionIndex, x) not in localArray:
                            localArray[(interactionIndex, x)] = 0
                        localArray[(interactionIndex, x)] += 1
    
    return [(k, localArray[k]) for k in localArray.keys()]


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
        .appName("Interaction")\
        .getOrCreate()

    sources = [122888184,119873820,131309869,132683886,133559808,126361152,84927704,84844129,54128989,81907727,26927943,26927946]

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    interactions = lines.filter(lambda l: getSourceLines(l, sources))

    interactions.saveAsTextFile("sourceLines")
    spark.stop()

    '''
    start = 0
    end = 48
    interaction_array = []
    for i in range(end):
        interaction_array.append([0]*end)

    interactions_dict = interactions.collect()
    for item in interactions_dict:
        interaction_array[item[0][0]][item[0][1]] = item[1]

    # print(interaction_array)
    spark.stop()

    pickle.dump(interaction_array, open("/home/hadoop/graphiteOOC/build/WebUKExtended_interactions.bin", 'wb'))
    '''
