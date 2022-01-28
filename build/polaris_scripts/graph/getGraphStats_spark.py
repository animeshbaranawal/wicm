import sys
import random
import pickle

from pyspark.sql import SparkSession

def getSnapshotVertices(x):
    l = x.split(" ")
    start = int(l[1])
    end = int(l[2])
    snapshots = []
    for s in range(start,end):
	snapshots.append((s,1))
    return snapshots

def getSnapshotEdges(x):
    l = x.split(" ")
    snapshots = []
    for eIndex in range(3,len(l),3):
        start = int(l[eIndex+1])
        end = int(l[eIndex+2])
        for s in range(start,end):
            snapshots.append((s,1))
    return snapshots

def getVertexDegree(x):
    l = x.split(" ")
    return (len(l[3:])//3, (int(l[0]), int(l[1]), int(l[2])))

### choose earlier starting source
def chooseDegreeSource(a, b):
    if a[1] < b[1]:
        return a
    else:
        return b

def getVertexStart(x):
    l = x.split(" ")
    return (int(l[1]), (int(l[0]), 1))

def getVertexStartDegree(x):
    l = x.split(" ")
    return (int(l[1]), (int(l[0]), len(l[3:])//3))

def chooseDegreeStartSource(a, b):
    if a[1] < b[1]:
        return b
    else:
        return a

def chooseSource(a, b):
    random.seed(hash(a) + hash(b))
    chooseSmaller = (random.uniform(0, 1) < 0.5)
    smallerId = a[0] if a[0] < b[0] else b[0]
    largerId = b[0] if b[0] > a[0] else a[0]
    if chooseSmaller:
        return (smallerId, a[1] + b[1])
    else:
        return (largerId, a[1] + b[1])

def getVertexLifespan(x):
    l = x.split(" ")
    return ((int(l[2])-int(l[1])), 1)

def getEdgeLifespan(x):
    l = x.split(" ")
    totalLifespan = 0
    totalEdges = 0
    for eIndex in range(3,len(l),3):
        totalLifespan += (int(l[eIndex+2]) - int(l[eIndex+1]))
        totalEdges += 1
    return (totalLifespan, totalEdges)

def addTuple(a, b):
    return (a[0]+b[0], a[1]+b[1])

def getVertexInterval(x):
    l = x.split(" ")
    return (int(l[1]), int(l[2]))

def minmax(a, b):
    return (min(a[0], b[0]), max(a[1], b[1]))

def integrityVerify(x):
    l = x.split(" ")
    verified = True
    vStart = int(l[1])
    vEnd = int(l[2])
    for eIndex in range(3, len(l), 3):
        eStart = int(l[eIndex+1])
        eEnd = int(l[eIndex+2])
        correct = (eStart >= vStart and eEnd <= vEnd and eStart < eEnd)
        if correct == False:
            verified = False
            break

    return (verified, int(l[0]))

def corrected(x):
    l = x.split(" ")
    vStart = int(l[1])
    vEnd = int(l[2])
    newline = l[0:3]
    for eIndex in range(3, len(l), 3):
        eStart = int(l[eIndex+1])
        eEnd = int(l[eIndex+2])
        if eEnd <= vStart or eStart >= vEnd or eEnd <= eStart:
            continue
        newline.append(l[eIndex])
        newline.append(str(max(eStart, vStart)))
        newline.append(str(min(eEnd, vEnd)))

    return " ".join(newline)

def getVertexId(x):
    l = x.split(" ")
    return ("id", int(l[0]))

def getOutVertexAddStat(x):
    l = x.split(" ")
    if len(l) > 3:
        return (int(l[1]), 1)
    return (int(l[1]), 0)

def getOutVertexDelStat(x):
    l = x.split(" ")
    if len(l) > 3:
        return (int(l[2]), 1)
    return (int(l[2]), 0)

def getVertexAddStat(x):
    l = x.split(" ")
    return (int(l[1]), 1)

def getVertexDelStat(x):
    l = x.split(" ")
    return (int(l[2]), 1)

def getEdgeAddStat(x):
    l = x.split(" ")
    localArray = dict()
    for eIndex in range(3,len(l),3):
        timepoint = int(l[eIndex+1])
        if timepoint not in localArray:
            localArray[timepoint] = 0
        localArray[timepoint] += 1

    return [(k, localArray[k]) for k in localArray.keys()]

def getEdgeDelStat(x):
    l = x.split(" ")
    localArray = dict()
    for eIndex in range(3,len(l),3):
        timepoint = int(l[eIndex+2])
        if timepoint not in localArray:
            localArray[timepoint] = 0
        localArray[timepoint] += 1

    return [(k, localArray[k]) for k in localArray.keys()]

def getTimepointEdgeStat(x):
    l = x.split(" ")
    localArray = dict()
    for t in range(int(l[1]), int(l[2])):
        localArray[t] = 0

    for eIndex in range(3,len(l),3):
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])

        t = (end - start)
        decr_step = 2. / (t + 1)
        weight = 2. * t / (t + 1)
        for step in range(start, end):
            localArray[step] += weight
            weight -= decr_step

    return [(k, localArray[k]) for k in localArray.keys()]


def getReverseTimepointEdgeStat(x):
    l = x.split(" ")
    localArray = dict()
    for t in range(int(l[1]), int(l[2])):
        localArray[t] = 0

    for eIndex in range(3,len(l),3):
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])

        t = (end - start)
        decr_step = 2. / (t + 1)
        weight = 2. * t / (t + 1)
        for step in range(start, end):
            localArray[(end-1)-(step-start)] += weight
            weight -= decr_step

    return [(k, localArray[k]) for k in localArray.keys()]

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GraphStats")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    '''
    snapshotVertices = lines.flatMap(lambda l: getSnapshotVertices(l)).reduceByKey(lambda a,b: a+b)
    snapshotEdges = lines.flatMap(lambda l: getSnapshotEdges(l)).reduceByKey(lambda a,b: a+b)
    snapshotVertices.saveAsTextFile('snapV')
    snapshotEdges.saveAsTextFile('snapE')
    '''

    ''' Choosing some source
    sources = lines.map(lambda l: getVertexStartDegree(l)).reduceByKey(lambda a,b: chooseDegreeStartSource(a,b))
    sources.saveAsTextFile("sources")
    '''

    #'''
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    #vertexAvgLs = lines.map(lambda l: getVertexLifespan(l)).reduce(lambda a,b: addTuple(a,b))
    #edgeAvgLs = lines.map(lambda l: getEdgeLifespan(l)).reduce(lambda a,b: addTuple(a,b))
    #graphLife = lines.map(lambda l: getVertexInterval(l)).reduce(lambda a,b: minmax(a,b))
    #correct = lines.map(lambda l: integrityVerify(l)).filter(lambda l: l[0] == False).groupByKey().mapValues(list)
    #maxId = lines.map(lambda l: getVertexId(l)).reduceByKey(lambda a,b: max(a,b))


    #print("Vertices: ",vertexAvgLs)
    #print("Edges: ", edgeAvgLs)
    #print("graph life: ", graphLife)
    #print("Id: ", maxId.collect())
    #print("correct: ", correct.collect())
    #'''
    
    #correctedGraph = lines.map(lambda l: corrected(l))
    #correctedGraph.saveAsTextFile(sys.argv[2])

    #'''
    insert_edge = lines.flatMap(lambda l: getEdgeAddStat(l)).reduceByKey(lambda a,b : a + b)
    delete_edge = lines.flatMap(lambda l: getEdgeDelStat(l)).reduceByKey(lambda a,b : a + b)
    insert_vertex = lines.map(lambda l: getVertexAddStat(l)).reduceByKey(lambda a,b : a + b)
    delete_vertex = lines.map(lambda l: getVertexDelStat(l)).reduceByKey(lambda a,b : a + b)
    insert_out_vertex = lines.map(lambda l: getOutVertexAddStat(l)).reduceByKey(lambda a,b : a + b)
    delete_out_vertex = lines.map(lambda l: getOutVertexDelStat(l)).reduceByKey(lambda a,b : a + b)
    timepoint_edge = lines.flatMap(lambda l: getTimepointEdgeStat(l)).reduceByKey(lambda a,b : a + b)
    reverse_timepoint_edge = lines.flatMap(lambda l: getReverseTimepointEdgeStat(l)).reduceByKey(lambda a,b : a + b)

    insert_edge_dict = insert_edge.collect()
    delete_edge_dict = delete_edge.collect()
    insert_vertex_dict = insert_vertex.collect()
    delete_vertex_dict = delete_vertex.collect()
    insert_out_vertex_dict = insert_out_vertex.collect()
    delete_out_vertex_dict = delete_out_vertex.collect()
    timepoint_edge_dict = timepoint_edge.collect()
    reverse_timepoint_edge_dict = reverse_timepoint_edge.collect()

    start = 0
    end = 104
    insert_edge_array = [0]*(end+1)
    delete_edge_array = [0]*(end+1)
    insert_vertex_array = [0]*(end+1)
    delete_vertex_array = [0]*(end+1)
    insert_out_vertex_array = [0]*(end+1)
    delete_out_vertex_array = [0]*(end+1)
    timepoint_edge_array = [0]*end
    reverse_timepoint_edge_array = [0]*end
    
    for item in insert_edge_dict:
        insert_edge_array[item[0]] = item[1]
    for item in delete_edge_dict:
        delete_edge_array[item[0]] = item[1]
    for item in insert_vertex_dict:
        insert_vertex_array[item[0]] = item[1]
    for item in delete_vertex_dict:
        delete_vertex_array[item[0]] = item[1]
    for item in insert_out_vertex_dict:
        insert_out_vertex_array[item[0]] = item[1]
    for item in delete_out_vertex_dict:
        delete_out_vertex_array[item[0]] = item[1]
    for item in timepoint_edge_dict:
        timepoint_edge_array[item[0]] = item[1]
    for item in reverse_timepoint_edge_dict:
        reverse_timepoint_edge_array[item[0]] = item[1]    

    # print(insert_edge_array)
    # print(delete_edge_array)
    # print(insert_vertex_array)
    # print(delete_vertex_array)
    # print(insert_out_vertex_array)
    # print(delete_out_vertex_array)
    #'''

    spark.stop()

    #''' 
    f = open("/home/hadoop/graphiteOOC/build/LDBC-9_0-FB-104_new.bin", 'wb')
    pickle.dump(insert_edge_array, f)
    pickle.dump(delete_edge_array, f)
    pickle.dump(insert_vertex_array, f)
    pickle.dump(delete_vertex_array, f)
    pickle.dump(insert_out_vertex_array, f)
    pickle.dump(delete_out_vertex_array, f)
    pickle.dump(timepoint_edge_array, f)
    pickle.dump(reverse_timepoint_edge_array, f)
    f.close()
    #'''
