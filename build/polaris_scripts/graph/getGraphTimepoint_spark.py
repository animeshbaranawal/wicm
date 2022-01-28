import pickle
import sys
from pyspark.sql import SparkSession


def getTimepointEdgeStat(x):
    l = x.split(" ")
    localArray = dict()
    localArrayReverse = dict()
    for t in range(int(l[1]), int(l[2])+1):
        localArray[t] = 0
        localArrayReverse[t] = 0

    for eIndex in range(3, len(l), 3):
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])

        localArray[start] += 1
	localArrayReverse[end] += 1
	#t = (end - start)
        #decr_step = 2. / (t + 1)
        #weight = 2. * t / (t + 1)
        #for step in range(start, end):
        #    localArray[step] += weight
        #    localArrayReverse[(end - 1) - (step - start)] += weight
        #    weight -= decr_step

    result = [(('F', k), localArray[k]) for k in localArray.keys()]
    result.extend([(('R', k), localArrayReverse[k]) for k in localArrayReverse.keys()])
    return result


def getVertexList(x):
    l = x.split(" ")
    return (int(l[0]), (int(l[1]), int(l[2])))


def get_vertex_cumulative_timepoint_edges(x):
    l = x.split(" ")
    vStart = int(l[1])
    vEnd = int(l[2])
    vLife = vEnd - vStart

    insert_array = [0] * (vLife + 1)
    delete_array = [0] * (vLife + 1)
    for eIndex in range(3, len(l), 3):
        eStart = int(l[eIndex + 1])
        eEnd = int(l[eIndex + 2])
        insert_array[eStart - vStart] += 1
        delete_array[eEnd - vStart] += 1

    timepoint_edges = [0] * vLife
    cum_sum = 0
    for i in range(vLife):
        cum_sum += insert_array[i]
        cum_sum -= delete_array[i]
        timepoint_edges[i] = cum_sum

    forward_sum = [0] * vLife
    #backward_sum = [0] * vLife
    #backward_sum[0] = timepoint_edges[0]
    forward_sum[vLife - 1] = timepoint_edges[vLife - 1]
    for i in range(1, vLife):
        #backward_sum[i] = timepoint_edges[i] + backward_sum[i - 1]
        forward_sum[vLife - 1 - i] = timepoint_edges[vLife - 1 - i] + forward_sum[vLife - i]

    for i in range(vLife):
        forward_sum[i] += 1
        #backward_sum[i] += 1

    return (int(l[0]), (vStart, forward_sum)) #, backward_sum))


def getEdgeList(x):
    l = x.split(" ")
    edges = []
    for eIndex in range(3, len(l), 3):
        dest = int(l[eIndex])
        start = int(l[eIndex + 1])
        end = int(l[eIndex + 2])
        edges.append((dest, (start, end)))

    return edges


def getTimepointEdgeStatv2(x):
    localArray = dict()
    localArrayReverse = dict()
    vStart = x[1][1][0]
    vEnd = x[1][1][1]
    for t in range(vStart, vEnd):
        localArray[t] = 0
        localArrayReverse[t] = 0

    for eIndex in x[1][0]:
        eStart = eIndex[0]
        eEnd = eIndex[1]
        T = vEnd - eStart
        l = eEnd - eStart

        w = (2. * T) / (2. * T - l + 1)
        d = w / T
        for step in range(eStart, eEnd):
            localArray[step] += w
            localArrayReverse[(eEnd - 1) - (step - eStart)] += w
            w -= d

    result = [(('F', k), localArray[k]) for k in localArray.keys()]
    result.extend([(('R', k), localArrayReverse[k]) for k in localArrayReverse.keys()])
    return result


def getTimepointEdgeStatv3(x):
    localArray = dict()
    #localArrayReverse = dict()

    vStart = x[1][1][0]
    forward_sum = x[1][1][1]
    #backward_sum = x[1][1][2]

    for t in range(vStart, vStart + len(forward_sum)):
        localArray[t] = 0
        #localArrayReverse[t] = 0

    forward_sum.append(1)
    #backward_sum.insert(0, 1)

    for eIndex in x[1][0]:
        eStart = eIndex[0]
        eEnd = eIndex[1]
        l = eEnd - eStart

        T_forward = sum(forward_sum[(eStart - vStart + 1):(eEnd - vStart + 1)])
        #T_backward = sum(backward_sum[(eStart - vStart):(eEnd - vStart)])
        for step in range(eStart, eEnd):
            localArray[step] += (forward_sum[step - vStart + 1] * 1. * l) / T_forward
            #localArrayReverse[step] += (backward_sum[step - vStart] * 1. * l) / T_backward

    result = [(('F', k), localArray[k]) for k in localArray.keys()]
    #result.extend([(('R', k), localArrayReverse[k]) for k in localArrayReverse.keys()])
    return result


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GraphTimepoint") \
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    timepoint_edge = lines.flatMap(lambda l: getTimepointEdgeStat(l)).reduceByKey(lambda a,b : a + b)

    #vList = lines.map(lambda x: getVertexList(x))
    #vList_cumulative = lines.map(lambda x: get_vertex_cumulative_timepoint_edges(x))

    #eList = lines.flatMap(lambda x: getEdgeList(x)).groupByKey()
    #c = eList.count()

    #veList = eList.join(vList)
    #veList_cumulative = eList.join(vList_cumulative)

    #cc = veList.count()
    #cc_cumulative = veList_cumulative.count()
    #assert c == cc
    #assert c == cc_cumulative

    #timepoint_edge_v2 = veList.flatMap(lambda l: getTimepointEdgeStatv2(l)).reduceByKey(lambda a, b: a + b)
    #timepoint_edge_v3 = veList_cumulative.flatMap(lambda l: getTimepointEdgeStatv3(l)).reduceByKey(lambda a, b: a + b)

    timepoint_edge_dict = timepoint_edge.collect()
    #timepoint_edge_dict_v2 = timepoint_edge_v2.collect()
    #timepoint_edge_dict_v3 = timepoint_edge_v3.collect()

    start = 0
    end = 30
    timepoint_edge_array = [0]*(end+1)
    reverse_timepoint_edge_array = [0]*(end+1)
    #timepoint_edge_array_v2 = [0] * end
    #reverse_timepoint_edge_array_v2 = [0] * end
    #timepoint_edge_array_v3 = [0] * end
    #reverse_timepoint_edge_array_v3 = [0] * end

    
    for item in timepoint_edge_dict:
        if item[0][0] == 'F':
            timepoint_edge_array[item[0][1]] = item[1]
        elif item[0][0] == 'R':
            reverse_timepoint_edge_array[item[0][1]] = item[1]
    '''
    for item in timepoint_edge_dict_v2:
        if item[0][0] == 'F':
            timepoint_edge_array_v2[item[0][1]] = item[1]
        elif item[0][0] == 'R':
            reverse_timepoint_edge_array_v2[item[0][1]] = item[1]
    
    for item in timepoint_edge_dict_v3:
        if item[0][0] == 'F':
            timepoint_edge_array_v3[item[0][1]] = item[1]
        #elif item[0][0] == 'R':
        #    reverse_timepoint_edge_array_v3[item[0][1]] = item[1]
    '''
    print(timepoint_edge_array, sum(timepoint_edge_array))
    print(reverse_timepoint_edge_array, sum(reverse_timepoint_edge_array))
    # print(timepoint_edge_array_v2, sum(timepoint_edge_array_v2))
    # print(reverse_timepoint_edge_array_v2, sum(reverse_timepoint_edge_array_v2))
    # print(timepoint_edge_array_v3, sum(timepoint_edge_array_v3))
    # print(reverse_timepoint_edge_array_v3, sum(reverse_timepoint_edge_array_v3))

    spark.stop()

    #f = open(sys.argv[2], 'wb')
    #pickle.dump(timepoint_edge_array, f)
    #pickle.dump(reverse_timepoint_edge_array, f)
    #pickle.dump(timepoint_edge_array_v2, f)
    #pickle.dump(reverse_timepoint_edge_array_v2, f)
    #pickle.dump(timepoint_edge_array_v3, f)
    #pickle.dump(reverse_timepoint_edge_array_v3, f)
    #f.close()

