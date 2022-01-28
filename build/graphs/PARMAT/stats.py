import math
import sys
import pickle
import time

import numpy as np

BASE_VERTICES = 29611
BASE_EDGES = 1000000
VERTEX_SIZE = 25
EDGE_SIZE = 12
MSG_SIZE = 16
MB = 1000*1000
ALPHA = 8.2
ETA = 4.6


def calculateTimeBoundaries(nWindows, edges):
    totalEdges = np.sum(edges)
    maxTimepoint = len(edges)
    approxEdges = totalEdges / nWindows
    # print(totalEdges, approxEdges)

    ## naive
    boundaries = [0]
    computedWindows = 0
    timepoint = 0
    graphEdges = 0

    while (computedWindows < nWindows) and (timepoint < maxTimepoint):
        if graphEdges > ((computedWindows + 1) * approxEdges):
            # print(timepoint, graphEdges, (computedWindows+1)*approxEdges)
            boundaries.append(timepoint)
            computedWindows += 1
            continue

        graphEdges += edges[timepoint]
        timepoint += 1

    # print(timepoint)
    boundaries.append(timepoint)
    boundaries_array = [(boundaries[i], boundaries[i + 1]) for i in range(len(boundaries) - 1)]
    boundaries_array = [x for x in boundaries_array if x[0] != x[1]]

    boundaries_output = [x[0] for x in boundaries_array]
    boundaries_output.append(boundaries_array[-1][1])
    return boundaries_output


def get_vertex_memory(num_vertices):
    base_vertex_size = (BASE_VERTICES * VERTEX_SIZE)/MB
    base_vertex_size = (base_vertex_size//10 + 1)*10
    memory = base_vertex_size * (num_vertices / BASE_VERTICES) * ALPHA
    memory = (memory//10 + 1)*10
    return memory


def get_edge_memory(num_edges):
    base_edge_size = (BASE_EDGES * EDGE_SIZE) / MB
    base_edge_size = (base_edge_size // 10 + 1) * 10
    memory = base_edge_size * (num_edges/BASE_EDGES)
    memory = (memory//10 + 1)*10
    return memory


def get_msg_memory(num_edges):
    base_msg_memory = (BASE_EDGES * MSG_SIZE) / MB
    base_msg_memory = (base_msg_memory//10 + 1) * 10
    memory = base_msg_memory * (num_edges/BASE_EDGES) * ETA
    memory = (memory//10 + 1)*10
    return memory


def get_breakpoint(edge_add, edge_delete, vertex_add, vertex_delete, target_memory, start, old_graph_memory, f=None):
    if old_graph_memory > target_memory:
        return -1

    if start == 48:
        if f is not None:
            target_memory = f*target_memory

    memory = (0, 0)
    topology = (0, 0)
    lifespan = len(edge_add)
    end = start+1
    # end = start-1

    while end < (lifespan-1):
    # while end >= 0:
        v = sum(vertex_add[:end]) - sum(vertex_delete[:(start+1)])
        e = sum(edge_add[:end]) - sum(edge_delete[:(start+1)])
        # v = sum(vertex_add[:start]) - sum(vertex_delete[:(end + 1)])
        # e = sum(edge_add[:start]) - sum(edge_delete[:(end + 1)])

        graph_memory = get_vertex_memory(v) + get_edge_memory(e)
        required_memory = graph_memory + get_msg_memory(e)
        total_memory = old_graph_memory + required_memory
        # print(end-1, v, e, graph_memory, required_memory, target_memory)
        if total_memory > target_memory:
            if (end-1) == start:
            # if (end + 1) == start:
                return -1, (0, 0)
            else:
                # print(topology)
                return end - 1, memory
                # return end+1, memory
        memory = (graph_memory, required_memory, old_graph_memory)
        topology = (v, e)
        end += 1
        # end -= 1

    # print(topology)
    return end, memory
    # return end+1, memory


f = open(sys.argv[1], 'rb')
insert_edges = pickle.load(f)
delete_edges = pickle.load(f)
insert_vertices = pickle.load(f)
delete_vertices = pickle.load(f)
insert_vertices_out = pickle.load(f)
delete_vertices_out = pickle.load(f)

total_vertices = sum(insert_vertices)
total_edges = sum(insert_edges)
print(sum(insert_vertices), sum(insert_edges))
# print(get_vertex_memory(total_vertices), get_edge_memory(total_edges), get_msg_memory(total_edges))
# for i in range(10, 1000):
#     ETA = i/10
#     graph_memory = get_vertex_memory(total_vertices) + get_edge_memory(total_edges)
#     msg_memory = get_msg_memory(total_edges)
#     if (graph_memory + msg_memory) > 122880:
#         break
#     print(ETA, graph_memory+msg_memory)

old = 0
peak_memory = 0
# estimate memory
strategy1 = [0,6,12,18,24,30,36,42,48]
# strategy2 = [0,175,196,216,229,238,365]
total_v = 0
total_e = 0

strategy = strategy1
for i in range(1, len(strategy)):
    j = i # len(strategy)-i
    v = sum(insert_vertices[:strategy[j]]) - sum(delete_vertices[:(strategy[j-1]+1)])
    e = sum(insert_edges[:strategy[j]]) - sum(delete_edges[:(strategy[j-1]+1)])
    total_v += v
    total_e += e
    # graph_memory = get_vertex_memory(v) + get_edge_memory(e)
    # msg_memory = get_msg_memory(e)
    # print(j, old, graph_memory, msg_memory, old+graph_memory+msg_memory)
    # peak_memory = max(peak_memory, graph_memory+msg_memory+old)
    # old = graph_memory
# print(peak_memory)
print(total_v, total_e)

# start = 0
# target = 65000
# while start < 365 and start != -1:
#     e, m = get_breakpoint(insert_edges, delete_edges, insert_vertices, delete_vertices, target, start, old)
#     old = m[0]
#     print(e,m)
#     start = e

#Reddit
# 5GB - 0;79;85;91;95;122 - 6400
# 2.5GB - 0;70;75;79;82;84;86;87;88;89;90;91;92;93;94;95;96;97;98;122 - 3300

#LDBC
# 0;20;40;60;80;100;120;140;160;180;200;220;240;260;280;300;320;340;360;365 - 9700/10000/9300/12000
# 0;175;196;216;229;238;365 - 8800/9600/8500/14400
# 0;167;186;204;215;223;229;233;236;239;241;242;243;244;365 - 8200/8900/8000/13200
# 0;52;117;148;168;183;194;203;210;216;221;222;365 - 8800

#Webuk - EAT: 50000, SSSP,TR: 46000 , LD: 60000
# 0;6;12;18;24;30;36;42;48 - 38000/36000/36000/37700
# 0;4;8;12;16;20;24;28;32;36;40;44;48 - 28400/27000/27000/32200