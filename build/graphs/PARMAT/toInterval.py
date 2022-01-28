import sys
import random

random.seed(0)

edges = dict()
lifespan = 40

fileHandler = open(sys.argv[1], 'r')
for line in fileHandler:
    l = line.strip().split("\t")
    source = int(l[0])
    destination = int(l[1])
    start = random.randint(0, lifespan-1)
    end = random.randint(start+1, lifespan)
    edges[(source, destination)] = (start, end)
fileHandler.close()

vertices = dict()
adjacencyList = dict()
for e in edges:
    source = e[0]
    destination = e[1]

    for v in [source, destination]:
        if v not in vertices:
            vertices[v] = [41, -1]
            adjacencyList[v] = []

        vertices[v][0] = min(vertices[v][0], edges[e][0])
        vertices[v][1] = max(vertices[v][1], edges[e][1])

    adjacencyList[source].append(destination)

fileHandler = open(sys.argv[2], 'w')
for v in vertices:
    line = str(v) + " " + str(vertices[v][0]) + " " + str(vertices[v][1])
    for e in adjacencyList[v]:
        edgeLife = edges[(v, e)]
        line = line + " " + str(e) + " " + str(edgeLife[0]) + " " + str(edgeLife[1])
    fileHandler.write(line+'\n')
fileHandler.close()

