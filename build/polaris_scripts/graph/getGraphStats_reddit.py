import sys
import re

vertices = 0
edges = 0
with open(sys.argv[1], 'r') as graph:
	for line in graph:
		l = line[:-1]
		data = [int(e) for e in re.split('[ \t]', l)]
		vertexID = data[0]
		vertexStart = data[1]
		vertexEnd = data[2]
		vertices += 1
		
		adjacencyList = data[3:]
		i = 0
		while i < len(adjacencyList):
			targetID = adjacencyList[i]
			edgeStart = adjacencyList[i+1]
			edgeEnd = adjacencyList[i+2]
			edges += 1
			i = i + 3

print("Vertices: ",vertices)
print("Edges: ",edges)