import sys
import re
import json 

vertices = 0
edges = 0
properties = 0
with open(sys.argv[1], 'r') as graph:
	for line in graph:
		l = line[:-1]
		data = json.loads(l)
		
		vertexID = data[0]
		vertexStart = data[1]
		vertexEnd = data[2]
		vertices += 1
		
		adjacencyList = data[3]
		for targetEdgeWithProperties in adjacencyList:
			targetID = targetEdgeWithProperties[0]
			edgeStart = targetEdgeWithProperties[1]
			edgeEnd = targetEdgeWithProperties[2]
			edges += 1
			
			eProperties = targetEdgeWithProperties[3]
			i = 0
			while i < len(eProperties):
				propertyStart = eProperties[i]
				propertyEnd = eProperties[i+1]
				propertyValue = eProperties[i+2]
				properties += 1
				i = i + 3

print("Vertices: ",vertices)
print("Edges: ",edges)
print("Properties: ",properties)