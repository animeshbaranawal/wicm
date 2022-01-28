import sys
import re
import json 

lowerEndpoint = int(sys.argv[1])
upperEndpoint = int(sys.argv[2])
difference = int(sys.argv[3])
windowNum = int(sys.argv[4])

windowStart = lowerEndpoint + windowNum*difference
windowEnd = lowerEndpoint + (windowNum+1)*difference

vertices = 0
edges = 0
properties = 0
fw = open(sys.argv[6], 'w')
with open(sys.argv[5], 'r') as graph:
	for line in graph:
		l = line[:-1]
		data = json.loads(l)
		# print(json_l)

		vertexID = data[0]
		vertexStart = data[1]
		vertexEnd = data[2]

		if vertexStart >= windowEnd or vertexEnd <= windowStart:
			continue

		Wdata = []
		vertexWID = (windowNum << 27 | vertexID)
		vertexWStart = max(vertexStart, windowStart)
		vertexWEnd = vertexEnd #min(vertexEnd, windowEnd)
		vertices += 1
		# print(vertexWID, vertexWStart, vertexWEnd)
		Wdata.extend((vertexWID, vertexWStart, vertexWEnd))
		
		adjacencyList = data[3]
		# print(adjacencyList)
		WAdjacencyList = []
		for targetEdgeWithProperties in adjacencyList:
			targetID = targetEdgeWithProperties[0]
			edgeStart = targetEdgeWithProperties[1]
			edgeEnd = targetEdgeWithProperties[2]

			if edgeStart >= windowEnd or edgeEnd <= windowStart:
				i = i + 1
				continue

			targetWEdgeWithProperties = []
			targetWID = (windowNum << 27 | targetID)
			edgeWStart = max(edgeStart, windowStart)
			edgeWEnd = min(edgeEnd, windowEnd)
			edges += 1
			targetWEdgeWithProperties.extend((targetWID, edgeWStart, edgeWEnd))

			eProperties = targetEdgeWithProperties[3]
			WeProperties = []
			i = 0
			while i < len(eProperties):
				propertyStart = eProperties[i]
				propertyEnd = eProperties[i+1]
				propertyValue = eProperties[i+2]

				if propertyStart >= windowEnd or propertyEnd <= windowStart:
					i = i + 3
					continue

				propertyWStart = max(propertyStart, windowStart)
				propertyWEnd = min(propertyEnd, windowEnd)
				properties += 1
				WeProperties.extend((propertyWStart, propertyWEnd, propertyValue))
				i = i + 3
			targetWEdgeWithProperties.append(WeProperties)
			WAdjacencyList.append(targetWEdgeWithProperties)
		
		Wdata.append(WAdjacencyList)
		json.dump(Wdata, fw, separators=(',', ':'))
		fw.write("\n")
fw.close()

print("Vertices: ",vertices)
print("Edges: ",edges)
print("Properties: ",properties)