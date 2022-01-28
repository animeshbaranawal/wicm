import sys
import re

lowerEndpoint = int(sys.argv[1])
upperEndpoint = int(sys.argv[2])
difference = int(sys.argv[3])
windowNum = int(sys.argv[4])

windowStart = lowerEndpoint + windowNum*difference
windowEnd = lowerEndpoint + (windowNum+1)*difference

vertices = 0
edges = 0
fw = open(sys.argv[6], 'w')
with open(sys.argv[5], 'r') as graph:
	for line in graph:
		l = line[:-1]
		data = [int(e) for e in re.split('[ \t]', l)]
		vertexID = data[0]
		vertexStart = data[1]
		vertexEnd = data[2]

		if vertexStart >= windowEnd or vertexEnd <= windowStart:
			continue

		vertexWID = (windowNum << 27 | vertexID)
		vertexWStart = max(vertexStart, windowStart)
		vertexWEnd = vertexEnd #min(vertexEnd, windowEnd)
		vertices += 1
		# print(vertexWID, vertexWStart, vertexWEnd)
		
		adjacencyList = data[3:]
		# print(adjacencyList)
		WAdjacencyList = []
		i = 0
		while i < len(adjacencyList):
			targetID = adjacencyList[i]
			edgeStart = adjacencyList[i+1]
			edgeEnd = adjacencyList[i+2]

			if edgeStart >= windowEnd or edgeEnd <= windowStart:
				i = i + 3
				continue

			targetWID = (windowNum << 27 | targetID)
			edgeWStart = max(edgeStart, windowStart)
			edgeWEnd = min(edgeEnd, windowEnd)
			edges += 1
			WAdjacencyList.extend((str(targetWID), str(edgeWStart), str(edgeWEnd)))
			i = i + 3
		
		fw.write(str(vertexWID)+" "+str(vertexWStart)+" "+str(vertexWEnd))
		if WAdjacencyList:
			fw.write(" ")
			fw.write(" ".join(WAdjacencyList))
		fw.write("\n")
fw.close()

print("Vertices: ",vertices)
print("Edges: ",edges)