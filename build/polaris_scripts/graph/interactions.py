import sys
import numpy as np
import pickle

### generate interactions
graphFile = sys.argv[1]
graphStart = int(sys.argv[2])
graphEnd = int(sys.argv[3])
output = sys.argv[4]

interactions = np.zeros((graphEnd,graphEnd)).astype(int)

count = 0
graph_fh = open(graphFile, 'r')
for l in graph_fh:
	if count % 10000 == 0:
		print(count)
	count += 1

	line = l.strip().split()
	# print(line)

	vEnd = int(line[2])
	# print(vEnd)
	for eIndex in range(3,len(line),3):
		eStart = int(line[eIndex+1])
		eEnd = int(line[eIndex+2])
		# print(eStart, eEnd)

		for interactionIndex in range(eStart,eEnd):
			destinationTimepoint = interactionIndex + 1
			if destinationTimepoint < vEnd:
				interactions[interactionIndex][destinationTimepoint:vEnd] += 1
graph_fh.close()

pickle.dump(interactions, open(output, "wb" ) )
print(interactions)

'''
### read interactions
inputFile = sys.argv[1]
interactions = pickle.load(open(inputFile, 'rb'))
print(interactions)
'''
