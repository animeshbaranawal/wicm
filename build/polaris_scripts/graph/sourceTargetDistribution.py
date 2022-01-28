import sys
import numpy as np
import pickle

inputFile = sys.argv[1]
start = int(sys.argv[2])
end = int(sys.argv[3])
output = sys.argv[4]

startSet = dict()
handler = open(inputFile, 'r')
count = 0
for l in handler:
	line = l.strip().split()
	startSet[int(line[0])] = int(line[1])
	count += 1
	if count % 10000 == 0:
		print(count)
handler.close()

distribution = np.zeros((end,end)).astype(int)
handler = open(inputFile, 'r')
count = 0
for l in handler:
	line = l.strip().split()
	sourceStart = int(line[1])

	for i in range(3,len(line),3):
		targetStart = startSet[int(line[i])]
		if targetStart > sourceStart:
			print(line)
			print(sourceStart, targetStart)
			handler.close()
			exit()
		
		distribution[int(line[1])][startSet[int(line[i])]] += 1

	count += 1
	if count % 10000 == 0:
		print(count)
handler.close()

pickle.dump(distribution, open(output, 'wb'))
print(distribution)
