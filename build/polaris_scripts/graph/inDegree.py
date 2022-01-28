import sys
import numpy as np
import pickle

inputFile = sys.argv[1]
start = int(sys.argv[2])
end = int(sys.argv[3])

distribution = np.zeros((end,1001)).astype(int)
localDistribution = np.zeros(end).astype(int)

handler = open(inputFile, 'r')
count = 0
for l in handler:
	line = l.strip().split()

	localDistribution[:] = 0
	for i in range(3,len(line),3):
		localDistribution[int(line[i+1]):int(line[i+2])] += 1

	for t, value in enumerate(localDistribution):
		if value >= 10000:
			valueIndex = 1000
		elif value >= 1000:
			valueIndex = 550 + ((value - 1000)//20)
		elif value >= 100:
			valueIndex = 100 + ((value - 100)//2)
		else:
			valueIndex = value

		distribution[t][valueIndex] += 1
	
	count += 1
	if count % 10000 == 0:
		print(count)
handler.close()

pickle.dump(distribution, open("dist.p", 'wb'))
print(distribution)
