import sys

graphFile = sys.argv[1]

maxID = -1

count = 0
fh = open(graphFile, 'r')
for l in fh:
	line = l.strip().split()
	maxID = max(maxID, int(line[0]))
	maxID = max(maxID, int(line[1]))
	count += 1
	if count % 1000000 == 0:
		print(count)
fh.close()

print(maxID)
