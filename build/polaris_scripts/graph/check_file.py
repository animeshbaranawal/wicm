import sys

f = open(sys.argv[1], 'r')
count = 0
for l in f:
	line = l.strip().split(",")
	'''
	id = line[0]
	targets = len(line[3::3])
	uniquetargets = len(set(line[3::3]))
	if targets != uniquetargets:
		print(line)
		f.close()
		exit()
	'''

	if len(line) < 5:
		print(line)
		f.close()
		exit()

	if count % 1000000 == 0:
		print(count)
	count += 1
f.close()

