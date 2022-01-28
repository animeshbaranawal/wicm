import sys
import os
from os import listdir
from os.path import isfile, join
import numpy as np

from enum import Enum
from datetime import datetime
import math
import pickle
import itertools
import operator

def int_to_bytes(x: int) -> bytes:
    return x.to_bytes((x.bit_length() + 7) // 8, 'big')
    
def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, 'big')

inputFile = sys.argv[1]
outputFileBasename = sys.argv[2]

maxId = 133633039
width = 15000000

for iteration in range(5000000, maxId+1, width):
	nodeMin = iteration
	nodeMax = iteration + width
	print("Iteration ", iteration, nodeMin, nodeMax)
	print(outputFileBasename+"-"+str(nodeMin)+".txt")	
	
	reverseAdjList = dict()
	sourceLifetime = dict()

	f = open(inputFile, 'r')
	count = 0
	for l in f:
		line = l.strip().split()

		source = int(line[0])
		if source >= nodeMin and source < nodeMax:
			sourceStart = np.byte(int(line[1]))
			sourceEnd = np.byte(int(line[2]))
			sourceLifetime[source] = (sourceStart,sourceEnd)
		
		for i in range(3,len(line),3):
			dest = int(line[i])
			if dest >= nodeMin and dest < nodeMax:
				destStart = np.byte(int(line[i+1]))
				destEnd = np.byte(int(line[i+2]))

				if dest not in reverseAdjList:
					reverseAdjList[dest] = []
				reverseAdjList[dest].append((source,destStart,destEnd))
		
		count += 1
		if count % 10000 == 0:
			print(count)
	f.close()

	f = open(outputFileBasename+"-"+str(iteration)+".txt", 'w')
	count = 0
	for k in sourceLifetime:
		life = sourceLifetime[k]
		f.write(str(k)+" "+str(life[0])+" "+str(life[1]))

		adjList = []
		if k in reverseAdjList:
			adjList = reverseAdjList[k]
		for e in adjList:
			f.write(" "+str(e[0])+" "+str(e[1])+" "+str(e[2]))
		f.write("\n")

		count += 1
		if count % 10000 == 0:
			print(count)
	f.close()
	
