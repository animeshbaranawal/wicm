import sys
import os
from os import listdir
from os.path import isfile, join

from enum import Enum
from datetime import datetime
import math
import pickle
import operator

inputFile = sys.argv[1]
outputFile = sys.argv[2]

output_fh = open(outputFile, 'w')
with open(inputFile) as textfile1:
	count = 0
	for l in textfile1:
		line = l.strip().split(",")
		#print(line)
			
		edgeSet = set()
		edgeList1 = [s for s in line[3].split("|") if s != ""]
		edgeList2 = [s for s in line[4].split("|") if s != ""]
		#print(edgeList1, edgeList2)
		for i in range(0,len(edgeList1),3):
			edgeSet.add(edgeList1[i]+" "+edgeList1[i+1]+" "+edgeList1[i+2])
		for i in range(0,len(edgeList2),3):
			edgeSet.add(edgeList2[i]+" "+edgeList2[i+1]+" "+edgeList2[i+2])

		output_fh.write("{0} {1} {2} {3}\n".format(line[0],line[1],line[2]," ".join(edgeSet)))

		if count % 100000 == 0:
			print(count)
		count += 1

output_fh.close()

