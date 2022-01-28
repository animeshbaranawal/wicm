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
inputReverseFile = sys.argv[2]
outputFile = sys.argv[3]

output_fh = open(outputFile, 'w')
with open(inputFile) as textfile1, open(inputReverseFile) as textfile2: 
	count = 0
	startLine = 0
	for x, y in zip(textfile1, textfile2):
		if count < startLine:
			if count % 1000000 == 0:
				print(count)
			count += 1
			continue		

		x = x.strip().split()
		y = y.strip().split()
		assert x[0] == y[0]

		output_fh.write("{0},{1},{2},{3},{4}\n".format(x[0],x[1],x[2],"|".join(x[3:]),"|".join(y[3:])))
		
		if count % 100000 == 0:
			print(count)
		count += 1

output_fh.close()

