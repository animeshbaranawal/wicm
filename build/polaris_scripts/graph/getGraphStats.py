import sys
import pickle
import numpy as np
import math
import os
from os import listdir
from os.path import isfile, join

lTimespan = int(sys.argv[1])
uTimespan = int(sys.argv[2])
timespan = uTimespan - lTimespan + 1
print(uTimespan, lTimespan, timespan)

insert_edges = np.zeros(timespan)
delete_edges = np.zeros(timespan)
insert_vertices = np.zeros(timespan)
delete_vertices = np.zeros(timespan)
insert_vertices_out = np.zeros(timespan)
delete_vertices_out = np.zeros(timespan)

f = open(sys.argv[3], 'r')
count = 0
for l in f:
	line = l[:-1].split(" ")
	#print(line)
	
	insert_vertices[int(line[1])-lTimespan] += 1
	delete_vertices[int(line[2])-lTimespan] += 1

	if(len(line) > 3):
		insert_vertices_out[int(line[1])-lTimespan] += 1
		delete_vertices_out[int(line[2])-lTimespan] += 1

	edge_i = 3
	while edge_i < len(line) :
		insert_edges[int(line[edge_i+1])-lTimespan] += 1
		delete_edges[int(line[edge_i+2])-lTimespan] += 1
		edge_i += 3

	count += 1
	if count % 1000000 == 0:
		print(count)
f.close()

print(insert_edges)
print(delete_edges)

print(insert_vertices)
print(delete_vertices)

print(insert_vertices_out)
print(delete_vertices_out)

f = open(sys.argv[4], 'wb')
pickle.dump(insert_edges, f)
pickle.dump(delete_edges, f)
pickle.dump(insert_vertices, f)
pickle.dump(delete_vertices, f)
pickle.dump(insert_vertices_out, f)
pickle.dump(delete_vertices_out, f)
f.close()
