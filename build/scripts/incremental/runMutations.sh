#!/bin/bash

HDFS_DST_DIR="sampleGraphReverseMutations"
lworkers=2
uworkers=2 # workers - 1
workers=0 # workers - 1
lower=2
upper=3 # number of windows

hdfs dfs -mkdir -p $HDFS_DST_DIR/window-1
for worker in $(seq -f "%05g" $lworkers $uworkers)
do
  hdfs dfs -mv $HDFS_DST_DIR/part-"$worker" $HDFS_DST_DIR/window-1/part-"$worker"
done

counter=1
for window in $(seq $lower $upper)
do
#  for worker in $(seq 0 $workers)
  for worker in $(seq $workers -1 0)
  do
    counterVal=$(printf "%05d" $counter)
    hdfs dfs -mv $HDFS_DST_DIR/part-"$counterVal" $HDFS_DST_DIR/window-"$window"-"$worker"
#    counter=$((counter+1))
    counter=$((counter-1))
  done
done
