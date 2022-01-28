#!/bin/bash

##HDFS_DIR="text"
#HDFS_DST_DIR="sampleGraphMutations"
##hdfs dfs -mkdir -p $HDFS_DST_DIR
##hdfs dfs -getmerge $HDFS_DIR/key=1-* window-1.txt
##hdfs dfs -copyFromLocal window-1.txt $HDFS_DST_DIR/window-1.txt
##rm -r window-1.txt
##echo "Processed window 1..."
#
#for window in $(seq 2 3)
#do
#  for worker in $(seq 0 0)
#  do
#    #hdfs dfs -getmerge $HDFS_DIR/key="$window"-"$worker" window-"$window"-"$worker"
#    #hdfs dfs -copyFromLocal window-"$window"-"$worker" $HDFS_DST_DIR/window-"$window"-"$worker"
#    #rm -r window-"$window"-"$worker"
#    #echo "Processed window ""$window""-""$worker""..."
#    hdfs dfs -mv $HDFS_DST_DIR/window-"$window"/part-0000"$worker" $HDFS_DST_DIR/window-"$window"-"$worker"
#  done
#  hdfs dfs -rm -r $HDFS_DST_DIR/window-"$window"
#done
#
##hdfs dfs -rm -r $HDFS_DIR

HDFS_DST_DIR="output"
lworkers=0 #0->7, x->x+7
uworkers=0
workers=0
lower=2
upper=40

hdfs dfs -mkdir -p $HDFS_DST_DIR/window-1
for worker in $(seq -f "%05g" $lworkers $uworkers)
do
  hdfs dfs -mv $HDFS_DST_DIR/part-"$worker" $HDFS_DST_DIR/window-1/part-"$worker"
done

counter=1
for window in $(seq $lower $upper)
do
  for worker in $(seq 0 $workers)
#  for worker in $(seq $workers -1 0)
  do
    counterVal=$(printf "%05d" $counter)
    hdfs dfs -mv $HDFS_DST_DIR/part-"$counterVal" $HDFS_DST_DIR/window-"$window"-"$worker"
    counter=$((counter+1))
#    counter=$((counter-1))
  done
done
