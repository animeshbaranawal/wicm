#!/bin/bash

lowerE=$1
upperE=$2
windowSize=$3
inputGraph=$4
outputDir=$5

# : <<'END'
### windowed
hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.Utilities.NoOp \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphite.io.formats.IntIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphiteOOC.OOC.WindowIntIntNullTextOutputFormat -op $outputDir"_windows" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca difference=$windowSize

#hdfs dfs -copyToLocal $outputDir"_windows" .
#hdfs dfs -rm -r $outputDir"_windows"
