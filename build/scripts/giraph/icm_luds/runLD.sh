#!/bin/bash

source=$1
bufferSize=$2
minMsg=$3
perfFlag=$4
inputGraph=$5
outputDir=$6
blockWarp="true" # to disable local warp unrolling, set to false

##### restart hadoop
:<<'END'
echo "Restarting YARN..."
$HADOOP_HOME/sbin/stop-yarn.sh
sleep 10
$HADOOP_HOME/sbin/start-yarn.sh
sleep 10
echo "YARN restarted!"
sleep 40
END
<<<<<<< HEAD:build/scripts/block/runLD.sh
echo "Starting ICM local unrolling job..."
=======
echo "Starting LU+DS job..."
>>>>>>> main:build/scripts/giraph/icm_luds/runLD.sh

hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm_luds.LD \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 3000 \
-vif in.dreamlab.graphite.io.formats.IntIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntIntIdWithValueTextOutputFormat -op $outputDir -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMax \
-ca icm.blockWarp=$blockWarp \
-ca wicm.localBufferSize="$bufferSize" \
-ca wicm.minMessages="$minMsg" \
-ca giraph.numComputeThreads=1 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

##### dump output
hdfs dfs -copyToLocal $outputDir .
hdfs dfs -rm -r $outputDir

##### dump logs
# appID=$(yarn app -list -appStates FINISHED,KILLED | grep "LD" | sort -k1 -n | tail -n 1 | awk '{print $1}')
# echo $appID
# yarn logs -applicationId $appID > "LD_"$outputDir"_"$source"_debug.log"

##### sort output for efficient diff
echo "Sorting debug output..."
cat $outputDir/part* >> $outputDir/output.txt
rm $outputDir/part*
sort -k1 -n < $outputDir/output.txt > $outputDir/sorted.txt
rm $outputDir/output.txt