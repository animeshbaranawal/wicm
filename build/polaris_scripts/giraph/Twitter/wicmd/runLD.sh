#!/bin/bash

source=$1
bufferSize=$2
minMsg=$3
blockWarp=$4
inputGraph=$5
outputDir=$6
lowerE=$7
upperE=$8
windows="$9"

##### restart hadoop
#:<<'END'
echo "Restarting Hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "Hadoop restarted!"
sleep 40
#END
echo "Starting WICM local unrolling job..."

hadoop jar WICMD-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.wicm.UByteLD \
--yarnjars WICMD-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.wicm.io.formats.UByteIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.wicm.io.formats.UByteIntIdWithValueTextOutputFormat -op $outputDir"_windowed" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.wicm.graphData.UByteIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.wicm.graphData.UByteIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.wicm.comm.messages.UByteIntEndSlimMessage \
-ca graphite.intervalClass=in.dreamlab.wicm.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMax \
-ca giraph.masterComputeClass=in.dreamlab.wicm.graph.computation.GraphiteIntReverseCustomWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.wicm.graph.computation.GraphiteDebugWindowWorkerContext \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca wicm.localBufferSize="$bufferSize" \
-ca wicm.minMessages="$minMsg" \
-ca icm.blockWarp=$blockWarp \
-ca sourceId=$source \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca debugPerformance=true

##### dump output
#hdfs dfs -copyToLocal $outputDir"_windowed" .
hdfs dfs -rm -r $outputDir"_windowed"

##### dump logs
appID=$(yarn app -list -appStates FINISHED,KILLED | grep "LD" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "LD_"$outputDir"_"$source"_window.log"

##### sort output for efficient diff
#echo "Sorting debug output..."
#cat $outputDir"_windowed"/part* >> $outputDir"_windowed"/output.txt
#rm $outputDir"_windowed"/part*
#sort -k1 -n < $outputDir"_windowed"/output.txt > $outputDir"_windowed"/sorted.txt
#rm $outputDir"_windowed"/output.txt
