#!/bin/bash

source=$1
perfFlag=$2
inputGraph=$3
outputDir=$4

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
echo "Starting ICM local unrolling job..."

hadoop jar ICMD-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm.UByteFAST \
--yarnjars ICMD-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.wicm.io.formats.UByteUByteNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.wicm.io.formats.UByteUByteFASTTextOutputFormat -op $outputDir"_debug" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.wicm.graphData.UByteUByteIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.wicm.graphData.UByteUByteIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.wicm.comm.messages.UByteStartSlimMessage \
-ca graphite.intervalClass=in.dreamlab.wicm.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.wicm.warpOperation.UByteMax \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

##### dump output
#hdfs dfs -copyToLocal $outputDir"_debug" .
hdfs dfs -rm -r $outputDir"_debug"

##### dump logs
appID=$(yarn app -list -appStates FINISHED,KILLED | grep "FAST" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "FAST_"$outputDir"_"$source"_debug.log"

##### sort output for efficient diff
#echo "Sorting debug output..."
#cat $outputDir"_debug"/part* >> $outputDir"_debug"/output.txt
#rm $outputDir"_debug"/part*
#sort -k1 -n < $outputDir"_debug"/output.txt > $outputDir"_debug"/sorted.txt
#rm $outputDir"_debug"/output.txt
