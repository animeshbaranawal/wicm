#!/bin/bash

source=$1
perfFlag=$2
inputGraph=$3
outputDir=$4

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

### default
hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.icm.UByteREACH_D \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.wicm.io.formats.UByteBooleanNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.wicm.io.formats.UByteBooleanIdWithValueTextOutputFormat -op $outputDir -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.wicm.graphData.UByteBooleanIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.wicm.graphData.UByteBooleanIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.wicm.comm.messages.UByteBooleanStartSlimMessage \
-ca graphite.intervalClass=in.dreamlab.wicm.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.BooleanOr \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca sourceId=$source \
-ca debugPerformance=$perfFlag

#hdfs dfs -copyToLocal $outputDir .
hdfs dfs -rm -r $outputDir

appID=$(yarn app -list -appStates FINISHED,KILLED | grep "REACH_D" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "TR_"$outputDir"_"$source"_debug.log"

#echo "Sorting windowed output..."
#cat $outputDir/part* >> $outputDir/output.txt
#rm $outputDir/part*
#sort -k1 -n < $outputDir/output.txt > $outputDir/sorted.txt
#rm $outputDir/output.txt
