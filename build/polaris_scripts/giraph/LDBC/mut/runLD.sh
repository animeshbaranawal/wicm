#!/bin/bash

source=$1
inputGraph=$2
outputDir=$3
lowerE=$4
upperE=$5
windows="$6"
mPath="$7"
bufferSize=$8

##### restart hadoop - cold start
:<<'END'
echo "Restarting Hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "Hadoop restarted!"
sleep 40
END
echo "Starting ICM Local unrolling job..."

hadoop jar WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.wicm.LD \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 200 \
-vif in.dreamlab.wicm.io.mutations.formats.LDTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntIntIdWithValueTextOutputFormat -op $outputDir -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMax \
-ca giraph.masterComputeClass=in.dreamlab.wicm.graph.mutations.WICMMutationsReverseWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.wicm.graph.mutations.WICMMutationsWorkerContext \
-ca giraph.vertexResolverClass=in.dreamlab.wicm.graph.mutations.resolver.LDVertexResolver \
-ca wicm.mutationReaderClass=in.dreamlab.wicm.io.mutations.LDMutationFileReader \
-ca giraph.numComputeThreads=3 \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca sourceId=$source \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca wicm.localBufferSize="$bufferSize" \
-ca wicm.minMessages=20 \
-ca icm.blockWarp=true \
-ca wicm.mutationPath="$mPath" \
-ca wicm.resolverPath="$outputDir" \
-ca debugPerformance=true

##### dump output
hdfs dfs -copyToLocal $outputDir .
hdfs dfs -rm -r $outputDir

##### dump logs
#appID=$(yarn app -list -appStates FINISHED,KILLED | grep "EAT" | sort -k1 -n | tail -n 1 | awk '{print $1}')
#echo $appID
#yarn logs -applicationId $appID > "EAT_"$outputDir"_"$source"_debug.log"

##### sort output for efficient diff
echo "Sorting debug output..."
cat $outputDir/part* >> $outputDir/output.txt
rm $outputDir/part*
cat $outputDir/dump* >> $outputDir/output.txt
rm $outputDir/dump*
sort -k1 -n < $outputDir/output.txt > $outputDir/sorted.txt
rm $outputDir/output.txt
sed -i '1d' $outputDir/sorted.txt