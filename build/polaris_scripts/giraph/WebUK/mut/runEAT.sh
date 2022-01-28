#!/bin/bash

source=$1
inputGraph="$2"
outputDir="$3"
lowerE=$4
upperE=$5
windows="$6"
mPath="$7"
last=$8
bufferSize=$9

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
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.wicm.UByteEAT \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 200 \
-vif in.dreamlab.wicm.io.mutations.formats.UByteEATTextInputFormat -vip $inputGraph \
-vof in.dreamlab.wicm.io.formats.UByteIntIdWithValueTextOutputFormat -op $outputDir"_mut" -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.wicm.graphData.UByteIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.wicm.graphData.UByteIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.wicm.comm.messages.UByteIntStartSlimMessage \
-ca graphite.intervalClass=in.dreamlab.wicm.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
-ca giraph.masterComputeClass=in.dreamlab.wicm.graph.mutations.WICMMutationsWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.wicm.graph.mutations.WICMMutationsWorkerContext \
-ca giraph.vertexResolverClass=in.dreamlab.wicm.graph.mutations.resolver.UByteEATVertexResolver \
-ca wicm.mutationReaderClass=in.dreamlab.wicm.io.mutations.UByteEATMutationFileReader \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=3 \
-ca sourceId="$source" \
-ca lastSnapshot=$last \
-ca lowerEndpoint="$lowerE" \
-ca upperEndpoint="$upperE" \
-ca windows="$windows" \
-ca wicm.localBufferSize="$bufferSize" \
-ca wicm.minMessages=20 \
-ca icm.blockWarp=true \
-ca wicm.mutationPath="$mPath" \
-ca wicm.resolverPath=$outputDir"_mut" \
-ca debugPerformance=true

##### dump output
hdfs dfs -copyToLocal "$outputDir""_mut" .
hdfs dfs -rm -r "$outputDir""_mut"

##### dump logs
#appID=$(yarn app -list -appStates FINISHED,KILLED | grep "EAT" | sort -k1 -n | tail -n 1 | awk '{print $1}')
#echo $appID
#yarn logs -applicationId $appID > "EAT_"$outputDir"_"$source"_mut.log"

##### sort output for efficient diff
echo "Sorting debug output..."
cat $outputDir"_mut"/part* >> $outputDir"_mut"/output.txt
rm $outputDir"_mut"/part*
cat $outputDir"_mut"/dump* >> $outputDir"_mut"/output.txt
rm $outputDir"_mut"/dump*
sort -k1 -n < $outputDir"_mut"/output.txt > $outputDir"_mut"/sorted.txt
rm $outputDir"_mut"/output.txt
sed -i '1d' $outputDir"_mut"/sorted.txt