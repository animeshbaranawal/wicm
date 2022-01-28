#!/bin/bash

source=$1
inputGraph=$2
outputDir=$3
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
org.apache.giraph.GiraphRunner in.dreamlab.wicm.algorithms.wicm.UByteFAST \
--yarnjars WICM-1.0-SNAPSHOT-jar-with-dependencies.jar \
--yarnheap 3000 \
-vif in.dreamlab.wicm.io.mutations.formats.UByteFASTTextInputFormat -vip $inputGraph \
-vof in.dreamlab.wicm.io.formats.UByteUByteFASTTextOutputFormat -op $outputDir -w 1 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.wicm.graphData.UByteUByteIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.wicm.graphData.UByteUByteIntervalData \
-ca giraph.inputOutEdgesClass=in.dreamlab.wicm.edge.ByteArrayEdgesClearable \
-ca giraph.outgoingMessageValueClass=in.dreamlab.wicm.comm.messages.UByteStartSlimMessage \
-ca graphite.intervalClass=in.dreamlab.wicm.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.wicm.warpOperation.UByteMax \
-ca giraph.masterComputeClass=in.dreamlab.wicm.graph.mutations.WICMMutationsWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.wicm.graph.mutations.WICMMutationsWorkerContext \
-ca giraph.vertexResolverClass=in.dreamlab.wicm.graph.mutations.resolver.UByteFASTVertexResolver \
-ca wicm.mutationReaderClass=in.dreamlab.wicm.io.mutations.UByteFASTMutationFileReader \
-ca giraph.numComputeThreads=3 \
-ca sourceId=$source \
-ca lastSnapshot=$last \
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