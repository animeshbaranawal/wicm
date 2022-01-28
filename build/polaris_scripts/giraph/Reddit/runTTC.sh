#!/bin/bash

mode=$1

############################################################ Window ICM ############################################################
if [[ "$mode" == "WICM" ]]; then
lowerE=$2
upperE=$3
windows="$4"
memFlag=$5
perfFlag=$6
inputGraph=$7
outputDir=$8

##### restart hadoop
#:<<'END'
echo "Restarting hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "Hadoop restarted!"
sleep 40
#END
echo "Starting WICM job..."

##### without shedding
#:<<'END'
hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Window.TemporalTC \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphiteOOC.io.formats.UByteCustomIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphiteOOC.io.formats.UByteCustomIntIdWithValueTextOutputFormat -op $outputDir"_windowed" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.CustomUByteIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.UByteUByteIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.UBytePairIntIntStartSlimMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.PairIntIntMapAppend \
-ca giraph.masterComputeClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteIntCustomWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteDebugWindowWorkerContext \
-ca giraph.numComputeThreads=14 \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag
#END

##### dump output
hdfs dfs -copyToLocal $outputDir"_windowed" .
hdfs dfs -rm -r $outputDir"_windowed"

##### dump logs
appID=$(yarn app -list -appStates FINISHED,KILLED | grep "TemporalTC" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "TTC_"$outputDir"_"$source"_window.log"

##### sort output for efficient diff
echo "Sorting windowed output..."
cat $outputDir"_windowed"/part* >> $outputDir"_windowed"/output.txt
rm $outputDir"_windowed"/part*
sort -k1 -n < $outputDir"_windowed"/output.txt > $outputDir"_windowed"/sorted.txt
rm $outputDir"_windowed"/output.txt

############################################################ Default ICM ############################################################
elif [[ "$mode" == "ICM" ]]; then
memFlag=$2
perfFlag=$3
inputGraph=$4
outputDir=$5

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
echo "Starting ICM job..."

hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Debug.TC \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphite.io.formats.IntIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.IntIntIdWithValueTextOutputFormat -op $outputDir"_debug" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntArrayAppend \
-ca giraph.messageEncodeAndStoreType=EXTRACT_BYTEARRAY_PER_PARTITION \
-ca giraph.useMessageSizeEncoding=true \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag

##### dump output
hdfs dfs -copyToLocal $outputDir"_debug" .
hdfs dfs -rm -r $outputDir"_debug"

##### dump logs
appID=$(yarn app -list -appStates FINISHED,KILLED | grep "TC" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "TC_"$outputDir"_debug.log"

##### sort output for efficient diff
echo "Sorting debug output..."
cat $outputDir"_debug"/part* >> $outputDir"_debug"/output.txt
rm $outputDir"_debug"/part*
sort -k1 -n < $outputDir"_debug"/output.txt > $outputDir"_debug"/sorted.txt
rm $outputDir"_debug"/output.txt

elif [[ "$mode" == "compare" ]]; then
ICMD="$2"
WICMD="$3"
diff "$ICMD"/sorted.txt "$WICMD"/sorted.txt > diff.txt

if [ -s diff.txt ]; then
	rm diff.txt
	echo "Not equivalent"
	exit 1
else
	rm diff.txt
	echo "Equivalent"
	exit 0
fi

fi
