#!/bin/bash

### MAG GC options
# -ca graphite.configureJavaOpts=true \
# -ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=10 -XX:G1NewSizePercent=1" \

mode=$1
if [[ "$mode" == "WICM" ]]; then
source=$2
lowerE=$3
upperE=$4
windows="$5"
memFlag=$6
perfFlag=$7
inputGraph=$8
outputDir=$9

#:<<'END'
echo "Restarting hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "hadoop restarted!"
sleep 40
#END
echo "Starting WICM job..."

### windowed
hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Window.TMST \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphiteOOC.io.formats.IntPairIntIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphiteOOC.io.formats.IntPairIntIdWithValueTextOutputFormat -op $outputDir"_windowed" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntPairIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntPairIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.TMSTOperator \
-ca giraph.masterComputeClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteIntCustomWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteDebugWindowWorkerContext \
-ca giraph.numComputeThreads=14 \
-ca sourceId=$source \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag

#hdfs dfs -copyToLocal $outputDir"_windowed" .
hdfs dfs -rm -r $outputDir"_windowed"

appID=$(yarn app -list -appStates FINISHED,KILLED | grep "TMST" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "TMST_"$outputDir"_"$source"_window.log"

#echo "Sorting windowed output..."
#cat $outputDir"_windowed"/part* >> $outputDir"_windowed"/output.txt
#rm $outputDir"_windowed"/part*
#sort -k1 -n < $outputDir"_windowed"/output.txt > $outputDir"_windowed"/sorted.txt
#rm $outputDir"_windowed"/output.txt

elif [[ "$mode" == "ICM" ]]; then
source=$2
memFlag=$3
perfFlag=$4
inputGraph=$5
outputDir=$6

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

### default
hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Debug.TMST \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphiteOOC.io.formats.IntPairIntIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphiteOOC.io.formats.IntPairIntIdWithValueTextOutputFormat -op $outputDir"_debug" -w 8 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntPairIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntPairIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.TMSTOperator \
-ca giraph.numComputeThreads=14 \
-ca sourceId=$source \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag

#hdfs dfs -copyToLocal $outputDir"_debug" .
hdfs dfs -rm -r $outputDir"_debug"

appID=$(yarn app -list -appStates FINISHED,KILLED | grep "TMST" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "TMST_"$outputDir"_"$source"_debug.log"

#echo "Sorting debug output..."
#cat $outputDir"_debug"/part* >> $outputDir"_debug"/output.txt
#rm $outputDir"_debug"/part*
#sort -k1 -n < $outputDir"_debug"/output.txt > $outputDir"_debug"/sorted.txt
#rm $outputDir"_debug"/output.txt

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
