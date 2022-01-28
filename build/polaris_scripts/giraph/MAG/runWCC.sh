#!/bin/bash

#### Input format difference
### reddit: in.dreamlab.graphite.io.formats.IntIntNullTextInputFormat

#-sourceId = 4650975 ## largest out degree source
# Different source: 9211182 3876428 2244438 8718214 4390156

### the same script will work for gplus.txt
#-ca sourceId=286938 ## largest out degree source for gplus.txt
# Different source: 12030605 7673718 14643076 19525363 3674897

### the same script will work for MAG.txt (without any props)
# Different source: 36451628 67119113 20179997 46036203 8598657

mode=$1
if [[ "$mode" == "WICM" ]]; then
lowerE=$2
upperE=$3
windows="$4"
memFlag=$5
perfFlag=$6
inputGraph=$7
outputDir=$8

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
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Window.WCCByte \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphite.io.formats.UByteIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.UByteIntIdWithValueTextOutputFormat -op $outputDir"_windowed" -w 10 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.UByteIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.UByteIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.UByteIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
-ca giraph.masterComputeClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteIntCustomWindowMaster \
-ca giraph.workerContextClass=in.dreamlab.graphiteOOC.graph.computation.GraphiteDebugWindowWorkerContext \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=3 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca lowerEndpoint=$lowerE \
-ca upperEndpoint=$upperE \
-ca windows="$windows" \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag

#hdfs dfs -copyToLocal $outputDir"_windowed" .
hdfs dfs -rm -r $outputDir"_windowed"

appID=$(yarn app -list -appStates FINISHED,KILLED | grep "WCCByte" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "WCCByte_"$outputDir"_window.log"

#echo "Sorting windowed output..."
#cat $outputDir"_windowed"/part* >> $outputDir"_windowed"/output.txt
#rm $outputDir"_windowed"/part*
#sort -k1 -n < $outputDir"_windowed"/output.txt > $outputDir"_windowed"/sorted.txt
#rm $outputDir"_windowed"/output.txt

elif [[ "$mode" == "ICM" ]]; then
memFlag=$2
perfFlag=$3
inputGraph=$4
outputDir=$5

#:<<'END'
echo "Restarting hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "hadoop restarted!"
sleep 40
#END
echo "Starting ICM job..."

### default
#-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=15 -XX:G1HeapWastePercent=1 -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=17 -XX:G1NewSizePercent=1 -XX:G1MixedGCLiveThresholdPercent=99 -XX:MaxGCPauseMillis=60000"

hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.Reddit.Debug.WCCByte \
--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
--yarnheap 60000 \
-vif in.dreamlab.graphite.io.formats.UByteIntNullTextInputFormat -vip $inputGraph \
-vof in.dreamlab.graphite.io.formats.UByteIntIdWithValueTextOutputFormat -op $outputDir"_debug" -w 10 \
-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.UByteIntIntervalData \
-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.UByteIntIntervalData \
-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.UByteIntIntervalMessage \
-ca graphite.intervalClass=in.dreamlab.graphite.types.UByteInterval \
-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
-ca graphite.configureJavaOpts=true \
-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=5 -XX:G1NewSizePercent=1" \
-ca giraph.numComputeThreads=14 \
-ca debugMemory=$memFlag \
-ca debugPerformance=$perfFlag

hdfs dfs -copyToLocal $outputDir"_debug" .
hdfs dfs -rm -r $outputDir"_debug"

appID=$(yarn app -list -appStates FINISHED,KILLED | grep "WCCByte" | sort -k1 -n | tail -n 1 | awk '{print $1}')
echo $appID
yarn logs -applicationId $appID > "WCCByte_"$outputDir"_debug.log"

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

elif [[ "$mode" == "MWICM" ]]; then
set -euxo pipefail
lowerE=$2
upperE=$3
windows="$4"
memFlag=$5
perfFlag=$6
inputGraph=$7
outputDir=$8
localInputGraph="$9"

:<<'END'
echo "Restarting hadoop..."
$HADOOP_HOME/sbin/stop-all.sh
sleep 10
$HADOOP_HOME/sbin/start-all.sh
sleep 10
echo "hadoop restarted!"
sleep 40
END
echo "Starting MWICM job..."

IFS=';' read -r -a windowArray <<< "$windows"
windowArraySize=${#windowArray[@]}
oldw=""
for (( c=0; c<(( $windowArraySize-1 )); c++ ))
do	
	#:<<'END'
	echo "Restarting hadoop..."
	$HADOOP_HOME/sbin/stop-all.sh
	sleep 10
	$HADOOP_HOME/sbin/start-all.sh
	sleep 10
	echo "hadoop restarted!"
	sleep 40
	#END
	
	w="${windowArray[$c]};${windowArray[$c+1]}"
	echo $w

	if (( c == 0 )); then ## processing the first partition
		hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
		org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.MemoryAnalysis.WCCHalt \
		--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
		--yarnheap 60000 \
		-vif in.dreamlab.graphiteOOC.io.formats.MemoryAnalysis.IntIntNullTextInputFormat -vip $inputGraph \
		-vof in.dreamlab.graphiteOOC.io.formats.MemoryAnalysis.IntIntIdWithValueTextOutputFormat -op $outputDir"_partition_"$w -w 10 \
		-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
		-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
		-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
		-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
		-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
		-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
		-ca giraph.masterComputeClass=in.dreamlab.graphiteOOC.graph.computation.MemoryAnalysis.GraphiteIntPartitionMaster \
		-ca giraph.workerContextClass=in.dreamlab.graphiteOOC.graph.computation.MemoryAnalysis.GraphiteDebugPartitionWorkerContext \
		-ca giraph.numComputeThreads=14 \
		-ca graphite.configureJavaOpts=true \
		-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=10 -XX:G1NewSizePercent=1" \
		-ca init=true \
		-ca lifespan="$lowerE;$upperE" \
		-ca partition="$w" \
		-ca debugMemory=$memFlag \
		-ca debugPerformance=$perfFlag

	else ## for other partitions
		hadoop jar giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
		org.apache.giraph.GiraphRunner in.dreamlab.graphiteOOC.algorithms.MemoryAnalysis.WCCHalt \
		--yarnjars giraph-1.3.0-SNAPSHOT-for-hadoop-3.1.1-jar-with-dependencies.jar \
		--yarnheap 60000 \
		-vif in.dreamlab.graphiteOOC.io.formats.MemoryAnalysis.IntIntWithValueNullTextInputFormat -vip $oldw"_"$inputGraph \
		-vof in.dreamlab.graphiteOOC.io.formats.MemoryAnalysis.IntIntIdWithValueTextOutputFormat -op $outputDir"_partition_"$w -w 10 \
		-ca giraph.vertexClass=in.dreamlab.graphite.graph.DefaultIntervalVertex \
		-ca giraph.vertexValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
		-ca giraph.edgeValueClass=in.dreamlab.graphite.graphData.IntIntIntervalData \
		-ca giraph.outgoingMessageValueClass=in.dreamlab.graphite.comm.messages.IntIntIntervalMessage \
		-ca graphite.intervalClass=in.dreamlab.graphite.types.IntInterval \
		-ca graphite.warpOperationClass=in.dreamlab.graphite.warpOperation.IntMin \
		-ca giraph.masterComputeClass=in.dreamlab.graphiteOOC.graph.computation.MemoryAnalysis.GraphiteIntPartitionMaster \
		-ca giraph.workerContextClass=in.dreamlab.graphiteOOC.graph.computation.MemoryAnalysis.GraphiteDebugPartitionWorkerContext \
		-ca giraph.numComputeThreads=14 \
		-ca graphite.configureJavaOpts=true \
		-ca graphite.worker.java.opts="-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:G1MaxNewSizePercent=10 -XX:G1NewSizePercent=1" \
		-ca init=false \
		-ca lifespan="$lowerE;$upperE" \
		-ca partition="$w" \
		-ca debugMemory=$memFlag \
		-ca debugPerformance=$perfFlag

		hdfs dfs -rm $oldw"_"$inputGraph
	fi

	hdfs dfs -copyToLocal $outputDir"_partition_"$w .
	hdfs dfs -rm -r $outputDir"_partition_"$w

	appID=$(yarn app -list -appStates FINISHED,KILLED | grep "WCC" | sort -k1 -n | tail -n 1 | awk '{print $1}')
	echo $appID
	yarn logs -applicationId $appID > "WCC_"$outputDir"_partition_"$w".log"

	outPath=$outputDir"_partition_"$w
	echo $outPath
	cat $outPath/part* >> $outPath/output.txt
	rm $outPath/part*
	sort -k1 -n < $outPath/output.txt > $outPath/sorted.txt
	rm $outPath/output.txt

	echo "Creating graph for next partition..."
	paste $outputDir"_partition_"$w/sorted.txt $localInputGraph > tmp
	echo "Uploading to hadoop..."
	hdfs dfs -copyFromLocal tmp $w"_"$inputGraph
	rm tmp
	rm -r $outPath

	oldw="$w"
done

hdfs dfs -rm "*;*"

fi

