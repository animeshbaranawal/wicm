package in.dreamlab.wicm.simulation;

import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.warpOperation.IntMax;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;

public class WarpSimulation {
    ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf;
    private WarpBlock warpBlock;
    TreeRangeMap<Integer, Integer> changedStates;

    public void init(){
        warpBlock.init();
    }

    public String dump(){
        StringBuilder sb = new StringBuilder();
        sb.append(warpBlock.warpTime).append(",").append(warpBlock.bucketingTime).append(",").append(warpBlock.replicationTime);
        sb.append(",").append(warpBlock.duplications).append(",").append(warpBlock.messageCount);
        sb.append(",").append(warpBlock.postWarpTime).append(",").append(warpBlock.computeCalls);
        return sb.toString();
    }

    public WarpSimulation(ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf) {
        this.conf = conf;
        warpBlock = new WarpBlock(new IntMax());
        warpBlock.init();
        changedStates = TreeRangeMap.create();
    }

    public TreeRangeMap<Integer, Integer> run(Iterable<IntIntIntervalMessage> messages,
                      TreeRangeMap<Integer, Integer> statePartitions,
                      IntInterval lifespan) {
        int blockStart = 0;
        int blockEnd = Integer.MAX_VALUE;

        changedStates.clear();
        warpBlock.warpBlock(messages, statePartitions, lifespan, blockStart, blockEnd, changedStates);

        return changedStates;
    }
}
