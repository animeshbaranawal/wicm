package in.dreamlab.wicm.simulation;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.warpOperation.IntMax;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;

public class WarpBlockSimulation {
    ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf;
    private WarpBlock warpBlock;
    TreeRangeMap<Integer, Integer> changedStates;
    int upperBound;

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

    public WarpBlockSimulation(ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf) {
        this.conf = conf;
        warpBlock = new WarpBlock(new IntMax());
        warpBlock.init();
        changedStates = TreeRangeMap.create();
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public TreeRangeMap<Integer, Integer> run(Iterable<IntIntIntervalMessage> messages,
                                                            TreeRangeMap<Integer, Integer> statePartitions,
                                                            IntInterval lifespan) {
        int numMessages = Iterables.size(messages);
        int numBlocks = 1, blockSize = numMessages, blockStart = 0;
        if (numMessages >= 10) {
            numBlocks = Integer.min(((int) Math.floor(Math.sqrt(numMessages))), upperBound);
            blockSize = ((int) Math.ceil(numMessages * 1. / numBlocks));
        }
        //System.out.println(blockSize+" "+numBlocks+" "+numMessages);
        changedStates.clear();

        while(blockStart < numMessages) {
            //System.out.println(blockSize+" "+blockStart);
            warpBlock.warpBlock(messages, statePartitions, lifespan, blockStart, blockStart+blockSize, changedStates);
            blockStart += blockSize;
        }

        return changedStates;
    }
}
