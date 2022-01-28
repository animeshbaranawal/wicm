package in.dreamlab.wicm.simulation;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.warpOperation.IntMax;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class WarpCopyBlockSimulation {
    DataInputOutput messageBlock;
    Iterable<IntIntIntervalMessage> messageBlockIterator;
    int upperBound;

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

    public WarpCopyBlockSimulation(ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf){
        this.conf = conf;

        messageBlock = this.conf.createMessagesInputOutput();
        MessageClasses<IntWritable, IntIntIntervalMessage> messageClasses = this.conf.getOutgoingMessageClasses();
        MessageValueFactory<IntIntIntervalMessage> messageValueFactory = messageClasses.createMessageValueFactory(this.conf);
        messageBlockIterator = new MessagesIterable<>(messageBlock, messageValueFactory);

        warpBlock = new WarpBlock(new IntMax());
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

        ((ExtendedDataOutput) messageBlock.getDataOutput()).reset();
        changedStates.clear();

        for(IntIntIntervalMessage m : messages) {
            try {
                m.write(messageBlock.getDataOutput());
                blockStart++;
            } catch (IOException e) {
                throw new RuntimeException("Caught exception when writing message");
            }

            if(blockStart == blockSize) {
                //System.out.println(blockSize+" "+counter);
                warpBlock.warpBlock(messageBlockIterator, statePartitions, lifespan, 0, blockStart, changedStates);
                blockStart = 0;
                ((ExtendedDataOutput) messageBlock.getDataOutput()).reset();
            }
        }

        if(blockStart > 0) {
            //System.out.println(blockSize+" "+counter);
            warpBlock.warpBlock(messageBlockIterator, statePartitions, lifespan, 0, blockStart, changedStates);
            ((ExtendedDataOutput) messageBlock.getDataOutput()).reset();
        }

        return changedStates;
    }
}
