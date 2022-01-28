package in.dreamlab.wicm.simulation;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.warpOperation.IntMax;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class WarpCopyBlockPrimitiveSimulation {
    UnsafeByteArrayOutputStream dOut;
    UnsafeByteArrayInputStream dIn;

    int upperBound, messageBlockPointer;
    TreeRangeMap<Integer, Integer> changedStates;
    IntIntIntervalMessage[] messageBlock;

    ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf;
    WarpBlockPrimitive warpBlock;

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

    public WarpCopyBlockPrimitiveSimulation(ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf){
        this.conf = conf;
        warpBlock = new WarpBlockPrimitive(new IntMax());
        changedStates = TreeRangeMap.create();
        dOut = new UnsafeByteArrayOutputStream();
        dIn = new UnsafeByteArrayInputStream(dOut.getByteArray(), 0, dOut.getPos());
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public TreeRangeMap<Integer, Integer> run(Iterable<IntIntIntervalMessage> messages,
                                              TreeRangeMap<Integer, Integer> statePartitions,
                                              IntInterval lifespan) throws IOException {
        int numMessages = Iterables.size(messages);
        int numBlocks = 1, blockSize = numMessages;
        if (numMessages >= 10) {
            numBlocks = Integer.min(((int) Math.floor(Math.sqrt(numMessages))), upperBound);
            blockSize = ((int) Math.ceil(numMessages * 1. / numBlocks));
        }
        //System.out.println(blockSize+" "+numBlocks+" "+numMessages);

        messageBlock = new IntIntIntervalMessage[blockSize];
        messageBlockPointer = 0;
        changedStates.clear();
        dOut.reset();

        for(IntIntIntervalMessage m : messages) {
            m.write(dOut);
            dIn.setBuffer(dOut.getByteArray(), 0, dOut.getPos());
            IntIntIntervalMessage messageClone = ReflectionUtils.newInstance(IntIntIntervalMessage.class);
            messageClone.readFields(dIn);
            messageBlock[messageBlockPointer++] = messageClone;
            dOut.reset();

            if(messageBlockPointer == blockSize) {
                warpBlock.warpBlock(messageBlock, statePartitions, lifespan, messageBlockPointer, changedStates);
                messageBlockPointer = 0;
            }
        }

        if(messageBlockPointer > 0) {
            //System.out.println(blockSize+" "+counter);
            warpBlock.warpBlock(messageBlock, statePartitions, lifespan, messageBlockPointer, changedStates);
        }

        return changedStates;
    }
}
