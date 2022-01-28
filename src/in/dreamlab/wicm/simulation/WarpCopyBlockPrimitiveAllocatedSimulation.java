package in.dreamlab.wicm.simulation;

import com.google.common.collect.Iterables;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.warpOperation.IntMax;
import in.dreamlab.wicm.utils.LocalWritableMessageBuffer;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class WarpCopyBlockPrimitiveAllocatedSimulation {
    int upperBound;
    TreeRangeMap<Integer, Integer> changedStates;
    LocalWritableMessageBuffer<IntIntIntervalMessage> messageBuffer;

    ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf;
    WarpBlock warpBlock;

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

    public WarpCopyBlockPrimitiveAllocatedSimulation(ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf){
        this.conf = conf;
        warpBlock = new WarpBlock(new IntMax());
        changedStates = TreeRangeMap.create();
        messageBuffer = new LocalWritableMessageBuffer<>(1000, IntIntIntervalMessage.class);
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
            blockSize = Integer.min(1000,((int) Math.ceil(numMessages * 1. / numBlocks)));
        }
        //System.out.println(blockSize+" "+numBlocks+" "+numMessages);

        changedStates.clear();
        messageBuffer.reset();

        for(IntIntIntervalMessage m : messages) {
            messageBuffer.addMessage(m);

            if(messageBuffer.filledBufferSize() == blockSize) {
                System.out.println("-------\n"+blockSize+" "+messageBuffer.filledBufferSize());
                //warpBlock.warpBlock(localMessages, statePartitions, lifespan, 0, messageBlockPointer, changedStates);

                for(IntIntIntervalMessage localm : messageBuffer.getIterable()) {
                    System.out.println(localm.getValidity()+", "+localm.getPayload());
                }
                for(IntIntIntervalMessage localm : messageBuffer.getIterable()) {
                    System.out.println(localm.getValidity()+", "+localm.getPayload());
                }
                messageBuffer.reset();
            }
        }

        if(!messageBuffer.isEmpty()) {
            System.out.println("-------\n"+blockSize+" "+messageBuffer.filledBufferSize());
            //warpBlock.warpBlock(messageBlock, statePartitions, lifespan, messageBlockPointer, changedStates);

            for(IntIntIntervalMessage localm : messageBuffer.getIterable()) {
                System.out.println(localm.getValidity()+", "+localm.getPayload());
            }
            for(IntIntIntervalMessage localm : messageBuffer.getIterable()) {
                System.out.println(localm.getValidity()+", "+localm.getPayload());
            }
        }

        return changedStates;
    }
}
