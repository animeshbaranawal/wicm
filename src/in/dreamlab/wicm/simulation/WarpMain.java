package in.dreamlab.wicm.simulation;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.*;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class WarpMain {
    public static void main(String[] args) throws IOException {
        GiraphConfiguration giraphConf = new GiraphConfiguration(new Configuration());
        giraphConf.set("giraph.outgoingMessageValueClass","in.dreamlab.graphite.comm.messages.IntIntIntervalMessage");
        ImmutableClassesGiraphConfiguration<IntWritable, IntIntIntervalData, IntIntIntervalData> conf =
                new ImmutableClassesGiraphConfiguration<>(giraphConf);

        DataInputOutput messageBlock = conf.createMessagesInputOutput();
        MessageClasses<IntWritable, IntIntIntervalMessage> messageClasses = conf.getOutgoingMessageClasses();
        MessageValueFactory<IntIntIntervalMessage> messageValueFactory = messageClasses.createMessageValueFactory(conf);
        Iterable<IntIntIntervalMessage> messages = new MessagesIterable<>(messageBlock, messageValueFactory);

        UnsafeByteArrayOutputStream dOut = new UnsafeByteArrayOutputStream();
        UnsafeByteArrayInputStream dIn = new UnsafeByteArrayInputStream(dOut.getByteArray(), 0, dOut.getPos());

        Random rn = new Random();
        rn.setSeed(0L);
        // benchmarks serialisation and copying costs
        /*long loopTime = 0;
        long copyLoopTime = 0;
        long cloneLoopTime = 0;
        long readLoopTime1 = 0;
        long readLoopTime2 = 0;
        long totalMessages = 0;
        for(int s = 0; s < 3; s++) {
            List<IntIntIntervalMessage> messageList = new LinkedList<>();
            int numMessages = Integer.max(((int)(rn.nextGaussian()*10 + 10)),3);
            for(int i=0; i<numMessages; i++) {
                int start = rn.nextInt((13));
                int payload = rn.nextInt(100);
                IntIntIntervalMessage m = new IntIntIntervalMessage(new IntInterval(start, Integer.MAX_VALUE), payload);
                System.out.println(m.getValidity()+", "+m.getPayload());
                messageList.add(m);
            }

            int a = 0;
            long time = System.nanoTime();
            for(IntIntIntervalMessage ignored : messageList) {
                a++;
            }
            loopTime += (System.nanoTime() - time);

            ((ExtendedDataOutput) messageBlock.getDataOutput()).reset();
            a = 0;
            time = System.nanoTime();
            for(IntIntIntervalMessage m : messageList) {
                a++;
                m.write(messageBlock.getDataOutput());
            }
            copyLoopTime += (System.nanoTime() - time);
            totalMessages += numMessages;

            a = 0;
            time = System.nanoTime();
            for(IntIntIntervalMessage ignored : messages) {
                a++;
            }
            readLoopTime1 += (System.nanoTime() - time);

            time = System.nanoTime();
            a = 0;
            IntIntIntervalMessage[] messageClones = new IntIntIntervalMessage[numMessages];
            for(IntIntIntervalMessage m : messageList) {
                // using giraph utils
                m.write(dOut);
                dIn.setBuffer(dOut.getByteArray(), 0, dOut.getPos());
                IntIntIntervalMessage messageClone = ReflectionUtils.newInstance(IntIntIntervalMessage.class);
                messageClone.readFields(dIn);
                messageClones[a++] = messageClone;
                dOut.reset();

                // using hadoop utils
                // messageClones[a++] = WritableUtils.clone(m, conf);
            }
            cloneLoopTime += (System.nanoTime() - time);

            a = 0;
            time = System.nanoTime();
            for(IntIntIntervalMessage ignored : messageClones) {
                a++;
                System.out.println(ignored.getValidity()+", "+ignored.getPayload());
            }
            readLoopTime2 += (System.nanoTime() - time);
        }
        //System.out.println(loopTime+", "+copyLoopTime+", "+readLoopTime1+", "
        //        +cloneLoopTime+", "+readLoopTime2+", "+totalMessages);*/

        // simulations
        //WarpSimulation originalWarp = new WarpSimulation(conf);
        //WarpCopyBlockSimulation copyBlockWarp = new WarpCopyBlockSimulation(conf);
        //WarpCopyBlockPrimitiveSimulation copyBlockPrimitiveWarp = new WarpCopyBlockPrimitiveSimulation(conf);
        WarpCopyBlockPrimitiveAllocatedSimulation copyBlockAllocatedWarp = new WarpCopyBlockPrimitiveAllocatedSimulation(conf);
        long time;

        int maximum = 12;
        int minimum = 0;
        IntInterval lifespan = new IntInterval(0,10);

        for(int s = 0; s < 3; s++){
            // generate random messages
            int numMessages = Integer.max(((int)(rn.nextGaussian()*10 + 10)),3);
            ((ExtendedDataOutput) messageBlock.getDataOutput()).reset();
            for (int i=0; i<numMessages; i++) {
                int start = rn.nextInt((maximum-minimum+1)) + minimum;
                int payload = rn.nextInt(100);
                IntIntIntervalMessage m = new IntIntIntervalMessage(new IntInterval(start, Integer.MAX_VALUE), payload);
                System.out.println(m.getValidity()+", "+m.getPayload());
                try {
                    m.write(messageBlock.getDataOutput());
                } catch (IOException e) {
                    throw new RuntimeException("Exception writing message");
                }
            }
            TreeRangeMap<Integer, Integer> vertexState = TreeRangeMap.create();
            StringBuilder sb = new StringBuilder();

            // Original Warp Simulation
            vertexState.clear();
            vertexState.put(Range.closedOpen(0,10), 0);
            copyBlockAllocatedWarp.init();
            copyBlockAllocatedWarp.setUpperBound(Integer.MAX_VALUE);
            time = System.nanoTime();
            TreeRangeMap<Integer, Integer> collectedStates = copyBlockAllocatedWarp.run(messages, vertexState, lifespan);
            time = System.nanoTime() - time;
            //System.out.println(vertexState.toString()+", "+collectedStates.toString());
            //sb.append(time).append(",").append(copyBlockAllocatedWarp.dump());

            /*vertexState.clear();
            vertexState.put(Range.closedOpen(0,10), 0);
            copyBlockAllocatedWarp.init();
            copyBlockAllocatedWarp.setUpperBound(Integer.MAX_VALUE);
            time = System.nanoTime();
            TreeRangeMap<Integer, Integer> collectedStates = copyBlockAllocatedWarp.run(messages, vertexState, lifespan);
            time = System.nanoTime() - time;
            //System.out.println(vertexState.toString()+", "+collectedStates.toString());
            sb.append(time).append(",").append(copyBlockAllocatedWarp.dump()).append(",");

            vertexState.clear();
            vertexState.put(Range.closedOpen(0,10), 0);
            copyBlockAllocatedWarp.init();
            copyBlockAllocatedWarp.setUpperBound(((int) Math.ceil(Math.sqrt(lifespan.getLength()))));
            time = System.nanoTime();
            collectedStates = copyBlockAllocatedWarp.run(messages, vertexState, lifespan);
            time = System.nanoTime() - time;
            //System.out.println(vertexState.toString()+", "+collectedStates.toString());
            sb.append(time).append(",").append(copyBlockAllocatedWarp.dump());*/

            System.out.println(sb.toString());
            //System.out.println("----------------------------------------------------------------");
        }
    }
}
