package in.dreamlab.wicm.algorithms.wicm;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
<<<<<<< HEAD:src/in/dreamlab/wicm/graph/computation/DebugUByteWindowIntervalComputation.java
import in.dreamlab.wicm.graph.mutations.DebugWICMMutationsIntervalComputation;
=======
import in.dreamlab.wicm.graph.computation.DebugWindowIntervalComputation;
>>>>>>> main:src/in/dreamlab/wicm/algorithms/wicm/DebugUByteWindowIntervalComputation.java
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

<<<<<<< HEAD:src/in/dreamlab/wicm/graph/computation/DebugUByteWindowIntervalComputation.java
public abstract class DebugUByteWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<UnsignedByte, S>, EP, E extends IntervalData<UnsignedByte, EP>, PW, P, IM extends IntervalMessage<UnsignedByte, P>> extends DebugWICMMutationsIntervalComputation<I, UnsignedByte, S, V, EP, E, PW, P, IM> {
=======
/**
 * Time domain is UnsignedByte
 * MUST be used with GraphiteIntCustomWindowMaster
 * MUST be used with GraphiteDebugWindowWorkerContext
 */
public abstract class DebugUByteWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<UnsignedByte, S>, EP, E extends IntervalData<UnsignedByte, EP>, PW, P, IM extends IntervalMessage<UnsignedByte, P>> extends DebugWindowIntervalComputation<I, UnsignedByte, S, V, EP, E, PW, P, IM> {
>>>>>>> main:src/in/dreamlab/wicm/algorithms/wicm/DebugUByteWindowIntervalComputation.java
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";
    private static final String Mutation = "isMutationSuperstep";

    @Override
    public void preSuperstep() {
<<<<<<< HEAD:src/in/dreamlab/wicm/graph/computation/DebugUByteWindowIntervalComputation.java
        isMutation = ((BooleanWritable) getAggregatedValue(Mutation)).get();
=======
        super.preSuperstep();

        // get information from master regarding window execution
>>>>>>> main:src/in/dreamlab/wicm/algorithms/wicm/DebugUByteWindowIntervalComputation.java
        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new UByteInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                                            ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new UByteInterval(windowInterval.getStart().intValue(),
                                        ((IntWritable) getAggregatedValue(GEnd)).get());

        super.preSuperstep();
    }
}

