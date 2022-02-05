package in.dreamlab.wicm.algorithms.wicm;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.graphite.types.IntInterval;
<<<<<<< HEAD:src/in/dreamlab/wicm/graph/computation/DebugIntReverseWindowIntervalComputation.java
import in.dreamlab.wicm.graph.mutations.DebugWICMMutationsIntervalComputation;
=======
import in.dreamlab.wicm.graph.computation.DebugWindowIntervalComputation;
>>>>>>> main:src/in/dreamlab/wicm/algorithms/wicm/DebugIntReverseWindowIntervalComputation.java
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Time domain is Integer
 * MUST be used with GraphiteIntReverseCustomWindowMaster
 * MUST be used with GraphiteDebugWindowWorkerContext
 */
public abstract class DebugIntReverseWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<Integer, S>, EP, E extends IntervalData<Integer, EP>, PW, P, IM extends IntervalMessage<Integer, P>> extends DebugWICMMutationsIntervalComputation<I, Integer, S, V, EP, E, PW, P, IM> {
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GStart = "graphTimeStart";
    private static final String Mutation = "isMutationSuperstep";

    @Override
    public void preSuperstep() {
<<<<<<< HEAD:src/in/dreamlab/wicm/graph/computation/DebugIntReverseWindowIntervalComputation.java
        isMutation = ((BooleanWritable) getAggregatedValue(Mutation)).get();
=======
        super.preSuperstep();

        // get information from master regarding window execution
>>>>>>> main:src/in/dreamlab/wicm/algorithms/wicm/DebugIntReverseWindowIntervalComputation.java
        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new IntInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new IntInterval(((IntWritable) getAggregatedValue(GStart)).get(), windowInterval.getStart());

        super.preSuperstep();
    }
}
