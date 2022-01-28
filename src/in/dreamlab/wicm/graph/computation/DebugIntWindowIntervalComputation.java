package in.dreamlab.wicm.graph.computation;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.wicm.graph.mutations.DebugWICMMutationsIntervalComputation;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Parent Computation class for window ICM with no halting
 * Time domain is Integer
 * MUST be used with GraphiteIntWindowMaster
 * MUST be used with GraphiteDebugWindowWorkerContext
 */
public abstract class DebugIntWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<Integer, S>, EP, E extends IntervalData<Integer, EP>, PW, P, IM extends IntervalMessage<Integer, P>> extends DebugWICMMutationsIntervalComputation<I, Integer, S, V, EP, E, PW, P, IM> {
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";
    private static final String Mutation = "isMutationSuperstep";

    @Override
    public void preSuperstep() {
        isMutation = ((BooleanWritable) getAggregatedValue(Mutation)).get();
        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new IntInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                                        ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new IntInterval(windowInterval.getEnd(), ((IntWritable) getAggregatedValue(GEnd)).get());

        super.preSuperstep();
    }
}
