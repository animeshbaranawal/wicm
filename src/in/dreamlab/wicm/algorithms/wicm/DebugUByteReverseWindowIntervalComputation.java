package in.dreamlab.wicm.algorithms.wicm;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graph.computation.DebugWindowIntervalComputation;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Time domain is UnsignedByte
 * MUST be used with GraphiteIntReverseCustomWindowMaster
 * MUST be used with GraphiteDebugWindowWorkerContext
 */
public abstract class DebugUByteReverseWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<UnsignedByte, S>, EP, E extends IntervalData<UnsignedByte, EP>, PW, P, IM extends IntervalMessage<UnsignedByte, P>> extends DebugWindowIntervalComputation<I, UnsignedByte, S, V, EP, E, PW, P, IM> {
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";
    private static final String GStart = "graphTimeStart";

    @Override
    public void preSuperstep() {
        super.preSuperstep();

        // get information from master regarding window execution
        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new UByteInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new UByteInterval(((IntWritable) getAggregatedValue(GStart)).get(), windowInterval.getStart().intValue());
    }
}
