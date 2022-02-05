package in.dreamlab.wicm.graph.computation;

import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graph.computation.BasicIntervalComputation;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.graphite.types.Interval;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * This class is used for building algorithms on native ICM.
 * @param <I> Vertex ID data type
 * @param <T> Time data type
 * @param <S> Vertex state data type
 * @param <V> Vertex Interval State
 * @param <EP> Edge Property data type
 * @param <E> Edge Interval State
 * @param <PW> Warpped Message data type
 * @param <P> Message payload data type
 * @param <IM> Message class
 */
public abstract class DebugBasicIntervalComputation<I extends WritableComparable, T extends Comparable, S, V extends IntervalData<T, S>, EP, E extends IntervalData<T, EP>, PW, P, IM extends IntervalMessage<T, P>> extends BasicIntervalComputation<I, T, S, V, EP, E, PW, P, IM> {
    private static Logger LOG = Logger.getLogger(DebugBasicIntervalComputation.class);

    private PerfDebugData localDebugData = new PerfDebugData(); // store important log information
    private final BooleanConfOption dumpPerfData = new BooleanConfOption("debugPerformance", false, "Collect debug metrics");

    @Override
    public void preSuperstep() {
        localDebugData.initialise();
    }

    @Override
    public void postSuperstep() {
        if(dumpPerfData.get(getConf())) {
            localDebugData.dump(LOG, getSuperstep());
        }
    }

    @Override
    public void compute(Vertex<I, V, E> vertex, Iterable<IM> messages) throws IOException {
        long intervalComputeRegion = System.nanoTime();
        intervalCompute((IntervalVertex<I, T, S, V, EP, E, PW, P, IM>) vertex, messages);
        intervalComputeRegion = System.nanoTime() - intervalComputeRegion;
        localDebugData.ICRegion += intervalComputeRegion;
        localDebugData.giraphCompCalls += 1;
    }

    public void intervalCompute(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Iterable<IM> messages)
            throws IOException {
        long timedRegion;
        if (getSuperstep() == 0) {
            // Initialize, called by infrastructure before the superstep 0 starts.
            timedRegion = System.nanoTime();
            boolean doScatter = init(intervalVertex);
            timedRegion = System.nanoTime() - timedRegion;
            localDebugData.InitRegion += timedRegion;

            // Initialise execution by calling scatter and sending messages.
            if (doScatter) {
                for (Map.Entry<Range<T>, S> vertexState : intervalVertex.getStateEntrySet()) {
                    timedRegion = System.nanoTime();
                    _scatter(intervalVertex, intervalVertex.createInterval(vertexState.getKey()), vertexState.getValue());
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData._SRegion += timedRegion;
                }
            }
        } else {
            int messageSize = Iterables.size(messages);
            int index = Integer.min((messageSize-1)/10, 499);
            localDebugData.WarpMessageDistribution[index] += 1;
            localDebugData.WarpCost += (messageSize*Math.log(messageSize+0.1));

            /** Compute-Side Warp*/
            timedRegion = System.nanoTime();
            RangeMap<T, Pair<S, PW>> warppedIntervals = intervalVertex.warp(messages, intervalVertex.getValue().getPropertyMap(getPropertyLabelForCompute()));
            timedRegion = System.nanoTime() - timedRegion;
            localDebugData.WarpDuplicationCount += intervalVertex.messageDuplicationCount;
            localDebugData.WarpRegion += timedRegion;
            localDebugData.WarpCalls += 1;

            Collection<Pair<Interval<T>, S>> updatedIntervalStates;
            for (Map.Entry<Range<T>, Pair<S, PW>> warpInterval : warppedIntervals.asMapOfRanges().entrySet()) {
                // User-Controlled Filter decides if compute should be invoked
                // By default, filterWarpOutput is always False
                if(!filterWarpOutput(warpInterval.getKey(), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight())) {
                    timedRegion = System.nanoTime();
                    // call user-defined compute operation
                    updatedIntervalStates = compute(intervalVertex, intervalVertex.createInterval(warpInterval.getKey()), warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.CRegion += timedRegion;
                    localDebugData.CompCalls += 1;

                    // for updated states, call scatter
                    for(Pair<Interval<T>, S> updatedIntervalState : updatedIntervalStates) {
                        timedRegion = System.nanoTime();
                        _scatter(intervalVertex, updatedIntervalState.getKey(), updatedIntervalState.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData._SRegion += timedRegion;
                    }

                    if(updatedIntervalStates.isEmpty())
                        localDebugData.RCompCalls += 1;
                }
            }
        }

        if(!intervalVertex.hasVotedToRemainActive())
            intervalVertex.voteToHalt();
    }

    protected void _scatter(IntervalVertex<I, T, S, V, EP, E, PW, P, IM> intervalVertex, Interval<T> interval, S vState) {
        long timedRegion;
        for (Edge<I, E> edge : intervalVertex.getEdges()) {
            /** Scatter-Side Warp*/
            Interval<T> intersection = edge.getValue().getLifespan().getIntersection(interval);
            if (intersection!=null) {
                if(getPropertyLabelForScatter()!=null) { // edge can have temporal properties
                    // State can span multiple fragemented sub-intervals of a single edge
                    for(Map.Entry<Range<T>, EP> edgeSubInterval : edge.getValue().getProperty(getPropertyLabelForScatter(), intersection)) {
                        timedRegion = System.nanoTime();
                        // Call user-defined scatter operation
                        Iterable<IM> messages = scatter(intervalVertex, edge, (Interval<T>) intervalVertex.createInterval(edgeSubInterval.getKey()), vState, edgeSubInterval.getValue());
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.SRegion += timedRegion;
                        localDebugData.ScattCalls += 1;

                        // send messages generated
                        timedRegion = System.nanoTime();
                        for(IM msg : messages) {
                            sendMessage(edge.getTargetVertexId(), msg);
                        }
                        timedRegion = System.nanoTime() - timedRegion;
                        localDebugData.MsgSerRegion += timedRegion;
                        localDebugData.Messages += Iterables.size(messages);
                    }
                } else { // in our case, edges do not have temporal properties
                    // Call user-defined scatter operation
                    timedRegion = System.nanoTime();
                    Iterable<IM> messages = scatter(intervalVertex, edge, intersection, vState, null);
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.SRegion += timedRegion;
                    localDebugData.ScattCalls += 1;

                    // send messages generated
                    timedRegion = System.nanoTime();
                    for(IM msg : messages) {
                        sendMessage(edge.getTargetVertexId(), msg);
                    }
                    timedRegion = System.nanoTime() - timedRegion;
                    localDebugData.MsgSerRegion += timedRegion;
                    localDebugData.Messages += Iterables.size(messages);
                }
            }
        }
    }
}
