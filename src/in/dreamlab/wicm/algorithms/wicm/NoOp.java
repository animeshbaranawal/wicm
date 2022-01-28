package in.dreamlab.wicm.algorithms.wicm;

import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.graph.computation.DebugIntWindowIntervalComputation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class NoOp extends
        DebugIntWindowIntervalComputation<IntWritable, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Integer>> compute(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Interval<Integer> interval, Integer currentEarliestArrival, Integer candidateEarliestArrival) throws IOException {
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntIntIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Edge<IntWritable, IntIntIntervalData> edge, Interval<Integer> interval, Integer earliestArrival, Integer nullProperty) {
        return Collections.emptyList();
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(Integer vertexValue) {
        return vertexValue == Integer.MAX_VALUE;
    }
}
