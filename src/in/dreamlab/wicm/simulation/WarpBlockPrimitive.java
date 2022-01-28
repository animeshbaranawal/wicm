package in.dreamlab.wicm.simulation;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.graphite.warpOperation.Operation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class WarpBlockPrimitive {
    public long warpTime;
    public long bucketingTime;
    public long replicationTime;
    public long duplications;
    public long messageCount;

    public long postWarpTime;
    public long computeCalls;

    Operation<Integer, Integer> warpOperator;

    public WarpBlockPrimitive(Operation<Integer, Integer> operation) {
        warpOperator = operation;
    }

    public void init() {
        warpTime = 0;
        bucketingTime = 0;
        replicationTime = 0;
        postWarpTime = 0;

        computeCalls = 0;
        duplications = 0;
        messageCount = 0;
    }

    private RangeMap<Integer, Boolean> getElementaryIntervals(IntIntIntervalMessage[] messages,
                                                              TreeRangeMap<Integer, Integer> statePartitions,
                                                              IntInterval lifespan,
                                                              boolean useStatePartitionInterval,
                                                              int messagesLength) {
        TreeMap<Integer, Integer> overlaps = new TreeMap<>();
        Integer timePoint, vertexStartTime = lifespan.getStart(), vertexEndTime = lifespan.getEnd();

        for (int i=0; i<messagesLength; i++) {
            IntIntIntervalMessage msg = messages[i];
            timePoint = msg.getValidity().getStart();
            timePoint = timePoint.compareTo(vertexStartTime) >= 0 ? timePoint : vertexStartTime;
            overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) + 1 : 1);

            timePoint = msg.getValidity().getEnd();
            timePoint = timePoint.compareTo(vertexEndTime) <= 0 ? timePoint : vertexEndTime;
            overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) - 1 : -1);
        }

        if (useStatePartitionInterval) {
            for (Range<Integer> range : statePartitions.asMapOfRanges().keySet()) {
                timePoint = range.lowerEndpoint();
                timePoint = timePoint.compareTo(vertexStartTime) >= 0 ? timePoint : vertexStartTime;
                overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) + 1 : 1);

                timePoint = range.upperEndpoint();
                timePoint = timePoint.compareTo(vertexEndTime) <= 0 ? timePoint : vertexEndTime;
                overlaps.put(timePoint, overlaps.containsKey(timePoint) ? overlaps.get(timePoint) - 1 : -1);
            }
        }

        RangeMap<Integer, Boolean> elementaryIntervalMap = TreeRangeMap.create();
        int overlap = 0;
        boolean flag = false;
        Integer last = null;
        for (Integer point : overlaps.keySet()) {
            if (flag)
                elementaryIntervalMap.put(Range.closedOpen(last, point), false);

            overlap += overlaps.get(point);
            last = point;
            flag = overlap > 0;
        }

        return elementaryIntervalMap;
    }

    public TreeRangeMap<Integer, Pair<Integer, Integer>> warp(IntIntIntervalMessage[] messages,
                                                              TreeRangeMap<Integer, Integer> statePartitions,
                                                              IntInterval lifespan,
                                                              boolean insertStatePartitions,
                                                              boolean useStatePartitionInterval,
                                                              int messagesLength) {
        long time = System.nanoTime();
        RangeMap<Integer, Boolean> elementaryIntervalMap = getElementaryIntervals(messages, statePartitions,
                lifespan, useStatePartitionInterval, messagesLength);
        bucketingTime += (System.nanoTime() - time);

        TreeRangeMap<Integer, Pair<Integer, Integer>> warppedIntervalMap = TreeRangeMap.create();

        time = System.nanoTime();
        // Warp InLine Messages
        for (int i=0; i<messagesLength; i++) {
            IntIntIntervalMessage msg = messages[i];

            RangeMap<Integer, Boolean> elementaryIntervals = elementaryIntervalMap
                    .subRangeMap(Range.closedOpen(msg.getValidity().getStart(), msg.getValidity().getEnd()));
            for (Map.Entry<Range<Integer>, Boolean> entry : elementaryIntervals.asMapOfRanges().entrySet()) {
                @Nullable
                Pair<Integer, Integer> wState = warppedIntervalMap.get(entry.getKey().lowerEndpoint());
                if (wState != null) {
                    warppedIntervalMap.put(entry.getKey(),
                            new MutablePair<>(statePartitions.get(entry.getKey().lowerEndpoint()),
                                    warpOperator.operate(wState.getValue(), msg.getPayload())));
                } else {
                    warppedIntervalMap.put(entry.getKey(),
                            new MutablePair<>(statePartitions.get(entry.getKey().lowerEndpoint()),
                                    warpOperator.operate(msg.getPayload())));
                }

                duplications++;
            }
            messageCount++;
        }
        replicationTime += (System.nanoTime() - time);

        return warppedIntervalMap;
    }

    private Collection<Pair<Interval<Integer>, Integer>> compute(TreeRangeMap<Integer, Integer> state,
                                                                 IntInterval interval,
                                                                 Integer currentState, Integer candidateState) {
        if(currentState < candidateState) {
            state.putCoalescing(Range.closedOpen(interval.getStart(), interval.getEnd()), candidateState);
            return Collections.singleton(new ImmutablePair<>(interval, candidateState));
        }

        return Collections.emptyList();
    }

    public void warpBlock(IntIntIntervalMessage[] messages,
                          TreeRangeMap<Integer, Integer> statePartitions,
                          IntInterval lifespan,
                          int messagesLength,
                          TreeRangeMap<Integer, Integer> collectedStates) {
        long time = System.nanoTime();
        RangeMap<Integer, Pair<Integer, Integer>> warppedIntervals = warp(messages, statePartitions, lifespan,
                false, true, messagesLength);
        warpTime += (System.nanoTime() - time);

        time = System.nanoTime();
        Collection<Pair<Interval<Integer>, Integer>> updatedIntervalStates;
        for (Map.Entry<Range<Integer>, Pair<Integer, Integer>> warpInterval : warppedIntervals.asMapOfRanges().entrySet()) {
            updatedIntervalStates = compute(statePartitions,
                    new IntInterval(warpInterval.getKey().lowerEndpoint(), warpInterval.getKey().upperEndpoint()),
                    warpInterval.getValue().getLeft(), warpInterval.getValue().getRight());
            computeCalls++;

            for(Pair<Interval<Integer>, Integer> updatedIntervalState : updatedIntervalStates) {
                collectedStates.putCoalescing(Range.closedOpen(updatedIntervalState.getKey().getStart(), updatedIntervalState.getKey().getEnd()),
                        updatedIntervalState.getValue());
            }
        }
        postWarpTime += (System.nanoTime() - time);
    }
}
