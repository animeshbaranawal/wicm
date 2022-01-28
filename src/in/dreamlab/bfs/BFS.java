package in.dreamlab.bfs;

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class BFS extends
        BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("source", 0, "Source");


    @Override
    public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        if(getSuperstep() == 0) {
            if(vertex.getId().get() == SOURCE_ID.get(getConf())) {
                vertex.getValue().set(0);
                IntWritable message = new IntWritable(1);
                for(Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
                    sendMessage(e.getTargetVertexId(), message);
                }
            }
        } else {
            int minLevel = Integer.MAX_VALUE;
            for(IntWritable m : messages) minLevel = Integer.min(minLevel, m.get());
            if(vertex.getValue().get() > minLevel) {
                vertex.getValue().set(minLevel);
                IntWritable message = new IntWritable(minLevel + 1);
                for(Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
                    sendMessage(e.getTargetVertexId(), message);
                }
            }
        }
        vertex.voteToHalt();
    }
}
