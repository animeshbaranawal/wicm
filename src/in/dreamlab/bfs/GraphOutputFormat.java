package in.dreamlab.bfs;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class GraphOutputFormat extends TextVertexOutputFormat<IntWritable, IntWritable, NullWritable> {

    /** Specify the output delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = " ";

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
        /** Saved delimiter */
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<IntWritable, IntWritable, NullWritable> vertex)
                throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(vertex.getId().toString());
            str.append(delimiter);

            if(vertex.getValue().get() == Integer.MAX_VALUE) str.append(-1);
            else str.append(vertex.getValue().get());

            for(Edge<IntWritable, NullWritable> e : vertex.getEdges()){
                str.append(delimiter).append(e.getTargetVertexId().get());
            }
            return new Text(str.toString());
        }
    }
}
