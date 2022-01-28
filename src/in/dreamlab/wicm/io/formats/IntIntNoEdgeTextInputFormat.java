package in.dreamlab.wicm.io.formats;

import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.regex.Pattern;

public class IntIntNoEdgeTextInputFormat extends
        TextVertexInputFormat<IntWritable, IntIntIntervalData, IntIntIntervalData> {
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context)
            throws IOException {
        return new IntDoubleNullVertexReader();
    }

    public class IntDoubleNullVertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {
        private IntWritable id;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            id = new IntWritable(Integer.parseInt(tokens[0]));
            return tokens;
        }

        @Override
        protected IntWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected IntIntIntervalData getValue(String[] tokens) throws IOException {
            return new IntIntIntervalData(new IntInterval(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2])));
        }

        @Override
        protected Iterable<Edge<IntWritable, IntIntIntervalData>> getEdges(
                String[] tokens) throws IOException {
            return Collections.emptyList();
        }
    }
}