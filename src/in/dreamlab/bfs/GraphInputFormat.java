package in.dreamlab.bfs;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class GraphInputFormat extends
        TextVertexInputFormat<IntWritable, IntWritable, NullWritable> {
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context)
            throws IOException {
        return new IntIntNullVertexReader();
    }

    public class IntIntNullVertexReader extends
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
        protected IntWritable getValue(String[] tokens) throws IOException {
            return new IntWritable(Integer.MAX_VALUE);
        }

        @Override
        protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
                String[] tokens) throws IOException {
            List<Edge<IntWritable, NullWritable>> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 1; n < tokens.length; n = n + 1) {
                edges.add(EdgeFactory.create(new IntWritable(Integer.parseInt(tokens[n]))));
            }
            return edges;
        }
    }
}