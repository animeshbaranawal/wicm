package in.dreamlab.wicm.io.formats;

import com.google.common.collect.Lists;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class UByteUByteNullTextInputFormat extends
        TextVertexInputFormat<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> {
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
        protected UByteUByteIntervalData getValue(String[] tokens) throws IOException {
            int start = Integer.parseInt(tokens[1]);
            int end = Integer.parseInt(tokens[2]);
            UByteInterval interval = new UByteInterval(new UnsignedByte(start), new UnsignedByte(end));
            return new UByteUByteIntervalData(interval);
        }

        @Override
        protected Iterable<Edge<IntWritable, UByteUByteIntervalData>> getEdges(
                String[] tokens) throws IOException {
            List<Edge<IntWritable, UByteUByteIntervalData>> edges =
                    Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 3; n < tokens.length; n=n+3) {
                edges.add(EdgeFactory.create(
                        new IntWritable(Integer.parseInt(tokens[n])),
                        new UByteUByteIntervalData(new UByteInterval(new UnsignedByte(Integer.parseInt(tokens[n+1])), new UnsignedByte(Integer.parseInt(tokens[n+2]))))
                ));
            }
            return edges;
        }
    }
}
