package in.dreamlab.wicm.io.mutations;

import com.google.common.collect.Lists;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.List;

public class UByteFASTMutationFileReader extends WICMMutationFileReader<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> {
    private static final IntConfOption LAST_SNAPSHOT = new IntConfOption("lastSnapshot", 255, "Snapshot number at infinity");

    @Override
    IntWritable setVertexId(String[] line) {
        return new IntWritable(Integer.parseInt(line[1]));
    }

    @Override
    UByteUByteIntervalData setVertexValue(String[] line) {
        if(getMode() == MODE.DELETE_VERTEX)
            return null;

        String[] points = line[2].split("/");
        int startpoint,endpoint;
        if(getMode() == MODE.ADD_VERTEX) {
            startpoint = Integer.parseInt(points[0]);
            endpoint = (points.length == 2) ? Integer.parseInt(points[1]) : LAST_SNAPSHOT.get(getConf());
        } else if(getMode() == MODE.TRUNCATE_VERTEX) {
            startpoint = 0;
            endpoint = Integer.parseInt(points[0]);
        } else {
            startpoint = 0;
            endpoint = 0;
        }

        return new UByteUByteIntervalData(new UByteInterval(startpoint, endpoint));
    }

    @Override
    List<Edge<IntWritable, UByteUByteIntervalData>> setEdges(String[] line) {
        if(getMode() == MODE.DELETE_VERTEX)
            return null;

        int startIndex = (getMode() == MODE.ADD_VERTEX || getMode() == MODE.TRUNCATE_VERTEX) ? 3 : 2;
        List<Edge<IntWritable, UByteUByteIntervalData>> edges =
                Lists.newArrayListWithCapacity((line.length - startIndex)/3);
        for (int n = startIndex; n < line.length; n=n+3) {
            edges.add(EdgeFactory.create(
                    new IntWritable(Integer.parseInt(line[n])),
                    new UByteUByteIntervalData(new UByteInterval(Integer.parseInt(line[n+1]), Integer.parseInt(line[n+2])))
            ));
        }
        return edges;
    }
}
