package in.dreamlab.wicm.io.mutations;

import com.google.common.collect.Lists;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import in.dreamlab.wicm.types.VarIntWritable;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;

import java.util.List;

public class UByteSSSPMutationFileReader extends WICMMutationFileReader<VarIntWritable, UByteIntIntervalData, UByteIntIntervalData> {
    private static final IntConfOption LAST_SNAPSHOT = new IntConfOption("lastSnapshot", 255, "Snapshot number at infinity");

    @Override
    VarIntWritable setVertexId(String[] line) {
        return new VarIntWritable(Integer.parseInt(line[1]));
    }

    @Override
    UByteIntIntervalData setVertexValue(String[] line) {
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

        return new UByteIntIntervalData(new UByteInterval(startpoint, endpoint));
    }

    @Override
    List<Edge<VarIntWritable, UByteIntIntervalData>> setEdges(String[] line) {
        if(getMode() == MODE.DELETE_VERTEX)
            return null;

        int startIndex = (getMode() == MODE.ADD_VERTEX || getMode() == MODE.TRUNCATE_VERTEX) ? 3 : 2;
        List<Edge<VarIntWritable, UByteIntIntervalData>> edges =
                Lists.newArrayListWithCapacity((line.length - startIndex)/3);
        for (int n = startIndex; n < line.length; n=n+3) {
            edges.add(EdgeFactory.create(
                    new VarIntWritable(Integer.parseInt(line[n])),
                    new UByteIntIntervalData(new UByteInterval(Integer.parseInt(line[n+1]), Integer.parseInt(line[n+2])))
            ));
        }
        return edges;
    }
}
