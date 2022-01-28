package in.dreamlab.wicm.graph.mutations.resolver;

import in.dreamlab.wicm.conf.WICMConstants;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public abstract class WICMVertexResolver<I extends WritableComparable, V extends Writable, E extends Writable>
        extends DefaultVertexResolver<I, V, E>
        implements WICMConstants {
    /** Class logger */
    protected static final Logger LOG = Logger.getLogger(WICMVertexResolver.class);

    @Override
    public Vertex<I, V, E> resolve(
            I vertexId,
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges,
            boolean hasMessages) {
        // 1. If vertex removal desired, remove the vertex.
        vertex = removeVertexIfDesired(vertex, vertexChanges);

        // 2. Create vertex, truncate vertex or modify vertex
        vertex = addVertexIfDesired(vertexId, vertex, vertexChanges, hasMessages);

        return vertex;
    }

    /**
     * Remove the vertex itself if the changes desire it. The actual removal is
     * notified by returning null. That is, this method does not do the actual
     * removal but rather returns null if it should be done.
     *
     * @param vertex Vertex to remove.
     * @param vertexChanges specifies if we should remove vertex
     * @return null if vertex should be removed, otherwise the vertex itself.
     */
    protected Vertex<I, V, E> removeVertexIfDesired(
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges) {
        if (hasVertexRemovals(vertexChanges)) {
            try {
                Text vertexStates = new Text(vertexToString(vertex));
                resolverOutput.write(vertexStates);
            } catch (Exception e) {
                LOG.info("Caught exception "+e);
            }
            vertex = null;
        }
        return vertex;
    }

    protected abstract String vertexToString(Vertex<I,V,E> v);

    /**
     * Add the Vertex if desired. Returns the vertex itself, or null if no vertex
     * added.
     *
     * @param vertexId ID of vertex
     * @param vertex Vertex, if not null just returns it as vertex already exists
     * @param vertexChanges specifies if we should add the vertex
     * @param hasMessages true if this vertex received any messages
     * @return Vertex created or passed in, or null if no vertex should be added
     */
    protected Vertex<I, V, E> addVertexIfDesired(
            I vertexId,
            Vertex<I, V, E> vertex,
            VertexChanges<I, V, E> vertexChanges,
            boolean hasMessages) {
        if (vertex == null && hasVertexAdditions(vertexChanges)) {
            vertex = vertexChanges.getAddedVertexList().get(0);
            initialiseState(vertex);
        } else if (hasVertexAdditions(vertexChanges)) {
            customAction(vertex, vertexChanges.getAddedVertexList().get(0));
        }
        return vertex;
    }

    protected abstract void initialiseState(Vertex<I,V,E> v);

    protected abstract void customAction(Vertex<I,V,E> originalVertex, Vertex<I,V,E> newVertex);
}
