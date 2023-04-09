package storage;

import elements.DirectedEdge;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * View for {@link DirectedEdge} storage in the {@link GraphStorage}
 */
public interface EdgesView  extends Collection<DirectedEdge> {

    /**
     * Filter Edges starting with source
     */
    EdgesView filterSrcId(String srcId);

    /**
     * Filter edges ending with dest
     */
    EdgesView filterDestId(String destId);

    /**
     * Filter edges with vertices on-either side
     */
    EdgesView filterVertexId(String vertexId);

    /**
     * Filter edge with some attribute
     */
    EdgesView filterAttribute(String attribute);

    /**
     * Filter edges with specific src and dest pair. For multi-graphs
     */
    EdgesView filterSrcAndDest(String srcId, String destId);

    /**
     * Remove edge
     */
    boolean remove(String srcId, String destId, @Nullable String attribute);

    @Override
    default boolean remove(Object o){
        DirectedEdge edge = (DirectedEdge) o;
        return remove(edge.getSrcId(), edge.getDestId(), edge.getAttribute());
    };

    /**
     * Get single edge
     */
    DirectedEdge get(String srcId, String destId, @Nullable String attribute);

    /**
     * Contains single edge
     */
    boolean contains(String srcId, String destId, @Nullable String attribute);

    @Override
    default boolean contains(Object o){
        DirectedEdge edge = (DirectedEdge) o;
        return contains(edge.getSrcId(), edge.getDestId(), edge.getAttribute());
    }

}
