package elements;

import ai.djl.ndarray.NDArray;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Edge class represents an Edge in the Graph.
 * @implNote In order to make edge ids unique we encode src and destination vertex ids in it along with optional attribute to represent timestamp or other things. Latter is needed in case of multi-modal or multigraphs
 * @implNote  Vertex updates should not happen within edges and they will be ignored
 */
public class Edge extends GraphElement {
    public static String DELIMITER = "~";

    @Nullable
    @OmitStorage
    public Vertex src;

    @Nullable
    @OmitStorage
    public Vertex dest;

    public Edge() {
        super();
    }

    /**
     * Normal Edge Creation
     */
    public Edge(Vertex src, Vertex dest) {
        super(encodeEdgeId(src.getId(), dest.getId()));
        this.src = src;
        this.dest = dest;
    }

    /**
     * Attributed Edge Creation
     */
    public Edge(Vertex src, Vertex dest, String attribute) {
        super(encodeEdgeId(src.getId(), dest.getId(), attribute));
        this.src = src;
        this.dest = dest;
    }

    public Edge(Edge e, boolean deepCopy) {
        super(e, deepCopy);
        this.src = e.src;
        this.dest = e.dest;
    }

    /**
     * Returns [src_id, dest_id, att]
     */
    public static String[] decodeVertexIdsAndAttribute(String edgeId) {
        return edgeId.split(DELIMITER);
    }

    /**
     * Encode attribute-less edge id
     */
    public static String encodeEdgeId(String srcId, String destId) {
        return srcId + DELIMITER + destId + DELIMITER;
    }

    /**
     * Encode atribute-full edge id
     */
    public static String encodeEdgeId(String srcId, String destId, String attribute) {
        return srcId + DELIMITER + destId + DELIMITER + attribute;
    }

    /**
     * Contains attribute or not
     */
    public static boolean isAttributed(String edgeId) {
        return !edgeId.substring(edgeId.length() - 1).equals(DELIMITER);
    }

    @Override
    public Edge copy() {
        return new Edge(this, false);
    }

    @Override
    public Edge deepCopy() {
        return new Edge(this, true);
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }

    public Vertex getSrc() {
        if (src == null && storage != null) src = storage.getVertex(decodeVertexIdsAndAttribute(getId())[0]);
        return src;
    }

    public Vertex getDest() {
        if (dest == null && storage != null) dest = storage.getVertex(decodeVertexIdsAndAttribute(getId())[1]);
        return dest;
    }

    @Override
    protected Consumer<Plugin> createElement() {
        assert storage != null;
        if (src != null && !storage.containsVertex(getSrc().getId())) {
            src.create();
        }
        if (dest != null && !storage.containsVertex(getDest().getId())) {
            dest.create();
        }
        src = null;
        dest = null;
        return super.createElement();
    }

    @Override
    public void applyForNDArrays(Consumer<NDArray> operation) {
        super.applyForNDArrays(operation);
        getSrc().applyForNDArrays(operation);
        getDest().applyForNDArrays(operation);
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        if (getSrc() != null) getSrc().setStorage(storage);
        if (getDest() != null) getDest().setStorage(storage);
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        if (getSrc() != null) getSrc().clearFeatures();
        if (getDest() != null) getDest().clearFeatures();
    }


}
