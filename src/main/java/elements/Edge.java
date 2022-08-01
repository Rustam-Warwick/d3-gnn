package elements;

import ai.djl.ndarray.NDArray;
import org.apache.commons.lang3.StringUtils;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Edge class represents an Edge in the Graph.
 * Normally and edge should only contain the ids of its vertex, however because of out-of-orderness we can have a situation where Vertices are not created before the edge arrives
 * We also attach the Edge src and destination features to the Edge
 * If those src, destination Vertices do not exist upon Edge creation those are created as well, otherwise they are turned null
 * Vertex updates should not happen within edges and they will be ignored
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
        String[] val = edgeId.split(DELIMITER);
        if (val.length == 2) {
            return new String[]{val[0], val[1], null};
        } else return val;
    }

    /**
     * Encode attribute-less edge id
     */
    public static String encodeEdgeId(String srcId, String destId) {
        return srcId + DELIMITER + destId;
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
        return StringUtils.countMatches(edgeId, DELIMITER) == 2;
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
        if (src == null) {
            src = storage.getVertex(decodeVertexIdsAndAttribute(getId())[0]);
        }
        return src;
    }

    public Vertex getDest() {
        if (dest == null) {
            dest = storage.getVertex(decodeVertexIdsAndAttribute(getId())[1]);
        }
        return dest;
    }

    @Override
    public Boolean create() {
        assert storage != null;
        if (src != null && !storage.containsVertex(getSrc().getId())) {
            src.create();
        }
        if (dest != null && !storage.containsVertex(getDest().getId())) {
            dest.create();
        }
        src = null;
        dest = null;
        return super.create();
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
        if (src != null) src.setStorage(storage);
        if (dest != null) dest.setStorage(storage);
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        if (src != null) src.clearFeatures();
        if (dest != null) dest.clearFeatures();
    }


}
