package elements;

import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Edge class represents an Edge in the Graph.
 * @implNote In order to make edge ids unique we encode src and destination vertex ids in it along with optional attribute to represent timestamp or other things. Latter is needed in case of multi-modal or multi-graphs
 * @implNote Vertex updates should not happen within edges they will be ignored
 */
public final class UniEdge extends GraphElement implements Edge {
    public static String DELIMITER = "~"; // Delimited for creating id

    @OmitStorage
    public String[] ids;

    @Nullable
    @OmitStorage
    public Vertex src;

    @Nullable
    @OmitStorage
    public Vertex dest;

    public UniEdge() {
        super();
    }

    public UniEdge(String id) {
        super();
        ids = decodeVertexIdsAndAttribute(id);
    }

    public UniEdge(Vertex src, Vertex dest) {
        super();
        this.src = src;
        this.dest = dest;
        ids = new String[]{src.getId(), dest.getId()};
    }

    public UniEdge(Vertex src, Vertex dest, String attributeId) {
        super();
        this.src = src;
        this.dest = dest;
        ids = new String[]{this.src.getId(), dest.getId(), attributeId};
    }

    public UniEdge(UniEdge e, boolean deepCopy) {
        super(e, deepCopy);
        this.src = e.src;
        this.dest = e.dest;
    }

    @Override
    public UniEdge copy() {
        return new UniEdge(this, false);
    }

    @Override
    public UniEdge deepCopy() {
        return new UniEdge(this, true);
    }


    // CRUD METHODS
    @Override
    public Vertex getSrc() {
        if (src == null && storage != null) src = storage.getVertex(ids[0]);
        return src;
    }

    @Override
    public Vertex getDest() {
        if (dest == null && storage != null) dest = storage.getVertex(ids[1]);
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


    // NORMAL METHOD

    @Override
    public String getId() {
        return ids[0] + DELIMITER + ids[1];
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        getSrc().setStorage(storage);
        getDest().setStorage(storage);
    }

    @Override
    public void delay() {
        super.delay();
        getSrc().delay();
        getDest().delay();
    }

    @Override
    public void resume() {
        super.resume();
        getSrc().resume();
        getDest().resume();
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        getSrc().clearFeatures();
        getDest().clearFeatures();
    }


    // STATIC METHODS

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
     * Encode attribute-full edge id
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

}
