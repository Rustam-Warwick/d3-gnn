package elements;

import org.apache.flink.api.java.tuple.Tuple3;
import storage.BaseStorage;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Represents a Directed-Edge in the Graph
 *
 * @implNote In order to make edge ids unique we encode src and destination vertex ids in it along with optional attribute to represent timestamp or other things. Latter is needed in case of multi-modal or multi-graphs
 * @implNote Vertex updates should not happen within edges they will be ignored
 */
public final class DEdge extends GraphElement implements Edge {
    public static String DELIMITER = "~"; // Delimited for creating id

    @OmitStorage
    public Tuple3<String, String, String> ids = new Tuple3<>(); // Ids of edge [src_id, dest_id, Optional[attribute_id]]

    @Nullable
    @OmitStorage
    public Vertex src;

    @Nullable
    @OmitStorage
    public Vertex dest;

    public DEdge() {
        super();
    }

    public DEdge(String id) {
        super();
        ids = decodeVertexIdsAndAttribute(id);
    }

    public DEdge(String srcId, String destId, @Nullable String attributedId) {
        super();
        ids.f0 = srcId;
        ids.f1 = destId;
        ids.f2 = attributedId;
    }

    public DEdge(Vertex src, Vertex dest) {
        super();
        this.src = src;
        this.dest = dest;
        ids.f0 = src.getId();
        ids.f1 = dest.getId();
    }

    public DEdge(Vertex src, Vertex dest, String attributeId) {
        super();
        this.src = src;
        this.dest = dest;
        ids.f0 = src.getId();
        ids.f1 = src.getId();
        ids.f2 = attributeId;
    }


    public DEdge(DEdge e, boolean deepCopy) {
        super(e, deepCopy);
        this.src = e.src;
        this.dest = e.dest;
    }

    /**
     * Returns [src_id, dest_id, att]
     */
    public static Tuple3<String, String, String> decodeVertexIdsAndAttribute(String edgeId) {
        String[] ids = edgeId.split(DELIMITER);
        return new Tuple3<>(ids[0], ids[1], ids.length > 2 ? ids[2] : null);
    }

    /**
     * Encode attribute-less edge id
     */
    public static String encodeEdgeId(String srcId, String destId, @Nullable String attributedId) {
        if (attributedId != null) return srcId + DELIMITER + destId + DELIMITER + attributedId;
        return srcId + DELIMITER + destId + DELIMITER;
    }

    /**
     * Contains attribute or not
     */
    public static boolean isAttributed(String edgeId) {
        return !edgeId.substring(edgeId.length() - 1).equals(DELIMITER);
    }

    @Override
    public DEdge copy() {
        return new DEdge(this, false);
    }

    @Override
    public DEdge deepCopy() {
        return new DEdge(this, true);
    }

    @Nullable
    @Override
    public String getAttribute() {
        return ids.f2;
    }

    // CRUD METHODS
    @Override
    public Vertex getSrc() {
        if (src == null && storage != null) src = storage.getVertex(ids.f0);
        return src;
    }

    @Override
    public String getSrcId() {
        return ids.f0;
    }

    @Override
    public Vertex getDest() {
        if (dest == null && storage != null) dest = storage.getVertex(ids.f1);
        return dest;
    }

    @Override
    public String getDestId() {
        return ids.f1;
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
    public String getId() {
        return encodeEdgeId(ids.f0, ids.f1, ids.f2);
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }


    // STATIC METHODS

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

}
