package elements;

import elements.annotations.OmitStorage;
import elements.enums.CopyContext;
import elements.enums.ElementType;
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
public final class DEdge extends GraphElement {

    public static String DELIMITER = "~"; // Delimited for creating id

    @OmitStorage
    public Tuple3<String, String, String> ids;// Ids of edge [src_id, dest_id, Optional[attribute_id]]

    @Nullable
    @OmitStorage
    public Vertex src;

    @Nullable
    @OmitStorage
    public Vertex dest;

    public DEdge() {
        super();
        ids = new Tuple3();
    }

    public DEdge(String id) {
        super();
        ids = decodeVertexIdsAndAttribute(id);
    }

    public DEdge(String srcId, String destId, @Nullable String attributedId) {
        super();
        ids = Tuple3.of(srcId, destId, attributedId);
    }

    public DEdge(Vertex src, Vertex dest) {
        super();
        this.src = src;
        this.dest = dest;
        ids = Tuple3.of(src.getId(), dest.getId(), null);
    }

    public DEdge(Vertex src, Vertex dest, String attributeId) {
        super();
        this.src = src;
        this.dest = dest;
        ids = Tuple3.of(src.getId(), dest.getId(), attributeId);
    }

    public DEdge(DEdge e, CopyContext context) {
        super(e, context);
        ids = e.ids;
        if (context != CopyContext.SYNC) {
            src = e.src;
            dest = e.dest;
        }
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
     * {@inheritDoc}
     */
    @Override
    public DEdge copy(CopyContext context) {
        return new DEdge(this, context);
    }

    @Nullable
    public String getAttribute() {
        return ids.f2;
    }

    public Vertex getSrc() {
        if (src == null && storage != null) src = storage.getVertex(ids.f0);
        return src;
    }

    public String getSrcId() {
        return ids.f0;
    }

    public Vertex getDest() {
        if (dest == null && storage != null) dest = storage.getVertex(ids.f1);
        return dest;
    }

    public String getDestId() {
        return ids.f1;
    }

    @Override
    protected Consumer<BaseStorage> createElement() {
        if (src != null && !storage.containsVertex(src.getId())) {
            src.create();
        }
        if (dest != null && !storage.containsVertex(dest.getId())) {
            dest.create();
        }
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


    /**
     * {@inheritDoc}
     */
    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        if (src != null) src.setStorage(storage);
        if (dest != null) dest.setStorage(storage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delay() {
        super.delay();
        if (src != null) src.delay();
        if (dest != null) dest.delay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resume() {
        super.resume();
        if (src != null) src.resume();
        if (dest != null) dest.resume();
    }

}
