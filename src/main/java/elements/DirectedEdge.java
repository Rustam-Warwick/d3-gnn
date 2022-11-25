package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jetbrains.annotations.Nullable;
import storage.BaseStorage;

import java.util.function.Consumer;


/**
 * Represents a Directed-Edge in the Graph
 *
 * @implNote In order to make edge ids unique we encode src and destination vertex ids in it along with optional attribute to represent timestamp or other things. Latter is needed in case of multi-modal or multi-graphs
 * @implNote A {@link DirectedEdge} will attempt to create {@link Vertex} but not update it, since latter can only happen in MASTER parts
 * <p>
 * src and dest {@link Vertex} should be populated during creation. If either of the vertices do not exist the entire creation pipeline will fail <br/>
 * </p>
 */
public final class DirectedEdge extends GraphElement {

    /**
     * Delimiter used for creating edge id
     */
    public static String DELIMITER = "~";

    /**
     * Ids of edge represented as [srcId, destId, @Optional attributeId]
     */
    public Tuple3<String, String, String> ids;

    /**
     * Src {@link Vertex} for this edge
     */
    @Nullable
    public Vertex src;

    /**
     * Dest {@link Vertex} for this edge
     */
    @Nullable
    public Vertex dest;

    public DirectedEdge() {
        super();
        ids = new Tuple3<>();
    }

    public DirectedEdge(String id) {
        super();
        ids = decodeVertexIdsAndAttribute(id);
    }

    public DirectedEdge(String srcId, String destId, @Nullable String attributedId) {
        super();
        ids = Tuple3.of(srcId, destId, attributedId);
    }

    public DirectedEdge(Vertex src, Vertex dest) {
        super();
        this.src = src;
        this.dest = dest;
        ids = Tuple3.of(src.getId(), dest.getId(), null);
    }

    public DirectedEdge(Vertex src, Vertex dest, String attributeId) {
        super();
        this.src = src;
        this.dest = dest;
        ids = Tuple3.of(src.getId(), dest.getId(), attributeId);
    }

    public DirectedEdge(DirectedEdge e, CopyContext context) {
        super(e, context);
        ids = e.ids;
        src = e.src;
        dest = e.dest;
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
    public DirectedEdge copy(CopyContext context) {
        return new DirectedEdge(this, context);
    }

    /**
     * Attribute of this edge
     */
    @Nullable
    public String getAttribute() {
        return ids.f2;
    }

    /**
     * Get src {@link Vertex}
     */
    public Vertex getSrc() {
        if (src == null && getStorage() != null) src = getStorage().getVertex(ids.f0);
        return src;
    }

    /**
     * Get src vertex id
     */
    public String getSrcId() {
        return ids.f0;
    }

    /**
     * Get dest {@link Vertex}
     */
    public Vertex getDest() {
        if (dest == null && getStorage() != null) dest = getStorage().getVertex(ids.f1);
        return dest;
    }

    /**
     * Get dest vertex id
     */
    public String getDestId() {
        return ids.f1;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creating src and dest vertices if they do not exist as well.
     * But never attempting to update it
     * </p>
     * @implNote If either vertices do not exist, and they are not in this element {@link NullPointerException} is thrown
     */
    @Override
    public Consumer<BaseStorage> createInternal() {
        if (!getStorage().containsVertex(getSrcId())) {
            getStorage().runCallback(src.create());
        }
        if (!getStorage().containsVertex(getDestId())) {
             getStorage().runCallback(dest.create());
        }
        return super.createInternal();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return encodeEdgeId(ids.f0, ids.f1, ids.f2);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.EDGE;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDeserialized() {
        super.onDeserialized();
        if (src != null) src.onDeserialized();
        if (dest != null) dest.onDeserialized();
    }
}
