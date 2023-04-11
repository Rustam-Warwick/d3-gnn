package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;
import org.apache.flink.api.java.tuple.Tuple3;
import org.jetbrains.annotations.Nullable;


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
     * Ids of edge represented as [srcId, destId, @Optional attributeId]
     */
    public Tuple3<String, String, String> id;

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
        id = new Tuple3<>();
    }

    public DirectedEdge(String srcId, String destId, @Nullable String attributedId) {
        super();
        id = Tuple3.of(srcId, destId, attributedId);
    }

    public DirectedEdge(Vertex src, Vertex dest) {
        super();
        this.src = src;
        this.dest = dest;
        id = Tuple3.of(src.getId(), dest.getId(), null);
    }

    public DirectedEdge(Vertex src, Vertex dest, String attributeId) {
        super();
        this.src = src;
        this.dest = dest;
        id = Tuple3.of(src.getId(), dest.getId(), attributeId);
    }

    public DirectedEdge(DirectedEdge e, CopyContext context) {
        super(e, context);
        id = e.id;
        src = e.src;
        dest = e.dest;
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
        return id.f2;
    }

    /**
     * Get src {@link Vertex}
     */
    public Vertex getSrc() {
        if (src == null && getRuntimeContext() != null)
            src = getRuntimeContext().getStorage().getVertex(id.f0);
        return src;
    }

    /**
     * Get src vertex id
     */
    public String getSrcId() {
        return id.f0;
    }

    /**
     * Get dest {@link Vertex}
     */
    public Vertex getDest() {
        if (dest == null && getRuntimeContext() != null)
            dest = getRuntimeContext().getStorage().getVertex(id.f1);
        return dest;
    }

    /**
     * Get dest vertex id
     */
    public String getDestId() {
        return id.f1;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creating src and dest vertices if they do not exist as well.
     * But never attempting to update it
     * </p>
     *
     * @implNote If either vertices do not exist, and they are not in this element {@link NullPointerException} is thrown
     */
    @Override
    public void createInternal() {
        if (!getRuntimeContext().getStorage().containsVertex(getSrcId())) {
            src.create();
        }
        if (!getRuntimeContext().getStorage().containsVertex(getDestId())) {
            dest.create();
        }
        super.createInternal();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple3<String, String, String> getId() {
        return id;
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
    public void destroy() {
        super.destroy();
        if (src != null) src.destroy();
        if (dest != null) dest.destroy();
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
