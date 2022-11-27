package elements;

import elements.enums.CopyContext;
import elements.enums.ElementType;

/**
 * Vertex --> {@link ReplicableGraphElement}
 */
public final class Vertex extends ReplicableGraphElement {

    /**
     * Vertex id
     */
    public String id;

    public Vertex() {
        super();
    }

    public Vertex(String id) {
        super();
        this.id = id;
    }

    public Vertex(String id, short master) {
        super(master);
        this.id = id;
    }

    public Vertex(Vertex element, CopyContext copyContext) {
        super(element, copyContext);
        this.id = element.id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex copy(CopyContext context) {
        return new Vertex(this, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ElementType getType() {
        return ElementType.VERTEX;
    }
}
