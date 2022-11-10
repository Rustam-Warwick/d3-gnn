package elements;

import elements.annotations.OmitStorage;
import elements.enums.CopyContext;
import elements.enums.ElementType;

public final class Vertex extends ReplicableGraphElement {

    @OmitStorage
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

    @Override
    public Vertex copy(CopyContext context) {
        return new Vertex(this, context);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
