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

    public Vertex(String id, boolean halo, short master) {
        super(halo, master);
        this.id = id;
    }

    public Vertex(Vertex v, boolean deepCopy) {
        super(v, deepCopy);
        this.id = v.id;
    }

    @Override
    public Vertex copy(CopyContext context) {
        return new Vertex(this, false);
    }

    // NORMAL METHODS
    @Override
    public String getId() {
        return id;
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
