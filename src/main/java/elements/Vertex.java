package elements;

public class Vertex extends ReplicableGraphElement {

    public Vertex() {
        super();
    }

    public Vertex(String id) {
        super(id, false, (short) -1);
    }

    public Vertex(String id, boolean halo, short master) {
        super(id, halo, master);
    }

    public Vertex(Vertex v, boolean deepCopy) {
        super(v, deepCopy);
    }

    @Override
    public Vertex copy() {
        return new Vertex(this, false);
    }

    @Override
    public Vertex deepCopy() {
        return new Vertex(this, true);
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
