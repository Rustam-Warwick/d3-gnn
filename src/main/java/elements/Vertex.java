package elements;

public class Vertex extends ReplicableGraphElement {

    public Vertex() {
        super();
    }

    public Vertex(String id) {
        super(id);
    }

    public Vertex(String id, boolean halo) {
        super(id, halo);
    }

    public Vertex(String id, boolean halo, short master) {
        super(id, halo, master);
    }

    @Override
    public GraphElement copy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
