package elements;

public class Vertex extends ReplicableGraphElement {

    public Vertex() {
        super();
    }

    public Vertex(String id) {
        super(id, false, (short) -1);
    }

    public Vertex(String id, boolean halo) {
        super(id, halo, (short) -1);
    }

    public Vertex(String id, boolean halo, short master) {
        super(id, halo, master);
    }

    @Override
    public Vertex copy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.ts = this.ts;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Vertex deepCopy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.ts = this.ts;
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
