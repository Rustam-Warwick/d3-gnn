package elements;

public class Vertex extends ReplicableGraphElement {

    public Vertex(){
        super();
    }

    public Vertex(String id) {
        super(id);
    }

    public Vertex(String id, boolean halo) {
        super(id, halo);
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
