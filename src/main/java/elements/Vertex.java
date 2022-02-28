package elements;

public class Vertex extends ReplicableGraphElement {
    public Vertex(String id) {
        super(id);
    }

    public Vertex(String id, short part_id) {
        super(id, part_id);
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
