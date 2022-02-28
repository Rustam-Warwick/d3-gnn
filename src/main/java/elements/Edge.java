package elements;

public class Edge extends GraphElement{
    public Vertex src;
    public Vertex dest;
    public Edge(Vertex src, Vertex dest) {
        super(src.id + ":" + dest.id);
        this.src = src;
        this.dest = dest;
    }

    public Edge(Vertex src, Vertex dest, short part_id) {
        super(src.id + ":" + dest.id, part_id);
        this.src = src;
        this.dest = dest;
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }
}
