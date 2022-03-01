package elements;

import scala.Tuple2;
import storage.BaseStorage;

public class Edge extends GraphElement{
    public Vertex src;
    public Vertex dest;
    public Edge(Vertex src, Vertex dest) {
        super(src.id + ":" + dest.id);
        this.src = src;
        this.dest = dest;
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }

    @Override
    public Boolean createElement() {
        this.src.createElement();
        this.dest.createElement();
        return super.createElement();
    }



    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        this.src.setStorage(storage);
        this.dest.setStorage(storage);
    }

}
