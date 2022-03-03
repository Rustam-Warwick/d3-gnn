package elements;

import scala.Tuple2;
import storage.BaseStorage;

public class Edge extends GraphElement{
    public Vertex src;
    public Vertex dest;
    public Edge(){
        super();
        this.src = null;
        this.dest = null;
    }
    public Edge(Vertex src, Vertex dest) {
        super(src.id + ":" + dest.id);
        this.src = src;
        this.dest = dest;
    }

    @Override
    public GraphElement copy() {
        Vertex srcCpy = (Vertex) this.src.copy();
        Vertex destCpy = (Vertex) this.dest.copy();
        return new Edge(srcCpy, destCpy);
    }

    @Override
    public GraphElement deepCopy() {
        Vertex srcCpy = (Vertex) this.src.deepCopy();
        Vertex destCpy = (Vertex) this.dest.deepCopy();
        return new Edge(srcCpy, destCpy);
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
