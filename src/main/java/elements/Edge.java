package elements;

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
        Edge tmp = new Edge(srcCpy, destCpy);
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        Vertex srcCpy = (Vertex) this.src.deepCopy();
        Vertex destCpy = (Vertex) this.dest.deepCopy();
        Edge tmp = new Edge(srcCpy, destCpy);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
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
