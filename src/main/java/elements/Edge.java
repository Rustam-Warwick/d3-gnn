package elements;

import storage.BaseStorage;

public class Edge extends GraphElement {
    public Vertex src;
    public Vertex dest;

    public Edge() {
        super();
        this.src = null;
        this.dest = null;
    }

    public Edge(Vertex src, Vertex dest) {
        super(src.getId() + ":" + dest.getId());
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

    public void reverse() {
        Vertex srcTemp = this.src;
        this.src = this.dest;
        this.dest = srcTemp;
        this.id = this.src.getId() + ":" + this.dest.getId();
    }

    @Override
    public ElementType elementType() {
        return ElementType.EDGE;
    }

    @Override
    public Boolean create() {
        this.src.create();
        this.dest.create();
        return super.create();
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        this.src.setStorage(storage);
        this.dest.setStorage(storage);
    }

}
