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
    public Edge copy() {
        Vertex srcCpy = this.src.copy();
        Vertex destCpy = this.dest.copy();
        Edge tmp = new Edge(srcCpy, destCpy);
        tmp.ts = this.ts;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Edge deepCopy() {
        Vertex srcCpy = this.src.deepCopy();
        Vertex destCpy = this.dest.deepCopy();
        Edge tmp = new Edge(srcCpy, destCpy);
        tmp.ts = this.ts;
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    public Edge reverse() {
        Vertex srcTemp = this.src;
        this.src = this.dest;
        this.dest = srcTemp;
        this.id = this.src.getId() + ":" + this.dest.getId();
        return this;
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
