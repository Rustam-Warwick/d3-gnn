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

    public Edge(Edge e, boolean deepCopy) {
        super(e, deepCopy);
        this.src = e.src != null ? e.src.copy() : null;
        this.dest = e.dest != null ? e.dest.copy() : null;
    }

    @Override
    public Edge copy() {
        return new Edge(this, false);
    }

    @Override
    public Edge deepCopy() {
        return new Edge(this, true);
    }

    /**
     * Reverses this Edge.
     *
     * @return thsi edge
     */
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
        // Update or Create Vertices
        assert storage != null;
        if (!storage.containsVertex(src.getId())) {
            src.create();
        } else {
            storage.getVertex(src.getId()).update(this.src);
        }
        if (!storage.containsVertex(dest.getId())) {
            dest.create();
        } else {
            storage.getVertex(dest.getId()).update(this.dest);
        }
        return super.create();
    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
        this.src.setStorage(storage);
        this.dest.setStorage(storage);
    }

    @Override
    public void clearFeatures() {
        super.clearFeatures();
        this.src.clearFeatures();
        this.dest.clearFeatures();
    }
}
