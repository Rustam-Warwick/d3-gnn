package types;


import storage.GraphStorage;

/**
 * Base class for all elements is the graph (Vertex,Edge,)
 */
abstract public class GraphElement{
    public String id = null;
    public transient GraphStorage storage = null;
    public Short partId = null;
    public GraphElement(String id, GraphStorage storage){
        this.setId(id);
        this.setStorage(storage);
    }
    public GraphElement(String id){
        this.setId(id);
        this.storage = null;
    }
    public GraphElement(){
        this.id = null;
        this.storage = null;
    }

    public String getId(){
        return this.id;
    }
    public void setId(String id){
        this.id = id;
    }
    public void setStorage(GraphStorage storage){
        this.storage = storage;
        this.partId = storage.getPart().getPartId();
    }

    public GraphStorage getStorage() {
        return storage;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

    public Short getPartId() {
        return partId;
    }

    @Override
    public boolean equals(Object o){
        GraphElement e =(GraphElement) o;
        return e.getClass().toString().equals(this.getClass().toString()) && this.getId().equals(e.getId());
    }

}
