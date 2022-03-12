package elements;

import scala.Tuple2;

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
    public Vertex(String id, boolean halo, short master) {
        super(id, halo, master);
    }

    @Override
    public GraphElement copy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement) {
        if(this.getId().equals("434") && this.storage.isLast()){
            if(this.state() == ReplicaState.MASTER){
                System.out.println("Sync Request From Replica: "+newElement.getPartId() + "To Master: "+this.getPartId());
            }
            else{
                System.out.println("Sync Request From Master: "+newElement.getPartId() + "To Replica: "+this.getPartId());            }
        }
        return super.syncElement(newElement);
    }

    @Override
    public GraphElement deepCopy() {
        Vertex tmp = new Vertex(this.id, this.halo, this.master);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }

    @Override
    public ElementType elementType() {
        return ElementType.VERTEX;
    }
}
