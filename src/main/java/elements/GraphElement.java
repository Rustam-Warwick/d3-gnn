package elements;

import scala.Tuple2;
import storage.BaseStorage;

import java.util.HashMap;

public class GraphElement {
    public String id;
    public short part_id = -1; // not in storage yet
    public BaseStorage storage = null;
    public HashMap<String,GraphElement> features = new HashMap<>();

    public GraphElement(String id) {
        this.id = id;
    }

    public GraphElement(String id, short part_id) {
        this.id = id;
        this.part_id = part_id;
    }

    public Boolean createElement(){
        boolean is_created = this.storage.addElement(this);
        if(is_created){
            for(GraphElement el: this.features.values()){
                el.createElement();
            }

        }
        return is_created;
    }

    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement){
        boolean is_updated = false;

        return new Tuple2<>(is_updated, this);

    }

    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement){
        return new Tuple2<>(false, this);
    }

    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement){
        return new Tuple2<>(false, this);
    }

    public ElementType elementType(){
        return ElementType.NONE;
    }

    public Boolean isReplicable(){
        return false;
    }

    public short masterPart(){
        return this.part_id;
    }

    public ReplicaState state(){
        return ReplicaState.MASTER;
    }

    public short[] replicaParts(){
        return new short[0];
    }

    public Boolean isHalo(){
        return false;
    }

}
