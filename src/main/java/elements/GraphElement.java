package elements;

import elements.features.Feature;
import org.apache.flink.api.python.shaded.org.apache.arrow.flatbuf.Bool;
import scala.Tuple2;
import storage.BaseStorage;

import java.util.HashMap;
import java.util.Map;

public class GraphElement {
    public String id;
    public short part_id = -1; // not in storage yet
    public BaseStorage storage = null;
    public HashMap<String,Feature> features = new HashMap<>();

    public GraphElement(String id) {
        this.id = id;
    }

    public GraphElement(String id, short part_id) {
        this.id = id;
        this.part_id = part_id;
    }

    public GraphElement copy(){
        return new GraphElement(this.id, this.part_id);
    }
    // Main Logical Stuff
    public Boolean createElement(){
        boolean is_created = this.storage.addElement(this);
        if(is_created){
            for(GraphElement el: this.features.values()){
                el.createElement();
            }
            this.storage.getAggregators().forEach(item->item.addElementCallback(this));
        }
        return is_created;
    }

    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement){
        GraphElement memento = this.copy();
        boolean is_updated = false;
        for(Map.Entry<String, Feature> entry: newElement.features.entrySet()){
            Feature thisFeature = this.get(entry.getKey());
            if(thisFeature != null){
                Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(entry.getValue());
                is_updated |= tmp._1();
                memento.features.put(entry.getKey(), (Feature) tmp._2());
            }else{

            }

        }
        if(is_updated){
            this.storage.updateElement(this);
            this.storage.getAggregators().forEach(item->item.updateElementCallback(this, memento));
        }

        return new Tuple2<>(is_updated, memento);

    }

    public Tuple2<Boolean, GraphElement> syncElement(GraphElement newElement){
        return new Tuple2<>(false, this);
    }

    public Tuple2<Boolean, GraphElement> externalUpdate(GraphElement newElement){
        return this.updateElement(newElement);
    }
    // Getters
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
        if(this.part_id == -1) return ReplicaState.UNDEFINED;
        if(this.part_id == this.masterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    public short[] replicaParts(){
        return new short[0];
    }

    public Boolean isHalo(){
        return false;
    }

    public Feature get(String id){
        Feature result = this.features.getOrDefault(id, null);
        if(result == null && this.storage!=null){
            result = this.storage.getFeature(id);
            this.features.put(id, result);
        }
        return result;
    }

}
