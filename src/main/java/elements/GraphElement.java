package elements;

import scala.Tuple2;
import storage.BaseStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class GraphElement {
    public String id;
    public short partId = -1; // not in storage yet
    public BaseStorage storage = null;
    public HashMap<String,Feature> features = new HashMap<>();

    public GraphElement(String id) {
        this.id = id;
    }

    public GraphElement(String id, short part_id) {
        this.id = id;
        this.partId = part_id;
    }

    public GraphElement copy(){
        return new GraphElement(this.id, this.partId);
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
            Feature thisFeature = this.getFeature(entry.getKey());
            if(Objects.nonNull(thisFeature)){
                Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(entry.getValue());
                is_updated |= tmp._1();
                memento.features.put(entry.getKey(), (Feature) tmp._2());
            }else{
                this.setFeature(entry.getValue());
                is_updated = true;
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



    // Typing stuff
    public ElementType elementType(){
        return ElementType.NONE;
    }

    public Boolean isReplicable(){
        return false;
    }

    public short masterPart(){
        return this.partId;
    }

    public ReplicaState state(){
        if(this.partId == -1) return ReplicaState.UNDEFINED;
        if(this.partId == this.masterPart()) return ReplicaState.MASTER;
        return ReplicaState.REPLICA;
    }

    public short[] replicaParts(){
        return new short[0];
    }

    public Boolean isHalo(){
        return false;
    }
    // Getters and Setters


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public short getPartId() {
        return partId;
    }

    public void setPartId(short partId) {
        this.partId = partId;
    }

    public void setStorage(BaseStorage storage){
        this.storage = storage;
        this.partId = storage.partId;
        for(Map.Entry<String, Feature> feature: this.features.entrySet()){
            feature.getValue().setStorage(storage);
        }
    }

    public Feature getFeature(String id){
        Feature result = this.features.getOrDefault(id, null);
        if(result == null && this.storage!=null){
            result = this.storage.getFeature(id);
            this.features.put(id, result);
        }
        return result;
    }

    public void setFeature(Feature feature){
        Feature exists = this.getFeature(feature.id);
        exists.setElement(this);
        exists.setStorage(this.storage);
        if(Objects.nonNull(exists))return;
        if(Objects.nonNull(this.storage)){
            if (feature.createElement()){
                this.features.put(feature.id, feature);
            }
        }else{
            this.features.put(feature.id, feature);
        }
    }

    public void cacheFeatures(){
        for(Map.Entry<String, Feature> feature: this.features.entrySet()){
            feature.getValue().cacheFeatures();
        }
    }

    @Override
    public String toString() {
        return "GraphElement{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GraphElement that = (GraphElement) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    
}
