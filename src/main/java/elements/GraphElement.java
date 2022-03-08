package elements;

import scala.Serializable;
import scala.Tuple2;
import storage.BaseStorage;

import java.util.*;

public class GraphElement implements Serializable {
    public String id;
    public short partId;
    public transient BaseStorage storage;
    public List<Feature> features;

    public GraphElement(){
        this.id = null;
        this.partId = -1;
        this.storage = null;
        this.features = new ArrayList<>();
    }

    public GraphElement(String id) {
        this.id = id;
        this.partId = -1;
        this.storage = null;
        this.features = new ArrayList<>();
    }

    public GraphElement copy(){
        GraphElement tmp = new GraphElement(this.id);
        tmp.partId = this.partId;
//        tmp.storage = this.storage;
        return tmp;
    }

    public GraphElement deepCopy(){
        GraphElement tmp = new GraphElement(this.id);
        tmp.partId = this.partId;
        tmp.storage = this.storage;
        tmp.features.addAll(this.features);
        return tmp;
    }
    // Main Logical Stuff
    public Boolean createElement(){
        boolean is_created = this.storage.addElement(this);
        if(is_created){
            for(GraphElement el: this.features){
                el.createElement();
            }
            this.storage.getPlugins().forEach(item->item.addElementCallback(this));
        }
        return is_created;
    }

    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement){
        GraphElement memento = this.copy();
        boolean is_updated = false;
        for(Feature feature: newElement.features){
            Feature thisFeature = this.getFeature(feature.getFieldName());
            if(Objects.nonNull(thisFeature)){
                Tuple2<Boolean, GraphElement> tmp = thisFeature.updateElement(feature);
                is_updated |= tmp._1();
                addIfNotExists(memento.features, feature);
            }else{
                this.setFeature(feature.getFieldName(), feature);
                is_updated = true;
            }
        }

        if(is_updated){
            this.storage.updateElement(this);
            this.storage.getPlugins().forEach(item->item.updateElementCallback(this, memento));
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

    public List<Short> replicaParts(){
        return null;
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
        if(Objects.nonNull(this.storage))return storage.currentKey;
        return partId;
    }

    public void setPartId(short partId) {
        this.partId = partId;
    }


    public void setStorage(BaseStorage storage){
        this.storage = storage;
        if(Objects.nonNull(storage))this.partId = storage.currentKey;
        for(Feature ft: this.features){
            ft.setStorage(storage);
        }
    }

    public static void addIfNotExists(List<Feature> list, Feature feature){
        if(!list.contains(feature)){
            list.add(feature);
        }
    }

    public Feature getFeature(String name){
        Feature result = this.features.stream().filter(item->item.getFieldName().equals(name)).findAny().orElse(null);
        if(result == null && this.storage!=null){
            result = this.storage.getFeature(this.getId() + name);
            if(Objects.nonNull(result)){
                result.setElement(this);
                addIfNotExists(this.features, result);
            }
        }
        return result;
    }

    public void setFeature(String name,Feature feature){
        Feature exists = this.getFeature(name);
        if(Objects.nonNull(exists))return;
        feature.setId(name);
        feature.setElement(this);
        feature.setStorage(this.storage);
        if(Objects.nonNull(this.storage)){
            if (feature.createElement()){
                addIfNotExists(this.features, feature);
            }
        }else{
            addIfNotExists(this.features, feature);
        }
    }

    public void cacheFeatures(){
        Map<String, Feature> myFeatures = this.storage.getFeatures(this);
        for(Map.Entry<String, Feature> feature: myFeatures.entrySet()){
            feature.getValue().setElement(this);
            this.setFeature(feature.getKey(), feature.getValue());
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
