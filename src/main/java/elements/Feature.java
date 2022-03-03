package elements;

import iterations.IterationState;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class Feature<T> extends ReplicableGraphElement {
    public Object value;
    public transient GraphElement element;
    public Tuple2<ElementType, String> attachedTo = new Tuple2<>(ElementType.NONE, null);
    public Feature(){
        super();
        this.value = null;
    }
    public Feature(Object value){
        super();
        this.value = value;
    }
    public Feature(Object value, boolean halo){
        super(null, halo);
        this.value = value;
    }
    public Feature(Object value, boolean halo, short master){
        super(null, halo, master);
        this.value = value;
    }
    public Feature(String id, Object value){
        super(id);
        this.value = value;
    }
    public Feature(String id, Object value, boolean halo){
        super(id, halo);
        this.value = value;
    }
    public Feature(String id, Object value, boolean halo, short master){
        super(id, halo, master);
        this.value = value;
    }


    // Main Logic
    @Override
    public Boolean createElement() {
        if(this.attachedTo._1 == ElementType.NONE) return super.createElement();
        else{
            boolean is_created = this.storage.addFeature(this);
            if(is_created){
                for(GraphElement el: this.features.values()){
                    el.createElement();
                }
                this.storage.getPlugins().forEach(item->item.addElementCallback(this));
                if(this.state() == ReplicaState.MASTER) this.syncReplicas(false);
            }
            return is_created;
        }
    }

    @Override
    public Tuple2<Boolean, GraphElement> updateElement(GraphElement newElement) {
        boolean isUpdated;
        Feature<T> memento = (Feature<T>) this.copy();
        Feature<T> newFeature = (Feature<T>) newElement;
        if(Objects.isNull(this.value) && Objects.isNull(newFeature.value)) isUpdated = false;
        else if(Objects.isNull(this.value) || Objects.isNull(newFeature.value)) isUpdated = true;
        else isUpdated = !this.valuesEqual(newFeature.value, this.value);
        if(isUpdated){
            this.value = newFeature.value;
            this.storage.updateFeature(this);
            this.storage.getPlugins().forEach(item->item.updateElementCallback(this,memento));
        }
        return new Tuple2<>(isUpdated, memento);
    }

    public void syncReplicas(boolean skipHalo){
        if((this.state() != ReplicaState.MASTER) ||  (this.isHalo() && skipHalo) || this.replicaParts()==null || this.replicaParts().isEmpty()) return;
        this.cacheFeatures();
        Feature cpy = (Feature) this.copy();
        if(this.isHalo()) cpy.value = null;
        for(Map.Entry<String, Feature> feature: this.features.entrySet()){
            if(skipHalo && feature.getValue().isHalo())continue;
            Feature tmp = feature.getValue();
            if(tmp.isHalo()){
                tmp = (Feature) tmp.copy();
                tmp.value = null;
            }
            cpy.setFeature(feature.getKey(), tmp);
        }
        this.replicaParts().forEach(part_id-> this.storage.message(new GraphOp(Op.SYNC, part_id, cpy, IterationState.ITERATE)));
    }

    public void syncReplica(short part_id){
        if(this.state() != ReplicaState.MASTER) return;
        this.cacheFeatures();
        Feature cpy = (Feature) this.copy();
        if(this.isHalo()) cpy.value = null;
        for(Map.Entry<String, Feature> feature: this.features.entrySet()){
            Feature tmp = feature.getValue();
            if(tmp.isHalo()){
                tmp = (Feature) tmp.copy();
                tmp.value = null;
            }
            cpy.setFeature(feature.getKey(), tmp);
        }

        this.storage.message(new GraphOp(Op.SYNC, part_id, cpy, IterationState.ITERATE));
    }



    // Abstract Methods and
    abstract public T getValue();
    abstract public boolean valuesEqual(Object v1, Object v2);
    // Getters and setters


    @Override
    public short masterPart() {
        if(Objects.nonNull(this.getElement())){
            return this.getElement().masterPart();
        }
        return this.masterPart();
    }

    @Override
    public ElementType elementType() {
        return ElementType.FEATURE;
    }

    @Override
    public List<Short> replicaParts() {
        if(Objects.nonNull(this.getElement())){
            return this.getElement().replicaParts();
        }
        return this.replicaParts();
    }

    @Override
    public String getId() {
        if(this.attachedTo._1 == ElementType.NONE) return super.getId();
        return this.attachedTo._2 + this.id;
    }

    public GraphElement getElement() {
        if(this.attachedTo._1 == ElementType.NONE) return null;
        if(this.element == null && this.storage != null){
            this.element = this.storage.getElement(this.attachedTo._2, this.attachedTo._1);
        }
        return this.element;
    }

    public void setElement(GraphElement element) {
        this.element = element;
        this.attachedTo = new Tuple2<>(element.elementType(), element.getId());
    }
}
