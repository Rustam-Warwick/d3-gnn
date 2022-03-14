package elements;

import iterations.IterationState;
import scala.Tuple2;

import java.util.List;
import java.util.Objects;

public class Feature<T, V> extends ReplicableGraphElement {
    public T value;
    public transient GraphElement element;
    public Tuple2<ElementType, String> attachedTo = new Tuple2<>(ElementType.NONE, null);
    public Feature(){
        super();
        this.value = null;
    }
    public Feature(T value){
        super();
        this.value = value;
    }
    public Feature(T value, boolean halo){
        super(null, halo);
        this.value = value;
    }
    public Feature(T value, boolean halo, short master){
        super(null, halo, master);
        this.value = value;
    }
    public Feature(String id, T value){
        super(id);
        this.value = value;
    }
    public Feature(String id, T value, boolean halo){
        super(id, halo);
        this.value = value;
    }
    public Feature(String id, T value, boolean halo, short master){
        super(id, halo, master);
        this.value = value;
    }

    @Override
    public GraphElement copy() {
        Feature<T,V> tmp = new Feature<T,V>(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        return tmp;
    }

    @Override
    public GraphElement deepCopy() {
        Feature<T,V> tmp = new Feature<T,V>(this.id, this.value, this.halo, this.master);
        tmp.attachedTo = this.attachedTo;
        tmp.partId = this.partId;
        tmp.element = this.element;
        tmp.storage = this.storage;
        return tmp;
    }

    // Main Logic
    @Override
    public Boolean createElement() {
        if(this.attachedTo._1 == ElementType.NONE) return super.createElement();
        else{
            boolean is_created = this.storage.addFeature(this);
            if(is_created){
                for(GraphElement el: this.features){
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
        Feature<T, V> memento = (Feature<T,V>) this.copy();
        Feature<T, V> newFeature = (Feature<T, V>) newElement;
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
        for(Feature feature: this.features){
            if(skipHalo && feature.isHalo())continue;
            Feature tmp = (Feature) feature.copy();
            if(tmp.isHalo()){
                tmp.value = null;
            }
            cpy.setFeature(feature.getFieldName(), tmp);
        }
        this.replicaParts().forEach(part_id-> this.storage.message(new GraphOp(Op.SYNC, part_id, cpy, IterationState.ITERATE)));
    }

    public void syncReplica(short part_id){
        if(this.state() != ReplicaState.MASTER) return;
        this.cacheFeatures();
        Feature cpy = (Feature) this.copy();
        if(this.isHalo()) cpy.value = null;
        for(Feature feature: this.features){
            Feature tmp = (Feature) feature.copy();
            if(tmp.isHalo()){
                tmp.value = null;
            }
            cpy.setFeature(feature.getFieldName(), tmp);
        }

        this.storage.message(new GraphOp(Op.SYNC, part_id, cpy, IterationState.ITERATE));
    }



    // Abstract Methods and
    public V getValue(){
        return (V) this.value;
    };
    public boolean valuesEqual(T v1, T v2){
        return v1.equals(v2);
    };
    // Getters and setters


    @Override
    public short masterPart() {
        if(Objects.nonNull(this.getElement())){
            return this.getElement().masterPart();
        }
        return super.masterPart();
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
        return super.replicaParts();
    }

    @Override
    public String getId() {
        if(this.attachedTo._1 == ElementType.NONE) return super.getId();
        return this.attachedTo._2 + this.id;
    }
    public String getFieldName(){
        return this.id;
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
        if(Objects.nonNull(element)){
            this.attachedTo = new Tuple2<>(element.elementType(), element.getId());
        }

    }
}
