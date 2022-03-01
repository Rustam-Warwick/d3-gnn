package elements;

import scala.Tuple2;

import java.util.Iterator;
import java.util.Objects;

public abstract class Feature<T> extends ReplicableGraphElement {
    public Object value;
    public GraphElement element = null;
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
    public Feature(String id, Object value){
        super(id);
        this.value = value;
    }
    public Feature(String id, Object value, boolean halo){
        super(id, halo);
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
                this.storage.getAggregators().forEach(item->item.addElementCallback(this));
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
            this.storage.getAggregators().forEach(item->item.updateElementCallback(this,memento));
        }
        return new Tuple2<>(isUpdated, memento);
    }


    @Override
    public ElementType elementType() {
       return ElementType.FEATURE;
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
    public Iterator<Short> replicaParts() {
        if(Objects.nonNull(this.getElement())){
            return this.getElement().replicaParts();
        }
        return this.replicaParts();
    }


    public GraphElement getElement() {
        if(this.attachedTo._1 == ElementType.NONE) return null;
        if(this.element == null){
            this.element = this.storage.getElement(this.attachedTo._2, this.attachedTo._1);
        }
        return this.element;
    }

    public void setElement(GraphElement element) {
        this.element = element;
        this.attachedTo = new Tuple2<>(element.elementType(), element.getId());
    }
}
