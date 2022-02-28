package elements;

import elements.ElementType;
import elements.GraphElement;
import elements.ReplicableGraphElement;
import scala.Tuple2;

import java.util.Objects;

public abstract class Feature<T> extends ReplicableGraphElement {
    public Object value = null;
    public GraphElement element = null;
    public Tuple2<ElementType, String> attachedTo = new Tuple2<>(ElementType.NONE, null);
    public Feature(String id, Object value){
        super(id);
        this.value = value;
    }

    public Feature(String id) {
        super(id);
    }

    public Feature(String id, short part_id) {
        super(id, part_id);
    }
    // Main Logic
    @Override
    public Boolean createElement() {
        if(this.attachedTo._1 == ElementType.NONE) return super.createElement();
        else{
            boolean is_created = this.storage.addElement(this);
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
        return super.updateElement(newElement);
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
    public short[] replicaParts() {
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
