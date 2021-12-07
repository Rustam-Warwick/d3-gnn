package features;
import types.GraphElement;
import types.ReplicableGraphElement;
import types.SerialFunction;

import java.util.concurrent.CompletableFuture;

abstract public class Feature<T> {
    /**
     * fieldName,attachedId,ClassName uniquely identify a feature within a graph
     */
    public String fieldName;
    public String attachedId;
    public String attachedToClassName;
    public Short partId;
    public transient GraphElement element;
    public Feature(){
        this.fieldName = null;
        this.attachedId = null;
        this.attachedToClassName = null;
        this.element = null;
        this.partId = null;
    }
    public Feature(String fieldName) {
        this.fieldName = fieldName;
        this.attachedId = null;
        this.attachedToClassName = null;
        this.element = null;
        this.partId = null;
    }
    public Feature(String fieldName, GraphElement element){
        this.fieldName = fieldName;
        this.setElement(element);
    }

    /**
     * Feature is attached to graph element
     * Need to update some parameters
     * @param element
     */
    public void setElement(GraphElement element) {
        this.element = element;
        this.attachedId = element.getId();
        this.attachedToClassName = element.getClass().getName();
        this.partId = this.element.partId;
    }

    abstract public CompletableFuture<T> getValue(); // Get the value as a future
    abstract public void setValue(T newValue); // Set the value
    abstract public void updateMessage(Update<T> msg); // External Update Message

    /**
     * Feature Update Requests are serialized using this class
     * Used to save memory
     * @param <T>
     */
    public static class Update<T>{
        public SerialFunction<? extends Feature<T>,Boolean> updateFn;
        public T value = null;
        public String fieldName;
        public Integer lastModified = 0;
        public Short partId;
        public ReplicableGraphElement.STATE state = ReplicableGraphElement.STATE.NONE;
        public String attachedId;
        public String attachedToClassName;

        public Update(){
            this.fieldName = null;
            this.attachedId = null;
            this.attachedToClassName = null;
            this.partId = null;

        }
        public Update(Feature<T> fromFeature, SerialFunction<? extends Feature<T>,Boolean> updateFn){
            this.fieldName = fromFeature.fieldName;
            this.updateFn = updateFn;
            this.attachedToClassName = fromFeature.attachedToClassName;
            this.attachedId = fromFeature.attachedId;
            this.partId = fromFeature.partId;
        }

        public void setLastModified(Integer lastModified) {
            this.lastModified = lastModified;
        }

        public void setState(ReplicableGraphElement.STATE state) {
            this.state = state;
        }

        public void setUpdateFn(SerialFunction<? extends Feature<T>, Boolean> updateFn) {
            this.updateFn = updateFn;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public void setPartId(Short partId) {
            this.partId = partId;
        }

        public void setAttachedId(String attachedId) {
            this.attachedId = attachedId;
        }

        public void setAttachedToClassName(String attachedToClassName) {
            this.attachedToClassName = attachedToClassName;
        }

        public void setValue(T value) {
            this.value = value;
        }
    }


    @Override
    public boolean equals(Object e){
        if(!(e instanceof Feature))return false;
        Feature<?> s = (Feature) e;
        return s.fieldName.equals(this.fieldName) && s.attachedId.equals(this.attachedId) && s.attachedToClassName.equals(this.attachedToClassName);
   }
}
