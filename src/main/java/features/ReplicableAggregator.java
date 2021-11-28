package features;


import org.nd4j.linalg.api.ndarray.INDArray;
import types.GraphElement;

abstract public class ReplicableAggregator<T> extends ReplicableFeature<T> {

    public ReplicableAggregator() {
        super();
    }

    public ReplicableAggregator(String fieldName) {
        super(fieldName);
    }

    public ReplicableAggregator(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }
    public ReplicableAggregator(String fieldName, GraphElement element, T value) {
        super(fieldName, element, value);
    }

    abstract public void addNewElement(T e);
    abstract public void updateElements(T ...e);
}
