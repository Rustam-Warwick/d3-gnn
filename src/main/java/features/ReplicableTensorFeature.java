package features;

import org.nd4j.linalg.api.ndarray.INDArray;
import types.GraphElement;

public class ReplicableTensorFeature extends ReplicableFeature<INDArray> {

    public ReplicableTensorFeature() {
        super();
    }

    public ReplicableTensorFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableTensorFeature(String fieldName, GraphElement element) {
        super(fieldName, element);
    }

    public ReplicableTensorFeature(String fieldName, GraphElement element, INDArray value) {
        super(fieldName, element, value);
    }

}
