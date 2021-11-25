package features;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import types.GraphElement;

public class ReplicableTensorFeature extends ReplicableFeature<INDArray> {


    public ReplicableTensorFeature() {
        super();
    }

    public ReplicableTensorFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableTensorFeature(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }

    public ReplicableTensorFeature(String fieldName, GraphElement element, INDArray value) {
        super(fieldName, element, value);
    }

    @Override
    public void setValue(INDArray value) {
        this.editHandler(item->{
            item.value = value;
            return true;
        });
    }
}
