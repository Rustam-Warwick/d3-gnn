package features;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.tensorflow.types.TFloat64;
import types.GraphElement;
import org.tensorflow.ndarray.DoubleNdArray;

public class ReplicableTFTensorFeature extends ReplicableFeature<TFloat64> {


    public ReplicableTFTensorFeature() {
        super();
    }

    public ReplicableTFTensorFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element, TFloat64 value) {
        super(fieldName, element, value);
    }

    @Override
    public void setValue(TFloat64 value) {
        this.editHandler(item->{
            item.value = value;
            return true;
        });
    }
}
