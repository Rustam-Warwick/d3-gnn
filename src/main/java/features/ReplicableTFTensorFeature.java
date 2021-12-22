package features;

import org.bytedeco.javacpp.annotation.Const;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.tensorflow.op.core.Constant;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import org.tensorflow.types.family.TType;
import types.GraphElement;
import org.tensorflow.ndarray.DoubleNdArray;
import types.TFWrapper;

public class ReplicableTFTensorFeature<T extends TType> extends ReplicableFeature<TFWrapper<T>> {


    public ReplicableTFTensorFeature() {
        super();
    }

    public ReplicableTFTensorFeature(String fieldName) {
        super(fieldName);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element) {
        super(fieldName, element,null);
    }

    public ReplicableTFTensorFeature(String fieldName, GraphElement element, TFWrapper<T> value) {
        super(fieldName, element, value);
    }

    @Override
    public void setValue(TFWrapper<T> value) {
        this.editHandler(item->{
            item.value = value;
            return true;
        });
    }
}
