package types;

import org.tensorflow.Tensor;
import org.tensorflow.types.family.TType;

public class TFWrapper {
    public TType value;
    public TFWrapper(){
        this.value = null;

    }
    public TFWrapper(TType e){
        this.value = e;
    }

    public TType getValue() {
        return value;
    }

    public void setValue(TType value) {
        this.value = value;
    }
}
