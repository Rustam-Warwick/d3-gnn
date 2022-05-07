package serializers;

import ai.djl.ndarray.types.LayoutType;
import ai.djl.ndarray.types.Shape;
import ai.djl.util.PairList;

import java.io.Serializable;
import java.util.List;

public class SerializableShape extends Shape implements Serializable {
    public SerializableShape(long... shape) {
        super(shape);
    }

    public SerializableShape(List<Long> shape) {
        super(shape);
    }

    public SerializableShape(PairList<Long, LayoutType> shape) {
        super(shape);
    }

    public SerializableShape(long[] shape, String layout) {
        super(shape, layout);
    }

    public SerializableShape(long[] shape, LayoutType[] layout) {
        super(shape, layout);
    }
}
