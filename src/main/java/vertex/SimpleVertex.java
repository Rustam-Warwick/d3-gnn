package vertex;

import annotations.FeatureAnnotation;
import features.ReplicableMinAggregator;
import features.ReplicableTFTensorFeature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.impl.dense.DoubleDenseNdArray;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import storage.GraphStorage;
import types.TFWrapper;

public class SimpleVertex extends BaseVertex {

    @FeatureAnnotation(level=0)
    public ReplicableTFTensorFeature<TFloat32> feature = null;
    @FeatureAnnotation(level=0)
    public ReplicableTFTensorFeature<TFloat32> agg = null;
    @FeatureAnnotation(level=-1)
    public ReplicableTFTensorFeature<TFloat32> hidden = null;

    public SimpleVertex(String id) {
        super(id);
    }
    public SimpleVertex() {
        super();
    }
    public SimpleVertex(SimpleVertex e){
        super(e);
    }

    @Override
    public void setStorageCallback(GraphStorage storage) {
        if(storage!=null){
            hidden = new ReplicableTFTensorFeature("hidden",this);
        }
        super.setStorageCallback(storage);

    }

    @Override
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }




}