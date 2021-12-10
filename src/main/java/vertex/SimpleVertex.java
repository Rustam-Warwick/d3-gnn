package vertex;

import annotations.FeatureAnnotation;
import features.ReplicableMinAggregator;
import features.ReplicableTFTensorFeature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.factory.Nd4j;
import org.tensorflow.ndarray.Shape;
import org.tensorflow.ndarray.impl.dense.DoubleDenseNdArray;
import org.tensorflow.types.TFloat64;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex {

    @FeatureAnnotation(level=0)
    public ReplicableTFTensorFeature feature = null;
    @FeatureAnnotation(level=0)
    public ReplicableTFTensorFeature h0agg = null;
    @FeatureAnnotation(level=1)
    public ReplicableTFTensorFeature h1 = null;
    @FeatureAnnotation(level=1)
    public ReplicableTFTensorFeature h1agg = null;
    @FeatureAnnotation(level = 2)
    public ReplicableTFTensorFeature h2 = null;
    @FeatureAnnotation(level = 2)
    public  ReplicableTFTensorFeature h2agg = null;
    @FeatureAnnotation(level = 3)
    public ReplicableTFTensorFeature h3 = null;

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
//            if(storage.getPart().getLevel()==1){
//                this.h1agg = new ReplicableTFTensorFeature("h1agg",this, TFloat64.tensorOf(Shape.of(8,8)));
//                this.h1 = new ReplicableTFTensorFeature("h1",this,TFloat64.tensorOf(Shape.of(8,8)));
//            }
//            if(storage.getPart().getLevel()==2){
//                this.h2agg = new ReplicableTFTensorFeature("h2agg",this,TFloat64.tensorOf(Shape.of(8,8)));
//                this.h2 = new ReplicableTFTensorFeature("h2",this,TFloat64.tensorOf(Shape.of(8,8)));
//            }
        }
        super.setStorageCallback(storage);

    }

    @Override
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }




}