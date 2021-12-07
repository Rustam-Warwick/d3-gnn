package vertex;

import annotations.FeatureAnnotation;
import features.Feature;
import features.ReplicableAggregator;
import features.ReplicableMinAggregator;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;

public class SimpleVertex extends BaseVertex {

    @FeatureAnnotation(level=0)
    public ReplicableTensorFeature feature = null;
    @FeatureAnnotation(level=0)
    public ReplicableMinAggregator h0agg = null;
    @FeatureAnnotation(level=1)
    public ReplicableTensorFeature h1 = null;
    @FeatureAnnotation(level=1)
    public ReplicableMinAggregator h1agg = null;

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
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }




}