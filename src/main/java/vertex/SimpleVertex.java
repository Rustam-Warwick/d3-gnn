package vertex;

import annotations.FeatureAnnotation;
import features.ReplicableMinAggregator;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.factory.Nd4j;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex {

    @FeatureAnnotation(level=0)
    public ReplicableTensorFeature feature = null;
    @FeatureAnnotation(level=0)
    public ReplicableMinAggregator h0agg = null;
    @FeatureAnnotation(level=1)
    public ReplicableTensorFeature h1 = null;
    @FeatureAnnotation(level=1)
    public ReplicableMinAggregator h1agg = null;
    @FeatureAnnotation(level = 2)
    public ReplicableTensorFeature h2 = null;
    @FeatureAnnotation(level = 2)
    public  ReplicableMinAggregator h2agg = null;
    @FeatureAnnotation(level = 3)
    public ReplicableTensorFeature h3 = null;

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
            if(storage.getPart().getLevel()==1){
                this.h1agg = new ReplicableMinAggregator("h1agg",this, Nd4j.zeros(8,8));
                this.h1 = new ReplicableTensorFeature("h1",this,Nd4j.zeros(8,8));
            }
            if(storage.getPart().getLevel()==2){
                this.h2agg = new ReplicableMinAggregator("h2agg",this, Nd4j.zeros(8,8));
                this.h2 = new ReplicableTensorFeature("h2",this,Nd4j.zeros(8,8));
            }
        }
        super.setStorageCallback(storage);

    }

    @Override
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }




}