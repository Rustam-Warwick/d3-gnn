package vertex;

import features.Feature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex {
    public ReplicableTensorFeature feature = null;
    public ReplicableTensorFeature h1agg = null;
    public ReplicableTensorFeature h1 = null;
//    public ReplicableTensorFeature h2acc = null;
//    public ReplicableTensorFeature h2agg = null;
//    public ReplicableTensorFeature h2 = null;



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
        if(storage.part.L==0){
            // L0 Part added
        }
        if(storage.part.L==1){
            // L1 Part added
            this.feature = null;
            this.h1agg = new ReplicableTensorFeature("h1agg",this,Nd4j.zeros(8,8));
            this.h1 = new ReplicableTensorFeature("h1",this,Nd4j.zeros(8,8));
        }

        super.setStorageCallback(storage);

    }

    @Override
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }

    @Override
    public Feature<INDArray> getFeature(short l) {
        switch (l){
            case 0:
                return this.feature;
            case 1:
                return this.h1;
            default:
                return null;
        }
    }

    @Override
    public Feature<INDArray> getAggregation(short l) {
        switch (l){
            case 1:
                return this.h1agg;
            default:
                return null;
        }
    }


}