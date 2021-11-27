package vertex;

import features.Feature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;

import java.util.Objects;

public class SimpleVertex extends BaseVertex {
    public ReplicableTensorFeature feature = null;
//    public ReplicableTensorFeature h1acc = null;
//    public ReplicableTensorFeature h1agg = null;
//    public ReplicableTensorFeature h1 = null;
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
        super.setStorageCallback(storage);
        if(!Objects.isNull(this.feature))this.feature.sync();
        this.feature.startTimer(1000,"3","260805","43287");
    }

    @Override
    public BaseVertex copy() {
        return new SimpleVertex(this);
    }

    @Override
    public Feature<INDArray> getFeature(short l) {
        return null;
//        switch (l){
//            case 0:
//                return this.feature;
//            case 1:
//                return this.h1;
//            case 2:
//                return this.h2;
//            default:
//                return null;
//        }
    }

    @Override
    public Feature<INDArray> getAccumulator(short l) {
        return null;
//        switch (l){
//            case 1:
//                return this.h1acc;
//            case 2:
//                return this.h2acc;
//            default:
//                return null;
//        }
    }

    @Override
    public Feature<INDArray> getAggegation(short l) {
        return null;
//        switch (l){
//            case 1:
//                return this.h1agg;
//            case 2:
//                return this.h2agg;
//            default:
//                return null;
//        }
    }


}