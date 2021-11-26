package vertex;

import features.Feature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex{
    public ReplicableTensorFeature feature = null;
    public ReplicableTensorFeature h1acc = null;
    public ReplicableTensorFeature h1agg = null;
    public ReplicableTensorFeature h1 = null;
    public ReplicableTensorFeature h2acc = null;
    public ReplicableTensorFeature h2agg = null;
    public ReplicableTensorFeature h2 = null;

    @Override
    public Feature<INDArray> getFeature(short l){
        switch (l){
            case 0:
                return this.feature;
            case 1:
                return this.h1;
            case 2:
                return this.h2;
            default:
                return null;
        }
    }

    @Override
    public Feature<INDArray> getAccumulator(short l) {
        switch (l){
            case 1:
                return this.h1acc;
            case 2:
                return this.h2acc;
            default:
                return null;
        }
    }

    @Override
    public Feature<INDArray> getAggegation(short l) {
        switch (l){
            case 1:
                return this.h1agg;
            case 2:
                return this.h2agg;
            default:
                return null;
        }
    }


    public SimpleVertex(String id, GraphStorage storage) {
        super(id, storage);
    }

    public SimpleVertex(String id) {
        super(id);
    }

    public SimpleVertex() {
        super();
    }

    @Override
    public void addVertexCallback() {
        super.addVertexCallback();
        this.parts.startTimer(1000,"3","253195","4791");
    }

    @Override
    public BaseVertex copy() {
       SimpleVertex a = new SimpleVertex(this.getId(),this.getStorage());
       return a;
    }
}
