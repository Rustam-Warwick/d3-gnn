package vertex;

import features.Feature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex{
    public ReplicableTensorFeature feature = null;
    public ReplicableTensorFeature l1 = null;

    @Override
    public Feature<INDArray> getFeature(short l){
        if(l==0)return this.feature;
        if(l==1)return this.l1;
        return this.feature;
    }

    public SimpleVertex(String id, GraphStorage part) {
        super(id, part);
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
        l1.startTimer(1000,"3");
    }

    @Override
    public BaseVertex copy() {
       return this;
    }
}
