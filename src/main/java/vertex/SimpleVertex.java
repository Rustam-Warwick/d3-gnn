package vertex;

import features.Feature;
import features.ReplicableTensorFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex{

    @Override
    public Feature<INDArray> getFeature(short l){
        return null;
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
        this.parts.startTimer(1000,"3","253195","4791");
    }

    @Override
    public BaseVertex copy() {
       return this;
    }
}
