package vertex;

import features.ReplicableTensorFeature;
import org.nd4j.linalg.factory.Nd4j;
import storage.GraphStorage;

public class SimpleVertex extends BaseVertex{
    public ReplicableTensorFeature feature;

    @Override
    public void addVertexCallback() {
        super.addVertexCallback();
        this.feature = new ReplicableTensorFeature("feature",this,Nd4j.create(new float[]{1,2,3,4},2,2));
        this.feature.startTimer("3");
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
    public BaseVertex copy() {
       return this;
    }
}
