package edge;


import features.Feature;
import features.StaticFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import storage.GraphStorage;
import vertex.BaseVertex;
import vertex.SimpleVertex;


public class SimpleEdge<VT extends BaseVertex>  extends BaseEdge<VT>{

    public SimpleEdge(VT source, VT destination) {
        super(source, destination);
    }

    public SimpleEdge() {
        super();
    }

    public SimpleEdge(SimpleEdge<VT> e){
        super(e);
    }

    @Override
    public BaseEdge<VT> copy() {
        return new SimpleEdge<VT>(this);
    }

    @Override
    public Feature<INDArray> getFeature(short l) {
        return null;
    }

    @Override
    public Feature<INDArray> getAccumulator(short l) {
        return null;
    }

    @Override
    public Feature<INDArray> getAggegation(short l) {
        return null;
    }

}
