package edge;


import annotations.FeatureAnnotation;
import features.Feature;
import features.ReplicableAggregator;
import features.StaticFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;
import vertex.BaseVertex;
import vertex.SimpleVertex;


public class SimpleEdge  extends BaseEdge<SimpleVertex>{
    @FeatureAnnotation(level=0)
    public StaticFeature<INDArray> feature = null;

    public SimpleEdge(SimpleVertex source, SimpleVertex destination) {
        super(source, destination);
    }

    public SimpleEdge() {
        super();
    }

    public SimpleEdge(SimpleEdge e){
        super(e);
    }

    @Override
    public BaseEdge<SimpleVertex> copy() {
        return new SimpleEdge(this);
    }

}
