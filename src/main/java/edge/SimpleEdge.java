package edge;


import features.Feature;
import features.StaticFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import storage.GraphStorage;
import vertex.BaseVertex;
import vertex.SimpleVertex;


public class SimpleEdge<VT extends BaseVertex>  extends BaseEdge<VT>{
    public StaticFeature<INDArray> feature = null;
    public SimpleEdge(String id, GraphStorage part, VT source, VT destination) {
        super(id, part, source, destination);
    }

    public SimpleEdge(String id, VT source, VT destination) {
        super(id, source, destination);
    }

    public SimpleEdge(VT source, VT destination) {
        super(source, destination);
    }

    public SimpleEdge() {
        super();
    }

    @Override
    public void addEdgeCallback() {
        this.feature = new StaticFeature<>("feature",this, Nd4j.zeros(1));
    }

    @Override
    public Feature<INDArray> getFeature(short l) {
        return this.feature;
    }

    @Override
    public Feature<INDArray> getAccumulator(short l) {
        return null;
    }

    @Override
    public Feature<INDArray> getAggegation(short l) {
        return null;
    }

    @Override
    public BaseEdge<VT> copy() {
        VT source = (VT) this.source.copy();
        VT dest = (VT) this.destination.copy();
         return new SimpleEdge<VT>(source,destination);
    }
}
