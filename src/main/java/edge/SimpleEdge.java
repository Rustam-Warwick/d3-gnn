package edge;


import features.Feature;
import features.StaticFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import storage.GraphStorage;
import vertex.BaseVertex;


public class SimpleEdge<VT extends BaseVertex>  extends BaseEdge<VT>{

    public StaticFeature<INDArray> feature = null;

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
        return this.feature;
    }

    @Override
    public void setStorageCallback(GraphStorage storage) {
        if(storage.part.L==0){

        }
        if(storage.part.L==1){
//            this.feature = null;
        }
        super.setStorageCallback(storage);
    }

    @Override
    public Feature<INDArray> getAggregation(short l) {
        return null;
    }
}
