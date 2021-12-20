package edge;


import annotations.FeatureAnnotation;
import features.Feature;
import features.ReplicableAggregator;
import features.StaticFeature;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.tensorflow.op.core.Constant;
import org.tensorflow.types.TFloat32;
import org.tensorflow.types.TFloat64;
import storage.GraphStorage;
import types.TFWrapper;
import vertex.BaseVertex;
import vertex.SimpleVertex;


public class SimpleEdge  extends BaseEdge<SimpleVertex>{
    @FeatureAnnotation(level=-1)
    public StaticFeature<TFWrapper> feature = null;

    public SimpleEdge(SimpleVertex source, SimpleVertex destination) {
        super(source, destination);
    }

    public SimpleEdge() {
        super();
    }

    public SimpleEdge(SimpleEdge e){
        super(e);
    }

    /**
     * Call the callback of aggregators if this edge is added to storage
     * @param storage GraphStorage
     */
    @Override
    public void setStorageCallback(GraphStorage storage) {
        super.setStorageCallback(storage);
        this.getStorage().getPart().aggFunctions.forEach(item->{
            item.addEdgeCallback(this);
        });
    }
    public BaseEdge<SimpleVertex> copy() {
        return new SimpleEdge(this);
    }

}
