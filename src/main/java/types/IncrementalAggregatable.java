package types;

import features.Feature;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.util.concurrent.CompletableFuture;

public interface IncrementalAggregatable {
    public Feature<INDArray> getFeature(short l);
    public Feature<INDArray> getAccumulator(short l);
    public Feature<INDArray> getAggegation(short l);
//    public Feature<?> getAccumulator();

}
