package types;

import features.Feature;
import features.ReplicableAggregator;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.util.concurrent.CompletableFuture;

public interface IncrementalAggregatable {
     Feature<INDArray> getFeature(short l);
     ReplicableAggregator<INDArray> getAggregation(short l);
}
