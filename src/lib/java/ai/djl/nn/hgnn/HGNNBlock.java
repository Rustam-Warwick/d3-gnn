package ai.djl.nn.hgnn;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.AbstractBlock;
import ai.djl.nn.Block;
import ai.djl.nn.gnn.AggregatorVariant;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;

abstract public class HGNNBlock extends GNNBlock {
    AggregatorVariant hyperEdgeAgg = AggregatorVariant.MEAN;

    /**
     * Set the F2 Aggregator Variant
     */
    public void setHyperEdgeAgg(AggregatorVariant hyperEdgeAgg) {
        this.hyperEdgeAgg = hyperEdgeAgg;
    }

    /**
     * Get the F2 Aggregator Variant
     */
    public AggregatorVariant getHyperEdgeAgg() {
        return hyperEdgeAgg;
    }
}
