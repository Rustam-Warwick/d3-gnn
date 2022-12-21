package ai.djl.nn.hgnn;

import ai.djl.nn.gnn.AggregatorVariant;
import ai.djl.nn.gnn.GNNBlock;

abstract public class HGNNBlock extends GNNBlock {
    AggregatorVariant hyperEdgeAgg = AggregatorVariant.MEAN;

    /**
     * Get the F2 Aggregator Variant
     */
    public AggregatorVariant getHyperEdgeAgg() {
        return hyperEdgeAgg;
    }

    /**
     * Set the F2 Aggregator Variant
     */
    public void setHyperEdgeAgg(AggregatorVariant hyperEdgeAgg) {
        this.hyperEdgeAgg = hyperEdgeAgg;
    }
}
