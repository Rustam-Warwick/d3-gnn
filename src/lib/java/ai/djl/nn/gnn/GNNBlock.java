package ai.djl.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.nn.AbstractBlock;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;

/**
 * Represents a Single GNN Block
 */
public abstract class GNNBlock extends AbstractBlock {

    AggregatorVariant agg = AggregatorVariant.MEAN;

    public GNNBlock() {

    }

    /**
     * Get the aggregator type for this GNN
     */
    public AggregatorVariant getAgg() {
        return agg;
    }

    /**
     * Set the Aggregator Type for this GNN
     */
    public void setAgg(AggregatorVariant agg) {
        this.agg = agg;
    }

    /**
     * Message can have any input
     */
    public abstract NDList message(ParameterStore parameterStore, NDList inputs, boolean training);

    /**
     * Input should have format of [embedding, aggregator]
     */
    public abstract NDList update(ParameterStore parameterStore, NDList inputs, boolean training);

    @Override
    protected NDList forwardInternal(ParameterStore parameterStore, NDList inputs, boolean training, PairList<String, Object> params) {
        throw new IllegalStateException("GNNBlocks should not be called as whole, use output and update functions separately");
    }
}
