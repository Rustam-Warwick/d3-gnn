package ai.djl.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.AbstractBlock;
import ai.djl.nn.Block;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;

public class HGNNBlock extends AbstractBlock {

    AggregatorVariant agg = AggregatorVariant.MEAN;
    AggregatorVariant hyperGraphAgg = AggregatorVariant.MEAN;
    Block messageBlock = null;
    Block updateBlock = null;

    public HGNNBlock() {

    }

    public AggregatorVariant getAgg() {
        return agg;
    }

    public void setAgg(AggregatorVariant agg) {
        this.agg = agg;
    }

    public AggregatorVariant getHyperGraphAgg() {
        return hyperGraphAgg;
    }

    public void setHyperGraphAgg(AggregatorVariant hyperGraphAgg) {
        this.hyperGraphAgg = hyperGraphAgg;
    }

    public Block getMessageBlock() {
        return messageBlock;
    }

    public void setMessageBlock(Block messageBlock) {
        this.messageBlock = messageBlock;
        addChildBlock("message", messageBlock);
    }

    public Block getUpdateBlock() {
        return updateBlock;
    }

    public void setUpdateBlock(Block updateBlock) {
        this.updateBlock = updateBlock;
        addChildBlock("update", updateBlock);
    }

    @Override
    protected NDList forwardInternal(ParameterStore parameterStore, NDList inputs, boolean training, PairList<String, Object> params) {
        throw new IllegalStateException("GNN Blocks are not used  yet instead use the message and update blocks separately");
    }

    @Override
    protected void initializeChildBlocks(NDManager manager, DataType dataType, Shape... inputShapes) {
        messageBlock.initialize(manager, dataType, inputShapes);
        Shape[] messageShapes = messageBlock.getOutputShapes(inputShapes);
        updateBlock.initialize(manager, dataType, messageShapes[0], messageShapes[0]); // One is aggregator one is message shape
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        Shape[] messsageShapes = getMessageBlock().getOutputShapes(inputShapes);
        Shape[] shape = new Shape[]{messsageShapes[0], messsageShapes[0]};
        return getUpdateBlock().getOutputShapes(shape);
    }
}
