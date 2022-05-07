package functions.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.AbstractBlock;
import ai.djl.nn.Block;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;
import scala.xml.PrettyPrinter;

/**
 * Represents a Single GNN Block
 */
abstract public class GNNBlock extends AbstractBlock {
    AggregatorVariant agg;
    Block messageBlock;
    Block updateBlock;


    public AggregatorVariant getAgg() {
        return agg;
    }

    public void setAgg(AggregatorVariant agg) {
        this.agg = agg;
    }

    public Block getMessageBlock() {
        return messageBlock;
    }

    public void setMessageBlock(Block messageBlock) {
        this.messageBlock = messageBlock;
    }

    public Block getUpdateBlock() {
        return updateBlock;
    }

    public void setUpdateBlock(Block updateBlock) {
        this.updateBlock = updateBlock;
    }

    @Override
    protected NDList forwardInternal(ParameterStore parameterStore, NDList inputs, boolean training, PairList<String, Object> params) {
        throw new IllegalStateException("GNN Blocks are not used separately yet");
    }

    @Override
    protected void initializeChildBlocks(NDManager manager, DataType dataType, Shape... inputShapes) {
        super.initializeChildBlocks(manager, dataType, inputShapes);
        messageBlock.initialize(manager, dataType, inputShapes);
        Shape[] messageShapes = messageBlock.getOutputShapes(inputShapes);
        updateBlock.initialize(manager, dataType, messageShapes);
    }
}
