package functions.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.ParallelBlock;
import ai.djl.nn.core.Linear;
import ai.djl.training.ParameterStore;

public class SAGEConv extends GNNBlock{
    int outFeatures;

    public SAGEConv(int outFeatures) {
        this.outFeatures = outFeatures;
        ParallelBlock updateBLock = new ParallelBlock(item->(
            new NDList(item.get(0).get(0).add(item.get(1).get(0)))
        ));
        updateBLock.add(Linear.builder().setUnits(outFeatures).optBias(true).build());
        updateBLock.add(Linear.builder().setUnits(outFeatures).optBias(true).build());
        LambdaBlock messageBlock = new LambdaBlock(item->item);
        setAgg(AggregatorVariant.MEAN);
        setMessageBlock(messageBlock);
        setUpdateBlock(updateBLock);
    }

    @Override
    public Shape[] getOutputShapes(Shape[] inputShapes) {
        return getUpdateBlock().getOutputShapes(messageBlock.getOutputShapes(inputShapes));
    }
}
