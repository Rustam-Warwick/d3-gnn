package ai.djl.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.ParallelBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;

import java.util.List;
import java.util.function.Function;

public class SAGEConv extends GNNBlock {

    public SAGEConv(int outFeatures, boolean optBias) {
        // Update Block expecs thisFeature, aggregator
        SequentialBlock updateBlock = new SequentialBlock();
        ParallelBlock updateMidBLock = new ParallelBlock(new Function<List<NDList>, NDList>() {
            @Override
            public NDList apply(List<NDList> item) {
                return new NDList(item.get(0).get(0).add(item.get(1).get(0)));
            }
        });
        updateMidBLock.add(
                new SequentialBlock()
                        .add(new LambdaBlock(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return new NDList(ndArrays.get(0));
                            }
                        }))
                        .add(Linear.builder().setUnits(outFeatures).optBias(optBias).build())
        );
        updateMidBLock.add(
                new SequentialBlock()
                        .add(new LambdaBlock(new Function<NDList, NDList>() {
                            @Override
                            public NDList apply(NDList ndArrays) {
                                return new NDList(ndArrays.get(1));
                            }
                        }))
                        .add(Linear.builder().setUnits(outFeatures).optBias(false).build())
        );
        updateBlock.add(updateMidBLock);
        updateBlock.add(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                NDArray sum = ndArrays.get(0).add(ndArrays.get(1));
                return new NDList(sum);
            }
        });
        // Message block is just a forward
        LambdaBlock messageBlock = new LambdaBlock(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                return ndArrays;
            }
        });

        setAgg(AggregatorVariant.MEAN);
        setMessageBlock(messageBlock);
        setUpdateBlock(updateMidBLock);
    }

}
