package ai.djl.nn.gnn;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.Activation;
import ai.djl.nn.LambdaBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;

import java.util.function.Function;

public class SAGEConv extends GNNBlock {

    public SAGEConv(int outFeatures, boolean optBias) {
        // Update Block expecs thisFeature, aggregator
        SequentialBlock updateBlock = new SequentialBlock();
        updateBlock.add(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                return new NDList(ndArrays.get(0).concat(ndArrays.get(1), -1));
            }
        });
        updateBlock.add(Linear.builder().setUnits(outFeatures).optBias(optBias).build());
        updateBlock.add(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                NDArray tmp = Activation.softPlus(ndArrays).get(0);
                return new NDList(tmp);
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
        setUpdateBlock(updateBlock);
    }

}
