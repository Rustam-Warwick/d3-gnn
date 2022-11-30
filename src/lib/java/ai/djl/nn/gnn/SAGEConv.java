package ai.djl.nn.gnn;

import ai.djl.ndarray.NDList;
import ai.djl.nn.Bias;
import ai.djl.nn.ParallelBlock;
import ai.djl.nn.SequentialBlock;
import ai.djl.nn.core.Linear;

import java.util.List;
import java.util.function.Function;

public class SAGEConv extends GNNBlock {


    public SAGEConv(int outFeatures, boolean optBias) {
        ParallelBlock updateBlock = new ParallelBlock(new Function<List<NDList>, NDList>() {
            @Override
            public NDList apply(List<NDList> ndLists) {
                return new NDList(ndLists.get(0).get(0).add(ndLists.get(1).get(1)));
            }
        });

        SequentialBlock self = new SequentialBlock();
        self.add(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                return new NDList(ndArrays.get(0));
            }
        });
        self.add(Linear.builder().setUnits(outFeatures).optBias(false).build());
        updateBlock.add(self);
        updateBlock.add(new Function<NDList, NDList>() {
            @Override
            public NDList apply(NDList ndArrays) {
                return ndArrays;
            }
        });

        SequentialBlock updateFinalBlock = new SequentialBlock().add(updateBlock);
        if (optBias) {
            updateFinalBlock.add(new Bias());
        }

        setUpdateBlock(updateFinalBlock);
        setMessageBlock(Linear.builder().setUnits(outFeatures).optBias(false).build());


        setAgg(AggregatorVariant.MEAN);
    }

}
