package aggregator.GNNAggregator;

import org.nd4j.linalg.api.ndarray.INDArray;
import scala.Tuple2;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MyFirstGNNAggregator<VT extends BaseVertex> extends BaseGNNAggregator<VT>{

    @Override
    public INDArray MESSAGE(INDArray sourceV, INDArray destV, INDArray edge, short l) {
       return sourceV.add(destV);
    }

    @Override
    public INDArray ACCUMULATE(INDArray m1, INDArray m2, AtomicInteger accumulator) {
        return m1.add(m2);
    }

    @Override
    public INDArray COMBINER(ArrayList<Tuple2<INDArray, Integer>> accumulations) {
        return accumulations.get(0)._1;
    }

    @Override
    public boolean stopPropagation(Short lNow, VT vertex) {
        return false;
    }

    @Override
    public INDArray UPDATE(INDArray featureNow, INDArray aggregations) {
       return aggregations;
    }
}
