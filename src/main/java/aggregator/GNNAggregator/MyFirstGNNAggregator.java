package aggregator.GNNAggregator;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import scala.Tuple2;
import vertex.BaseVertex;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class MyFirstGNNAggregator<VT extends BaseVertex> extends BaseGNNAggregator<VT>{

    @Override
    public INDArray MESSAGE(INDArray sourceV, INDArray destV, INDArray edge, short l) {
       return Nd4j.concat(0,sourceV,destV,edge);
    }

    @Override
    public INDArray ACCUMULATE(INDArray m1, INDArray m2, AtomicInteger accumulator) {
        accumulator.incrementAndGet();
        return m1.add(m2);
    }

    @Override
    public INDArray COMBINER(ArrayList<Tuple2<INDArray, Integer>> accumulations) {

        INDArray[] res = new INDArray[accumulations.size()];
        int i=0;
        for(Tuple2<INDArray,Integer> x:accumulations){
            res[i] = x._1;
            i++;
        }
        return Nd4j.accumulate(res).div(accumulations.size());
    }

    @Override
    public boolean stopPropagation(Short lNow, VT vertex) {
        return false;
    }

    @Override
    public INDArray UPDATE(INDArray featureNow, INDArray aggregations) {
        return featureNow.add(aggregations).div(2);
    }
}
