package aggregator.GNNAggregator;

import org.nd4j.linalg.api.ndarray.INDArray;

public class GNNQuery {

    public enum OPERATORS {NONE,REQUEST,RESPONSE}
    public String vertexId = null;
    public Short responsePart = null;
    public Short l = 1;
    public OPERATORS op = OPERATORS.NONE;
    public INDArray agg = null;
    public Integer accumulator = null;
    public String uuid = null;

    public GNNQuery withId(String id){
        uuid = id;
        return this;
    }
    public GNNQuery withResponsePart(Short id){
        this.responsePart = id;
        return this;
    }

    public GNNQuery withAggValue(INDArray a){
        this.agg = a;
        return this;
    }
    public GNNQuery withAccumulator(Integer a){
        this.accumulator = a;
        return this;
    }
    public GNNQuery withOperator(OPERATORS op){
        this.op = op;
        return this;
    }

    public GNNQuery withLValue(short L){
        this.l = L;
        return this;
    }

    public GNNQuery withVertex(String v){
        this.vertexId = v;
        return this;
    }
}
