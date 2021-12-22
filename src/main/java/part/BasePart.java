package part;

import aggregator.BaseAggregator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import partitioner.BasePartitioner;
import storage.GraphStorage;
import types.GraphQuery;
import vertex.BaseVertex;

import java.io.IOException;
import java.util.ArrayList;

abstract public class BasePart extends ProcessFunction<GraphQuery,GraphQuery>  {
    private transient GraphStorage storage = null;
    public transient Short partId = null;
    public Short level;
    public transient volatile Collector<GraphQuery> out = null;
    public transient ArrayList<BaseAggregator> aggFunctions = null;

    public BasePart(){
        this.level = 0;
    }

    abstract public GraphStorage newStorage();
    abstract public BaseAggregator[] newAggregators();
    abstract public void dispatch(GraphQuery g);

    /**
     *
     * @param e
     * @param forceNextOperator disallow optimization if true and send anyway. Even if it is going to the same operator
     */
    synchronized public void collect(GraphQuery e,boolean forceNextOperator){
        if(forceNextOperator){
            this.out.collect(e);
        }else{
            if(e.part.equals(this.partId)){
                this.dispatch(e);
            }
            else this.out.collect(e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.aggFunctions = new ArrayList<>();
        this.setPartId((short)this.getRuntimeContext().getIndexOfThisSubtask()); // Set this part id
        this.storage = this.newStorage();
        BaseAggregator[] tmp = this.newAggregators();
        for(BaseAggregator i:tmp){
            this.attachAggregator(i);
        }
    }



    @Override
    public void processElement(GraphQuery value, ProcessFunction<GraphQuery, GraphQuery>.Context ctx, Collector<GraphQuery> out) throws Exception {
        this.out = out;
        this.dispatch(value);
    }


    public BasePart attachAggregator(BaseAggregator e){
        aggFunctions.add(e);
//        e.attachedTo(this);
        return this;
    }

    public void detachAggregator(BaseAggregator e){
        aggFunctions.remove(e);

    }

    public Short getPartId() {
        return partId;
    }

    public GraphStorage getStorage() {
            return this.storage;
    }

    public void setPartId(Short partId) {
        this.partId = partId;
    }

    public void setLevel(Short level) {
        this.level = level;
    }

    public Short getLevel() {
        return level;
    }

    public static DataStream<GraphQuery> partWithIteration(DataStream<GraphQuery> source, BasePart x, FilterFunction<GraphQuery> iterateCondition, FilterFunction<GraphQuery> propegateCondition){
           IterativeStream<GraphQuery> iteration = source.iterate();
//           DataStream<GraphQuery> iteration_partitioned = iteration.partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
           DataStream<GraphQuery> res = iteration.process(x);
           DataStream<GraphQuery> filteredIteration = res.filter(iterateCondition).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
           iteration.closeWith(filteredIteration);
           DataStream<GraphQuery> filteredSend = res.filter(propegateCondition);
           return filteredSend;
    }
}
