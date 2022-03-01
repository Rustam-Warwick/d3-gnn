package helpers;

import elements.GraphOp;
import functions.GraphProcessFn;
import iterations.IterationState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.BasePartitioner;

import java.util.Objects;

public class GraphStream {
    public short parallelism;
    public short layers;
    public short position_index = 1;
    public StreamExecutionEnvironment env;
    private DataStream last = null;
    private IterativeStream iterator = null;

    public GraphStream(short parallelism, short layers){
        this.parallelism = parallelism;
        this.layers = layers;
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(this.parallelism);
    }

    public DataStream<GraphOp> readSocket(MapFunction<String, GraphOp> parser, String host, int port){
        this.last = this.env.socketTextStream(host, port).map(parser).name("Input Stream Parser");
        return this.last;
    }

    public DataStream<GraphOp> partition(BasePartitioner partitioner){
        partitioner.partitions = this.parallelism;
        short part_parallelism = this.parallelism;
        if(!partitioner.isParallel())part_parallelism = 1;
        this.last = this.last.map(partitioner).setParallelism(part_parallelism).name("Partitioner");
        return this.last;
    }

    public DataStream<GraphOp> gnnLayer(GraphProcessFn storageProcess){
        storageProcess.layers = this.layers;
        storageProcess.position = this.position_index;
        IterativeStream<GraphOp> iterator = this.last.iterate();
        KeyedStream<GraphOp, Short> ks = iterator.keyBy(item->item.part_id);
        DataStream<GraphOp> res = ks.process(storageProcess).name("Gnn Process");
        DataStream<GraphOp> iterateFilter = res.filter(item->item.state == IterationState.ITERATE);
        DataStream<GraphOp> forwardFilter = res.filter(item->item.state == IterationState.FORWARD);
        if(Objects.nonNull(this.iterator)){
            DataStream<GraphOp> backFilter = res.filter(item->item.state == IterationState.BACKWARD);
            this.iterator.closeWith(backFilter);
        }
        iterator.closeWith(iterateFilter);
        this.last = forwardFilter;
        this.iterator = iterator;
        this.position_index++;
        return this.last;
    }
}
