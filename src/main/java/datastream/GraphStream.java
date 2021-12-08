package datastream;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import part.BasePart;
import part.GNNPart;
import partitioner.BasePartitioner;
import partitioner.RandomPartitioning;
import types.GraphQuery;
import vertex.SimpleVertex;

import javax.xml.crypto.Data;
import java.util.Objects;

public class GraphStream {
    private final StreamExecutionEnvironment env;
    public final DataStream<GraphQuery> input;

    public GraphStream(DataStream<GraphQuery> input, StreamExecutionEnvironment env){
        this.env = env;
        this.input = input;
    }


    public GraphStream partitionBy(BasePartitioner e){
        if(e.isParallel()){
            DataStream<GraphQuery> tmp = input.map(e).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
            return new GraphStream(tmp,this.env);
        }
        else{
            DataStream<GraphQuery>tmp= input.map(e).setParallelism(1).map(item->item).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
            return new GraphStream(tmp,this.env);
        }
    }

    /**
     * Add l level aggregation in one operator
     * @param l depth of GNN aggregation
     * @return
     */
    public GraphStream addGNN(int l){
        GNNPart s = new GNNPart();
        s.setLevel((short)l);
        DataStream<GraphQuery> next = BasePart.partWithIteration(this.input,s,item->item.op== GraphQuery.OPERATORS.SYNC,item->item.op!= GraphQuery.OPERATORS.SYNC);
        return new GraphStream(next.partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor()),this.env);
    }

    public StreamExecutionEnvironment getEnvironment() {
        return this.env;
    }
}
