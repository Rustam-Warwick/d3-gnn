package datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import part.SimplePart;
import partitioner.BasePartitioner;
import partitioner.RandomPartitioning;
import types.GraphQuery;
import vertex.SimpleVertex;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

public class GraphStreamBuilder {
    private final StreamExecutionEnvironment env;
    private ConnectedStreams<GraphQuery, GraphQuery> chain;
    private DataStream<GraphQuery> input = null;
    private IterativeStream<GraphQuery> iterationStart=null;
    public enum PARTITIONER {RANDOM};


    public GraphStreamBuilder(SingleOutputStreamOperator<GraphQuery> queries, StreamExecutionEnvironment env){
        this.env = env;
        this.input = queries;
    }

    public StreamExecutionEnvironment getEnvironment() {
        return this.env;
    }

    public DataStream<GraphQuery> build(){
        assert !Objects.isNull(this.iterationStart);
        this.input = this.iterationStart.closeWith(this.input);
        return this.input;
    }
    public GraphStreamBuilder startAccepting(PARTITIONER partitioner) throws ExceptionInChainedOperatorException {
        try {
            SingleOutputStreamOperator<GraphQuery> tmp = (SingleOutputStreamOperator) this.input;
            // 1. First comes Partitioner
            this.iterationStart = tmp.iterate();
            this.input = this.iterationStart.map(new RandomPartitioning()).setParallelism(1).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
            // 2. Then comes Part based on the part assigned
            this.input = this.input.process(new SimplePart<SimpleVertex>());


        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new ExceptionInChainedOperatorException("Salam", new Exception());
        }
        return this;
    }





}
