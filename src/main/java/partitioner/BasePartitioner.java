package partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import types.GraphQuery;

abstract public class BasePartitioner extends RichMapFunction<GraphQuery, GraphQuery> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    public static class PartExtractPartitioner implements Partitioner<Short>{

        @Override
        public int partition(Short key, int numPartitions) {
            return key;
        }
    }

    public static DataStream<GraphQuery> partitionHelper(DataStream<GraphQuery> inputStream,BasePartitioner basePartitioner){
        return inputStream.map(basePartitioner).setParallelism(1).map(item->item).partitionCustom(new BasePartitioner.PartExtractPartitioner(),new BasePartitioner.PartKeyExtractor());
    }

    public static class PartKeyExtractor implements KeySelector<GraphQuery,Short>{
        @Override
        public Short getKey(GraphQuery value) throws Exception {
            return value.part;
        }
    }

}
