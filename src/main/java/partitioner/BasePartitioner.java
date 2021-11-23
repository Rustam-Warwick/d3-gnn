package partitioner;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import types.GraphQuery;

abstract public class BasePartitioner extends RichMapFunction<GraphQuery, GraphQuery> {




    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    public static class PartExtractPartitioner implements Partitioner<Short>{
        @Override
        public int partition(Short key, int numPartitions) {
            return (int)key;
        }
    }
    public static class PartKeyExtractor implements KeySelector<GraphQuery,Short>{
        @Override
        public Short getKey(GraphQuery value) throws Exception {
            return value.part;
        }
    }
}
