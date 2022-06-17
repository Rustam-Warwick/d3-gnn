package partitioner;


import elements.Edge;
import elements.ElementType;
import elements.GraphOp;
import elements.Vertex;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

class RandomPartitioner extends BasePartitioner {
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        return inputDataStream.map(new RandomMapFunction(this.partitions)).setParallelism(1);
    }

    @Override
    public void parseCmdArgs(String[] cmdArgs) {

    }

    @Override
    public String getName() {
        return "Random-Partitioner";
    }

    public static class RandomMapFunction extends RichMapFunction<GraphOp, GraphOp> {
        public final short partitions;
        public Map<String, Short> masters = new ConcurrentHashMap<>(5000);

        public RandomMapFunction(short partitions) {
            this.partitions = partitions;
        }

        @Override
        public GraphOp map(GraphOp value) throws Exception {
            if (value.element.elementType() == ElementType.EDGE) {
                value.partId = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
                Edge edge = (Edge) value.element;
                this.masters.putIfAbsent(edge.src.getId(), value.partId);
                this.masters.putIfAbsent(edge.dest.getId(), value.partId);
                edge.src.master = this.masters.get(edge.src.getId());
                edge.dest.master = this.masters.get(edge.dest.getId());
            } else if (value.element.elementType() == ElementType.VERTEX) {
                short part_tmp = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
                this.masters.putIfAbsent(value.element.getId(), part_tmp);
                ((Vertex) value.element).master = this.masters.get(value.element.getId());
                value.partId = part_tmp;
            }
            return value;
        }
    }
}