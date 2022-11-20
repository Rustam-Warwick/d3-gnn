package partitioner;


import elements.DEdge;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

class RandomPartitioner extends BasePartitioner {
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        return inputDataStream.map(new RandomMapFunction(this.partitions)).name("Random Partitioner").setParallelism(1);
    }

    public static class RandomMapFunction extends RichMapFunction<GraphOp, GraphOp> {
        public final short partitions;
        public Map<String, List<Short>> masters = new ConcurrentHashMap<>(5000);
        public AtomicInteger totalNumberOfVertices = new AtomicInteger(0);
        public AtomicInteger totalNumberOfReplicas = new AtomicInteger(0);

        public RandomMapFunction(short partitions) {
            this.partitions = partitions;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().getMetricGroup().addGroup("partitioner").gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    int totalVertices = totalNumberOfVertices.get();
                    int totalReplicas = totalNumberOfReplicas.get();
                    if (totalVertices == 0) return 0;
                    return (int) ((float) totalReplicas / totalVertices * 1000);
                }
            });

            getRuntimeContext().getMetricGroup().getMetricIdentifier("Replication Factor");
        }

        @Override
        public GraphOp map(GraphOp value) throws Exception {
            if (value.element.elementType() == ElementType.EDGE) {
                value.partId = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
                DEdge dEdge = (DEdge) value.element;
                masters.compute(dEdge.getSrc().getId(), (srcId, val) -> {
                    if (val == null) val = new ArrayList<>();
                    if (!val.contains(value.partId)) {
                        if (val.isEmpty()) totalNumberOfVertices.incrementAndGet();
                        else totalNumberOfReplicas.incrementAndGet();
                        val.add(value.partId);
                    }
                    return val;
                });

                masters.compute(dEdge.getDest().getId(), (destId, val) -> {
                    if (val == null) val = new ArrayList<>();
                    if (!val.contains(value.partId)) {
                        if (val.isEmpty()) totalNumberOfVertices.incrementAndGet();
                        else totalNumberOfReplicas.incrementAndGet();
                        val.add(value.partId);
                    }
                    return val;
                });
                dEdge.getSrc().master = this.masters.get(dEdge.getSrc().getId()).get(0);
                dEdge.getDest().master = this.masters.get(dEdge.getDest().getId()).get(0);
            } else if (value.element.elementType() == ElementType.VERTEX) {
                short part_tmp = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
                masters.compute(value.element.getId(), (srcId, val) -> {
                    if (val == null) val = new ArrayList<>();
                    if (!val.contains(value.partId)) {
                        if (val.isEmpty()) totalNumberOfVertices.incrementAndGet();
                        else totalNumberOfReplicas.incrementAndGet();
                        val.add(value.partId);
                    }
                    return val;
                });
                ((Vertex) value.element).master = this.masters.get(value.element.getId()).get(0);
                value.partId = part_tmp;
            }
            return value;
        }
    }
}