package partitioner;


import elements.DirectedEdge;
import elements.GraphOp;
import elements.Vertex;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Implementation of Random graph partitioning algorithm.
 * Works for {@link DirectedEdge} and {@link elements.HyperEgoGraph} Streams
 */
public class RandomPartitioner extends Partitioner {

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        return inputDataStream.map(new RandomMapFunction(partitions)).name("Random Partitioner").setParallelism(1);
    }

    @Override
    public boolean isResponsibleFor(String partitionerName) {
        return partitionerName.equals("random");
    }

    public static class RandomMapFunction extends RichMapFunction<GraphOp, GraphOp> {

        public final short partitions;
        public transient Random random;
        public transient Map<String, ShortList> partitionTable;
        public transient int totalNumberOfReplicas;

        public RandomMapFunction(short partitions) {
            this.partitions = partitions;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            random = new Random(0);
            partitionTable = new HashMap<>();
            getRuntimeContext().getMetricGroup().addGroup("partitioner").gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    if (partitionTable.isEmpty()) return 0;
                    return (int) ((float) totalNumberOfReplicas / partitionTable.size() * 1000);
                }
            });
        }

        protected void updateVertexPartitionTableAndAssignMaster(Vertex vertex, short part) {
            partitionTable.compute(vertex.getId(), (key, val) -> {
                if (val == null) {
                    // This is the first part of this vertex hence the master
                    vertex.masterPart = part;
                    return new ShortArrayList(List.of(part));
                } else {
                    if (!val.contains(part)) {
                        // Second or more part hence the replica
                        totalNumberOfReplicas++;
                        val.add(part);
                    }
                    vertex.masterPart = val.getShort(0);
                    return val;
                }
            });
        }

        @Override
        public GraphOp map(GraphOp value) throws Exception {
            short part = (short) random.nextInt(this.partitions);
            switch (value.element.getType()) {
                case EDGE:
                    DirectedEdge directedEdge = (DirectedEdge) value.element;
                    updateVertexPartitionTableAndAssignMaster(directedEdge.getSrc(), part);
                    updateVertexPartitionTableAndAssignMaster(directedEdge.getDest(), part);
                    break;
                case HYPEREDGE:
                    break;
                default:
                    throw new NotImplementedException("Other Element Types are not allowed: Received" + value.element.getType());
            }
            return value.setPartId(part);
        }
    }
}