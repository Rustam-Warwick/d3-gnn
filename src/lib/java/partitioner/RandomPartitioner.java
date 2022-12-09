package partitioner;


import elements.DirectedEdge;
import elements.GraphOp;
import elements.HyperEdge;
import elements.HyperEgoGraph;
import elements.enums.ElementType;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of Random graph partitioning algorithm.
 * Works for {@link DirectedEdge} and {@link elements.HyperEgoGraph} Streams
 */
class RandomPartitioner extends Partitioner {

    public RandomPartitioner(String[] cmdArgs) {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        return inputDataStream.map(new RandomMapFunction(this.partitions)).name("Random Partitioner").setParallelism(1);
    }

    /**
     * Actual Random graph partitioning process function
     */
    public static class RandomMapFunction extends RichMapFunction<GraphOp, GraphOp> {

        public final short partitions;

        public Map<String, List<Short>> vertexPartitionTable = new ConcurrentHashMap<>();

        public Map<String, List<Short>> hyperEdgePartitionTable = new ConcurrentHashMap<>();

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
            short part = (short) ThreadLocalRandom.current().nextInt(0, this.partitions);
            if (value.element.getType() == ElementType.EDGE) {
                DirectedEdge directedEdge = (DirectedEdge) value.element;
                vertexPartitionTable.compute(directedEdge.getSrcId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        directedEdge.getSrc().masterPart = part;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(part)));
                    } else {
                        if (!val.contains(part)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(part);
                        }
                        directedEdge.getSrc().masterPart = val.get(0);
                        return val;
                    }
                });
                vertexPartitionTable.compute(directedEdge.getDestId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        directedEdge.getDest().masterPart = part;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(part)));
                    } else {
                        if (!val.contains(part)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(part);
                        }
                        directedEdge.getDest().masterPart = val.get(0);
                        return val;
                    }
                });
            } else if (value.element.getType() == ElementType.GRAPH) {
                HyperEgoGraph hyperEgoGraph = (HyperEgoGraph) value.element;
                vertexPartitionTable.compute(hyperEgoGraph.getCentralVertex().getId(), (key, val) -> {
                    if (val == null) {
                        hyperEgoGraph.getCentralVertex().masterPart = part;
                        return new ArrayList<>(List.of(part));
                    } else {
                        if (!val.contains(part)) {
                            val.add(part);
                        }
                        hyperEgoGraph.getCentralVertex().masterPart = val.get(0);
                        return val;
                    }
                });
                for (HyperEdge hyperEdge : hyperEgoGraph.getHyperEdges()) {
                    hyperEdgePartitionTable.compute(hyperEdge.getId(), (key, val) -> {
                        if (val == null) {
                            // This is the first part of this vertex hence the master
                            hyperEdge.masterPart = part;
                            totalNumberOfVertices.incrementAndGet();
                            return Collections.synchronizedList(new ArrayList<Short>(List.of(part)));
                        } else {
                            if (!val.contains(part)) {
                                // Seocond or more part hence the replica
                                totalNumberOfReplicas.incrementAndGet();
                                val.add(part);
                            }
                            hyperEdge.masterPart = val.get(0);
                            return val;
                        }
                    });
                }
            }
            return value.setPartId(part);
        }
    }
}