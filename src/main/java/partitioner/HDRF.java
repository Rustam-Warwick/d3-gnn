package partitioner;

import elements.DEdge;
import elements.GraphElement;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import operators.MultiThreadedProcessOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class HDRF extends BasePartitioner {

    public final float epsilon = 8; // Leave it as is, used to not have division by zero errors

    public float lambda = 1f; // More means more balance constraint comes into play

    /**
     * {@inheritDoc}
     */
    @Override
    public BasePartitioner parseCmdArgs(String[] cmdArgs) {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        int numThreats = 3;
        return inputDataStream.transform(String.format("%s-%sThreads", "HDRF", numThreats),
                TypeInformation.of(GraphOp.class),
                new MultiThreadedProcessOperator<>(new HDRFProcessFunction(partitions, lambda, epsilon), numThreats)).uid(String.format("%s-%sThreads", "HDRF", numThreats)).setParallelism(1);
    }

    public static class HDRFProcessFunction extends ProcessFunction<GraphOp, GraphOp> {
        public final short numPartitions;
        public final float lamb;
        public final float eps;
        public ConcurrentHashMap<String, Integer> partialDegTable = new ConcurrentHashMap<>();
        public ConcurrentHashMap<String, List<Short>> partitionTable = new ConcurrentHashMap<>();
        public ConcurrentHashMap<Short, Integer> partitionsSize = new ConcurrentHashMap<>();

        public Set<String> currentlyProcessing = Collections.synchronizedSet(new HashSet<String>());
        public AtomicInteger maxSize = new AtomicInteger(0);
        public AtomicInteger minSize = new AtomicInteger(0);
        // Metrics proprs
        public AtomicInteger totalNumberOfVertices = new AtomicInteger(0);
        public AtomicInteger totalNumberOfReplicas = new AtomicInteger(0);

        public HDRFProcessFunction(short numPartitions, float lambda, float eps) {
            this.numPartitions = numPartitions;
            this.lamb = lambda;
            this.eps = eps;

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

        public float G(String vertexId, float normalDeg, short partition) {
            if (partitionTable.getOrDefault(vertexId, Collections.emptyList()).contains(partition)) {
                return 2 - normalDeg; // 1 + (1 - \theta)
            } else return 0;
        }

        public float REP(DEdge dEdge, short partition) {
            int srcDeg = partialDegTable.get(dEdge.getSrc().getId());
            int destDeg = partialDegTable.get(dEdge.getDest().getId());
            float srcDegNormal = (float) srcDeg / (srcDeg + destDeg);
            float destDegNormal = 1 - srcDegNormal;
            return this.G(dEdge.getSrc().getId(), srcDegNormal, partition) + this.G(dEdge.getDest().getId(), destDegNormal, partition);
        }

        public float BAL(short partition) {
            float res = (float) (maxSize.get() - this.partitionsSize.getOrDefault(partition, 0)) / (eps + maxSize.get() - minSize.get());
            return lamb * res;
        }

        public short computePartition(DEdge dEdge) {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(dEdge.getSrc().getId(), 1, Integer::sum);
            partialDegTable.merge(dEdge.getDest().getId(), 1, Integer::sum);

            // 2. Calculate the partition
            float maxScore = Float.NEGATIVE_INFINITY;
            List<Short> tmp = new ArrayList<>();
            for (short i = 0; i < this.numPartitions; i++) {
                float score = REP(dEdge, i) + BAL(i);
                if (score > maxScore) {
                    tmp.clear();
                    maxScore = score;
                    tmp.add(i);
                } else if (score == maxScore) tmp.add(i);
            }

            final short finalSelected = tmp.get(ThreadLocalRandom.current().nextInt(tmp.size()));

            // 3. Update the tables
            int newSizeOfPartition = partitionsSize.merge(finalSelected, 1, Integer::sum);

            maxSize.set(Math.max(maxSize.get(), newSizeOfPartition));
            minSize.set(partitionsSize.reduceValues(Long.MAX_VALUE, Math::min));
            partitionTable.compute(dEdge.getSrc().getId(), (key, val) -> {
                if (val == null) {
                    // This is the first part of this vertex hence the master
                    totalNumberOfVertices.incrementAndGet();
                    return Collections.synchronizedList(new ArrayList<Short>(List.of(finalSelected)));
                } else {
                    if (!val.contains(finalSelected)) {
                        // Seocond or more part hence the replica
                        totalNumberOfReplicas.incrementAndGet();
                        val.add(finalSelected);
                    }
                    return val;
                }
            });
            // Same as previous
            partitionTable.compute(dEdge.getDest().getId(), (key, val) -> {
                if (val == null) {
                    totalNumberOfVertices.incrementAndGet();
                    return Collections.synchronizedList(new ArrayList<Short>(List.of(finalSelected)));
                } else {
                    if (!val.contains(finalSelected)) {
                        totalNumberOfReplicas.incrementAndGet();
                        val.add(finalSelected);
                    }
                    return val;
                }
            });

            return finalSelected;
        }

        public short computePartition(Vertex vertex) {
            // Initialize the tables if absent
            partitionTable.putIfAbsent(vertex.getId(), new ArrayList<>());
            if (partitionTable.get(vertex.getId()).isEmpty()) {
                // If no previously assigned partition for this vertex
                float maxScore = Float.NEGATIVE_INFINITY;
                short selected = 0;
                for (short i = 0; i < this.numPartitions; i++) {
                    float score = BAL(i);
                    if (score > maxScore) {
                        maxScore = score;
                        selected = i;
                    }
                }
                this.partitionTable.get(vertex.getId()).add(selected);
                return selected;
            } else {
                // If already assigned use that as master
                return partitionTable.get(vertex.getId()).get(0);
            }

        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            GraphElement elementToPartition = value.element;
            if (elementToPartition.elementType() == ElementType.EDGE) {
                // Main partitioning logic, otherwise just assign edges
                DEdge dEdge = (DEdge) elementToPartition;
//                while (currentlyProcessing.contains(edge.src.getId()) || currentlyProcessing.contains(edge.dest.getId())) {
//                    // Wait for completion
//                }
//                currentlyProcessing.add(edge.src.getId());
//                currentlyProcessing.add(edge.dest.getId());
                short partition = this.computePartition(dEdge);
                dEdge.getSrc().master = this.partitionTable.get(dEdge.getSrc().getId()).get(0);
                dEdge.getDest().master = this.partitionTable.get(dEdge.getDest().getId()).get(0);
//                currentlyProcessing.remove(edge.src.getId());
//                currentlyProcessing.remove(edge.dest.getId());
                value.partId = partition;
            }
            out.collect(value);
        }
    }
}
