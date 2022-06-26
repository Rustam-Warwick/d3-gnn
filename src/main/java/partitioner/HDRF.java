package partitioner;

import elements.*;
import operators.MultiThreadedProcessOperator;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HDRF extends BasePartitioner {
    public final float epsilon = 1; // Leave it as is, used to not have division by zero errors
    public float lambda = 0.6f; // More means more balance constraint comes into play

    @Override
    public void parseCmdArgs(String[] cmdArgs) {
        // Pass for now
    }

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        StreamExecutionEnvironment envThis = inputDataStream.getExecutionEnvironment();
        int numThreats = 3;
        SingleOutputStreamOperator<GraphOp> res = inputDataStream.transform(String.format("%s-%sThreads", getName(), numThreats),
                TypeInformation.of(GraphOp.class),
                new MultiThreadedProcessOperator<>(new HDRFProcessFunction(partitions, lambda, epsilon), numThreats)).uid(String.format("%s-%sThreads", getName(), numThreats)).setParallelism(1);
        if (fineGrainedResourceManagementEnabled) {
//            envThis.registerSlotSharingGroup(
//                    SlotSharingGroup
//                            .newBuilder(getName())
//                            .setCpuCores(1.0)
//                            .setTaskHeapMemoryMB(100)
//                            .build());
            res.slotSharingGroup(getName());
        }
        return res;
    }

    @Override
    public String getName() {
        return "HDRF-Partitioner";
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

        public float REP(Edge edge, short partition) {
            int srcDeg = partialDegTable.get(edge.src.getId());
            int destDeg = partialDegTable.get(edge.dest.getId());
            float srcDegNormal = (float) srcDeg / (srcDeg + destDeg);
            float destDegNormal = 1 - srcDegNormal;
            return this.G(edge.src.getId(), srcDegNormal, partition) + this.G(edge.dest.getId(), destDegNormal, partition);
        }

        public float BAL(short partition) {
            float res = (float) (maxSize.get() - this.partitionsSize.getOrDefault(partition, 0)) / (eps + maxSize.get() - minSize.get());
            return lamb * res;
        }

        public short computePartition(Edge edge) {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(edge.src.getId(), 1, Integer::sum);
            partialDegTable.merge(edge.dest.getId(), 1, Integer::sum);

            // 2. Calculate the partition
            float maxScore = Float.NEGATIVE_INFINITY;
            short selected = 0;
            for (short i = 0; i < this.numPartitions; i++) {
                float score = REP(edge, i) + BAL(i);
                if (score > maxScore) {
                    maxScore = score;
                    selected = i;
                }
            }
            final short finalSelected = selected;

            // 3. Update the tables
            int newSizeOfPartition = partitionsSize.merge(finalSelected, 1, Integer::sum);

            maxSize.set(Math.max(maxSize.get(), newSizeOfPartition));
            minSize.set(partitionsSize.reduceValues(Long.MAX_VALUE, Math::min));
            partitionTable.compute(edge.src.getId(), (key, val) -> {
                if (val == null) {
                    totalNumberOfVertices.incrementAndGet();
                    return new ArrayList(List.of(finalSelected));
                } else {
                    if (!val.contains(finalSelected)) {
                        totalNumberOfReplicas.incrementAndGet();
                        val.add(finalSelected);
                    }
                    return val;
                }
            });

            partitionTable.compute(edge.dest.getId(), (key, val) -> {
                if (val == null) {
                    totalNumberOfVertices.incrementAndGet();
                    return new ArrayList(List.of(finalSelected));
                } else {
                    if (!val.contains(finalSelected)) {
                        totalNumberOfReplicas.incrementAndGet();
                        val.add(finalSelected);
                    }
                    return val;
                }
            });

            return selected;
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
                Edge edge = (Edge) elementToPartition;
//                while(currentlyProcessing.contains(edge.src.getId()) || currentlyProcessing.contains(edge.dest.getId())){
//                    // Wait for completion
//                }
//                currentlyProcessing.add(edge.src.getId());
//                currentlyProcessing.add(edge.dest.getId());
                short partition = this.computePartition(edge);
                edge.src.master = this.partitionTable.get(edge.src.getId()).get(0);
                edge.dest.master = this.partitionTable.get(edge.dest.getId()).get(0);
//                currentlyProcessing.remove(edge.src.getId());
//                currentlyProcessing.remove(edge.dest.getId());
                value.partId = partition;
            }
            out.collect(value);
        }
    }
}
