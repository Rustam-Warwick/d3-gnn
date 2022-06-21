package partitioner;

import elements.*;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class HDRFWithParallelSearch extends BasePartitioner {
    public float lambda = 1; // More means more balance constraint comes into play

    public final float epsilon = 1; // Leave it as is, used to not have division by zero errors

    @Override
    public void parseCmdArgs(String[] cmdArgs) {
        // Pass for now
    }

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        StreamExecutionEnvironment envThis = inputDataStream.getExecutionEnvironment();
        int numThreats = envThis.getParallelism();
        SingleOutputStreamOperator<GraphOp> res = inputDataStream
                .process(new HDRFProcessFunction(partitions, lambda, epsilon, numThreats))
                .setParallelism(1)
                .name(String.format("%s-%sThreads",getName(), numThreats));
        if(fineGrainedResourceManagementEnabled){
            envThis.registerSlotSharingGroup(
                    SlotSharingGroup
                            .newBuilder(getName())
                            .setCpuCores(5)
                            .setTaskHeapMemoryMB(100)
                            .build());
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
        public final int nThreads;
        public HashMap<String, Integer> partialDegTable = new HashMap<>();
        public HashMap<String, List<Short>> partitionTable = new HashMap<>();
        public HashMap<Short, Integer> partitionsSize = new HashMap<>();
        public Integer maxSize = 0;
        public Integer minSize = 0;
        // Metrics proprs
        public Integer totalNumberOfVertices = 0;
        public Integer totalNumberOfReplicas = 0;

        public HDRFProcessFunction(short numPartitions, float lambda, float eps, int nThreads) {
            this.numPartitions = numPartitions;
            this.lamb = lambda;
            this.eps = eps;
            this.nThreads = nThreads;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().getMetricGroup().gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    if(totalNumberOfVertices==0)return 0;
                    return (int) ((float) totalNumberOfReplicas/totalNumberOfVertices * 1000);
                }
            });
            for (short i = 0; i < numPartitions; i++) {
                partitionsSize.put(i,0);
            }
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
            float res = (float) (maxSize - this.partitionsSize.getOrDefault(partition,0)) / (eps + maxSize - minSize);
            return lamb * res;
        }

        public short computePartition(@Nonnull Edge edge) throws ExecutionException, InterruptedException {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(edge.src.getId(),1, Integer::sum);
            partialDegTable.merge(edge.dest.getId(),1, Integer::sum);

            // 2. Calculate the partition
            final Tuple2<Float, Short> finalSelected =
                            partitionsSize
                                    .keySet()
                                    .stream()
                                    .map((part)->{
                                        float score = REP(edge, part) + BAL(part);
                                        return new Tuple2<Float, Short>(score, part);
                                    })
                                    .max((o1, o2) -> {
                                        if(o1.f0 < o2.f0) return -1;
                                        else if(o1.f0 > o2.f0) return 1;
                                        return 0;
                            }).get();

            // 3. Update the tables
            int newSizeOfPartition = partitionsSize.merge(finalSelected.f1, 1, Integer::sum);
            maxSize = (Math.max(maxSize, newSizeOfPartition));
            minSize = partitionsSize.values().stream().reduce(Integer.MAX_VALUE, Math::min);
            partitionTable.compute(edge.src.getId(), (key, val) -> {
                if(val==null){
                    totalNumberOfVertices++;
                    return new ArrayList<Short>(List.of(finalSelected.f1));
                }else{
                    if (!val.contains(finalSelected.f1)){
                        totalNumberOfReplicas++;
                        val.add(finalSelected.f1);
                    }
                    return val;
                }
            });

            partitionTable.compute(edge.dest.getId(), (key, val) -> {
                if(val==null){
                    totalNumberOfVertices++;
                    return new ArrayList<Short>(List.of(finalSelected.f1));
                }else{
                    if (!val.contains(finalSelected.f1)){
                        totalNumberOfReplicas++;
                        val.add(finalSelected.f1);
                    }
                    return val;
                }
            });

            return finalSelected.f1;
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
                short partition = this.computePartition(edge);
                edge.src.master = this.partitionTable.get(edge.src.getId()).get(0);
                edge.dest.master = this.partitionTable.get(edge.dest.getId()).get(0);
                value.partId = partition;
            }
            out.collect(value);
        }
    }
}
