package partitioner;

import elements.DEdge;
import elements.GraphElement;
import elements.GraphOp;
import elements.enums.ElementType;
import operators.MultiThreadedProcessOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class HDRF extends BasePartitioner {

    public final float epsilon = 1; // Leave it as is, used to not have division by zero errors

    public final float lambda = 4f; // More means more balance constraint comes into play

    public final int numThreads = 1; // Number of thread that will process this dataset

    /**
     * {@inheritDoc}
     */
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        return inputDataStream.transform(String.format("%s-%sThreads", "HDRF", numThreads),
                TypeInformation.of(GraphOp.class),
                new MultiThreadedProcessOperator<>(new HDRFProcessFunction(partitions, lambda, epsilon), numThreads)).uid(String.format("%s-%sThreads", "HDRF", numThreads)).setParallelism(1);
    }

    /**
     * Actual ProcessFunction for HDRF
     */
    public static class HDRFProcessFunction extends ProcessFunction<GraphOp, GraphOp> {
        public final short numPartitions;
        public final float lamb;
        public final float eps;
        public ConcurrentHashMap<String, Integer> partialDegTable = new ConcurrentHashMap<>();
        public ConcurrentHashMap<String, List<Short>> partitionTable = new ConcurrentHashMap<>();
        public ConcurrentHashMap<Short, Integer> partitionsSize = new ConcurrentHashMap<>();

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
            int srcDeg = partialDegTable.get(dEdge.getSrcId());
            int destDeg = partialDegTable.get(dEdge.getDestId());
            float srcDegNormal = (float) srcDeg / (srcDeg + destDeg);
            float destDegNormal = 1 - srcDegNormal;
            return this.G(dEdge.getSrcId(), srcDegNormal, partition) + this.G(dEdge.getDestId(), destDegNormal, partition);
        }

        public float BAL(short partition) {
            float res = (float) (maxSize.get() - this.partitionsSize.getOrDefault(partition, 0)) / (eps + maxSize.get() - minSize.get());
            return lamb * res;
        }

        public short computePartition(DEdge dEdge) {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(dEdge.getSrcId(), 1, Integer::sum);
            partialDegTable.merge(dEdge.getDestId(), 1, Integer::sum);

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
//            final short finalSelected = tmp.get(0);
            // 3. Update the tables
            int newSizeOfPartition = partitionsSize.merge(finalSelected, 1, Integer::sum);
            maxSize.set(Math.max(maxSize.get(), newSizeOfPartition));
            minSize.set(partitionsSize.reduceValues(Long.MAX_VALUE, Math::min));
            return finalSelected;
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            GraphElement elementToPartition = value.element;
            if (elementToPartition.elementType() == ElementType.EDGE) {
                DEdge dEdge = (DEdge) elementToPartition;
                short partition = this.computePartition(dEdge);
                partitionTable.compute(dEdge.getSrcId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        dEdge.getSrc().master = partition;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(partition)));
                    } else {
                        if (!val.contains(partition)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(partition);
                        }
                        dEdge.getSrc().master = val.get(0);
                        return val;
                    }
                });

                partitionTable.compute(dEdge.getDestId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        dEdge.getDest().master = partition;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(partition)));
                    } else {
                        if (!val.contains(partition)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(partition);
                        }
                        dEdge.getDest().master = val.get(0);
                        return val;
                    }
                });

                value.partId = partition;
            }
            out.collect(value);
        }
    }
}
