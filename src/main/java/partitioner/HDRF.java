package partitioner;

import elements.DirectedEdge;
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
import org.apache.flink.util.Preconditions;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of HDRF <strong>vertex-cut</strong> partitioning algorithm
 */
public class HDRF extends Partitioner {

    /**
     * A small value to overcome division by zero errors on score calculation
     */
    @CommandLine.Option(names = {"--hdrf:epsilon"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = {"Epsilon to be used in HDRF"})
    public float epsilon;

    /**
     * <strong>Balance coefficient</strong>. Higher value usually means higher distribution of edges.
     */
    @CommandLine.Option(names = {"--hdrf:lambda"}, defaultValue = "4", fallbackValue = "4", arity = "1", description = {"Lambda to be used in HDRF"})
    public float lambda;

    /**
     * Number of threads to use for partitioning
     */
    @CommandLine.Option(names = {"--hdrf:numThreads"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = {"Number of threads to distribute HDRF"})
    public int numThreads;

    public HDRF(String[] cmdArgs) {
        super(cmdArgs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        Preconditions.checkState(partitions > 0);
        Preconditions.checkNotNull(inputDataStream);
        String opName = String.format("HDRF[l=%s,eps=%s,threads=%s]", lambda, epsilon, numThreads);
        return inputDataStream.transform(opName,
                TypeInformation.of(GraphOp.class),
                new MultiThreadedProcessOperator<>(new HDRFProcessFunction(partitions, lambda, epsilon), numThreads)).setParallelism(1);
    }

    /**
     * Actual HDRF Processing Function
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

        public float REP(DirectedEdge directedEdge, short partition) {
            int srcDeg = partialDegTable.get(directedEdge.getSrcId());
            int destDeg = partialDegTable.get(directedEdge.getDestId());
            float srcDegNormal = (float) srcDeg / (srcDeg + destDeg);
            float destDegNormal = 1 - srcDegNormal;
            return this.G(directedEdge.getSrcId(), srcDegNormal, partition) + this.G(directedEdge.getDestId(), destDegNormal, partition);
        }

        public float BAL(short partition) {
            float res = (float) (maxSize.get() - this.partitionsSize.getOrDefault(partition, 0)) / (eps + maxSize.get() - minSize.get());
            return lamb * res;
        }

        public short computePartition(DirectedEdge directedEdge) {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(directedEdge.getSrcId(), 1, Integer::sum);
            partialDegTable.merge(directedEdge.getDestId(), 1, Integer::sum);

            // 2. Calculate the partition
            float maxScore = Float.NEGATIVE_INFINITY;
            List<Short> tmp = new ArrayList<>();
            for (short i = 0; i < this.numPartitions; i++) {
                float score = REP(directedEdge, i) + BAL(i);
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
            if (elementToPartition.getType() == ElementType.EDGE) {
                DirectedEdge directedEdge = (DirectedEdge) elementToPartition;
                short partition = this.computePartition(directedEdge);
                partitionTable.compute(directedEdge.getSrcId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        directedEdge.getSrc().masterPart = partition;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(partition)));
                    } else {
                        if (!val.contains(partition)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(partition);
                        }
                        directedEdge.getSrc().masterPart = val.get(0);
                        return val;
                    }
                });

                partitionTable.compute(directedEdge.getDestId(), (key, val) -> {
                    if (val == null) {
                        // This is the first part of this vertex hence the master
                        directedEdge.getDest().masterPart = partition;
                        totalNumberOfVertices.incrementAndGet();
                        return Collections.synchronizedList(new ArrayList<Short>(List.of(partition)));
                    } else {
                        if (!val.contains(partition)) {
                            // Seocond or more part hence the replica
                            totalNumberOfReplicas.incrementAndGet();
                            val.add(partition);
                        }
                        directedEdge.getDest().masterPart = val.get(0);
                        return val;
                    }
                });

                value.partId = partition;
            }
            out.collect(value);
        }
    }
}
