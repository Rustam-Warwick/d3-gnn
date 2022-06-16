package partitioner;

import elements.*;
import operators.MultiThreadedProcessOperator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HDRF extends BasePartitioner{
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        return inputDataStream.transform(String.format("%s-5Threads", getName()),
                TypeInformation.of(GraphOp.class),
                new MultiThreadedProcessOperator<>(new HDRFProcessFunction(partitions),5))
                .setParallelism(1);
    }

    public static class HDRFProcessFunction extends ProcessFunction<GraphOp, GraphOp>{
        public Map<String, Integer> partialDegTable = new ConcurrentHashMap<>();

        public Map<String, List<Short>> partitionTable = new ConcurrentHashMap<>();

        public Map<Short, Integer> partitionsSize = new ConcurrentHashMap<>();

        public final short partitions;

        public transient double replicationFactor = 0f;

        public transient int totalNumberOfVertices = 1;

        public transient int totalNumberOfReplicas = 0;

        public int maxSize = 0;

        public int minSize = 0;

        public final float lamb;

        public final float eps;

        public HDRFProcessFunction(short partitions){
            this(partitions, 1f);
        }

        public HDRFProcessFunction(short partitions, float lambda) {
            this(partitions, lambda, 1f);
        }

        public HDRFProcessFunction(short partitions, float lambda, float eps) {
            this.partitions = partitions;
            this.lamb = lambda;
            this.eps = eps;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().getMetricGroup().gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return (int) (replicationFactor * 1000);
                }
            });
        }

        public float G(String vertexId, float normalDeg, short partition) {
            if (partitionTable.get(vertexId).contains(partition)) {
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
            this.partitionsSize.putIfAbsent(partition, 0);
            float res = (float) (maxSize - this.partitionsSize.get(partition)) / (eps + maxSize - minSize);
            return lamb * res;
        }

        public short computePartition(Edge edge) {
            // Initialize the tables if absent
            partialDegTable.putIfAbsent(edge.src.getId(), 0);
            partialDegTable.putIfAbsent(edge.dest.getId(), 0);
            partitionTable.putIfAbsent(edge.src.getId(), new ArrayList<>());
            partitionTable.putIfAbsent(edge.dest.getId(), new ArrayList<>());
            // Increment Partial Degree
            partialDegTable.compute(edge.src.getId(), (key, item) -> item + 1);
            partialDegTable.compute(edge.dest.getId(), (key, item) -> item + 1);
            float maxScore = Float.NEGATIVE_INFINITY;
            short selected = 0;
            for (short i = 0; i < this.partitions; i++) {
                float score = REP(edge, i) + BAL(i);
                if (score > maxScore) {
                    maxScore = score;
                    selected = i;
                }
            }
            // Update the partition size and vertex partition tables
            this.partitionsSize.compute(selected, (key, item) -> item + 1);
            maxSize = Math.max(maxSize, this.partitionsSize.get(selected));
            minSize = this.partitionsSize.values().stream().min(Integer::compareTo).get();
            if (!this.partitionTable.get(edge.src.getId()).contains(selected)) {
                this.partitionTable.get(edge.src.getId()).add(selected);
                if (this.partitionTable.get(edge.src.getId()).get(0) != selected) this.totalNumberOfReplicas++;
            }
            if (!this.partitionTable.get(edge.dest.getId()).contains(selected)) {
                this.partitionTable.get(edge.dest.getId()).add(selected);
                if (this.partitionTable.get(edge.dest.getId()).get(0) != selected) this.totalNumberOfReplicas++;
            }

            return selected;
        }

        public short computePartition(Vertex vertex) {
            // Initialize the tables if absent
            partitionTable.putIfAbsent(vertex.getId(), new ArrayList<>());
            if (partitionTable.get(vertex.getId()).isEmpty()) {
                // If no previously assigned partition for this vertex
                float maxScore = Float.NEGATIVE_INFINITY;
                short selected = 0;
                for (short i = 0; i < this.partitions; i++) {
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
            if (elementToPartition.elementType() == ElementType.FEATURE) {
                Feature<?, ?> feature = (Feature<?, ?>) elementToPartition;
                if (feature.attachedTo.f0 == ElementType.VERTEX) {
                    elementToPartition = new Vertex(feature.attachedTo.f1);
                } else {
                    String[] srcDestIds = feature.attachedTo.f1.split(":");
                    Vertex src = new Vertex(srcDestIds[0]);
                    Vertex dest = new Vertex(srcDestIds[1]);
                    elementToPartition = new Edge(src, dest);
                }
            }

            if (elementToPartition.elementType() == ElementType.EDGE) {
                // Main partitioning logic, otherwise just assign edges
                Edge edge = (Edge) elementToPartition;
                if (!this.partitionTable.containsKey(edge.src.getId())) this.totalNumberOfVertices++;
                if (!this.partitionTable.containsKey(edge.dest.getId())) this.totalNumberOfVertices++;
                short partition = this.computePartition(edge);
                edge.src.master = this.partitionTable.get(edge.src.getId()).get(0);
                edge.dest.master = this.partitionTable.get(edge.dest.getId()).get(0);
                value.partId = partition;
            } else if (elementToPartition.elementType() == ElementType.VERTEX) {
                Vertex vertex = (Vertex) elementToPartition;
                if (!this.partitionTable.containsKey(vertex.getId())) this.totalNumberOfVertices++;
                short partition = this.computePartition(vertex);
                vertex.master = this.partitionTable.get(vertex.getId()).get(0);
                value.partId = partition;
            }
            this.replicationFactor = (float) this.totalNumberOfReplicas / this.totalNumberOfVertices;
            out.collect(value);
        }
    }

    @Override
    public String getName() {
        return "HDRF-Partitioner";
    }
}
