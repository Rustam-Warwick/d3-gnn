package partitioner;

import elements.DirectedEdge;
import elements.Feature;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.fastutil.shorts.ShortList;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import picocli.CommandLine;

import java.util.*;

/**
 * Cascading-HDRF for handling balancing cascading load of GNNs
 */
public class CHDRF extends Partitioner {

    /**
     * A small value to overcome division by zero errors on score calculation
     */
    @CommandLine.Option(names = {"--hdrf:epsilon"}, defaultValue = "1", fallbackValue = "1", arity = "1", description = {"Epsilon to be used in HDRF"})
    public float epsilon;

    /**
     * <strong>Balance coefficient</strong>. Higher value usually means higher distribution of edges.
     */
    @CommandLine.Option(names = {"--hdrf:lambda"}, defaultValue = "2", fallbackValue = "2", arity = "1", description = {"Lambda to be used in HDRF"})
    public float lambda;

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        super.partition(inputDataStream);
        return inputDataStream.process(new CHDRFProcessFunction(partitions, lambda, epsilon)).name(String.format("CHDRF[l=%s,eps=%s]", lambda, epsilon)).setParallelism(1);
    }

    @Override
    public boolean isResponsibleFor(String partitionerName) {
        return partitionerName.equals("chdrf");
    }

    public static class CHDRFProcessFunction extends ProcessFunction<GraphOp, GraphOp> {
        public final short numPartitions;
        public final float lamb;
        public final float eps;
        public transient Random random;
        public transient Object2IntMap<String> partialDegTable;
        public transient Map<String, ShortList> partitionTable;
        public transient int[] partitionsSize;
        public transient ShortList emptyShortList;
        public transient ShortList reuseShortList;
        public transient long degreeSum;
        public transient long maxSize;
        public transient long minSize;
        public transient long totalNumberOfVertices;
        public transient long totalNumberOfReplicas;

        public CHDRFProcessFunction(short numPartitions, float lambda, float eps) {
            this.numPartitions = numPartitions;
            this.lamb = lambda;
            this.eps = eps;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            random = new Random(0);
            partialDegTable = new Object2IntOpenHashMap<>();
            partitionTable = new HashMap<>();
            partitionsSize = new int[numPartitions];
            emptyShortList = new ShortArrayList(0);
            reuseShortList = new ShortArrayList();
            getRuntimeContext().getMetricGroup().addGroup("partitioner").gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    if (totalNumberOfVertices == 0) return 0;
                    return (int) ((float) totalNumberOfReplicas / totalNumberOfVertices * 1000);
                }
            });
        }

        protected float G(String vertexId, float normalDeg, short partition) {
            if (partitionTable.getOrDefault(vertexId, emptyShortList).contains(partition)) {
                return 2 - normalDeg; // 1 + (1 - \theta)
            } else return 0;
        }

        protected float REP(DirectedEdge directedEdge, short partition) {
            int srcDeg = partialDegTable.getInt(directedEdge.getSrcId());
            int destDeg = partialDegTable.getInt(directedEdge.getDestId());
            float srcDegNormal = (float) srcDeg / (srcDeg + destDeg);
            float destDegNormal = 1 - srcDegNormal;
            return G(directedEdge.getSrcId(), srcDegNormal, partition) + G(directedEdge.getDestId(), destDegNormal, partition);
        }

        protected float BAL(short partition) {
            float res = (float) (maxSize - partitionsSize[partition]) / (eps + maxSize - minSize);
            return lamb * res;
        }

        protected short partitionEdge(DirectedEdge directedEdge) {
            // 1. Increment the node degrees seen so far
            partialDegTable.merge(directedEdge.getSrcId(), 1, Integer::sum);
            partialDegTable.merge(directedEdge.getDestId(), 1, Integer::sum);
            degreeSum += 2;

            // 2. Calculate the partition
            float maxScore = Float.NEGATIVE_INFINITY;
            reuseShortList.clear();
            for (short i = 0; i < numPartitions; i++) {
                float score = REP(directedEdge, i) + BAL(i);
                if (score > maxScore) {
                    reuseShortList.clear();
                    maxScore = score;
                    reuseShortList.add(i);
                } else if (score == maxScore) reuseShortList.add(i);
            }
            final short finalSelected = reuseShortList.getShort(random.nextInt(reuseShortList.size()));

            // 3. Update the tables
            partitionsSize[finalSelected]++;
            maxSize = Math.max(maxSize, partitionsSize[finalSelected]);
            minSize = Arrays.stream(partitionsSize).min().getAsInt();
            return finalSelected;
        }

        protected void updatePartitionTableAndAssignMaster(Vertex vertex, short part) {
            partitionTable.compute(vertex.getId(), (key, val) -> {
                if (val == null) {
                    // This is the first part of this vertex hence the master
                    vertex.masterPart = part;
                    totalNumberOfVertices++;
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
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            switch (value.element.getType()) {
                case EDGE: {
                    DirectedEdge directedEdge = (DirectedEdge) value.element;
                    short part = partitionEdge(directedEdge);
                    updatePartitionTableAndAssignMaster(directedEdge.getSrc(), part);
                    updatePartitionTableAndAssignMaster(directedEdge.getDest(), part);
                    partitionsSize[directedEdge.getSrc().masterPart] += 1 + (degreeSum / totalNumberOfVertices);
                    out.collect(value.setPartId(part));
                    break;
                }
                case VERTEX: {
                    Vertex v = (Vertex) value.element;
                    v.masterPart = partitionTable.get(value.element.getId()).getShort(0);
                    out.collect(value.setPartId(partitionTable.get(value.element.getId()).getShort(0)));
                    break;
                }
                case ATTACHED_FEATURE: {
                    Feature<?, ?> feature = (Feature<?, ?>) value.element;
                    if (feature.getAttachedElementType() == ElementType.VERTEX) {
                        out.collect(value.setPartId(partitionTable.get(feature.getAttachedElementId()).getShort(0)));
                        break;
                    }
                }
                default:
                    throw new NotImplementedException("Other Element Types are not allowed: Received" + value.element.getType());
            }
        }
    }
}