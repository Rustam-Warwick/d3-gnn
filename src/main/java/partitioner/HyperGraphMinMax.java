package partitioner;

import elements.GraphOp;
import elements.HyperEdge;
import elements.HyperEgoGraph;
import elements.enums.ElementType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of Min-Max <strong>hyperedge-cut</strong> hypergraph partitioning algorithm
 * Only works for {@link HyperEgoGraph}
 */
public class HyperGraphMinMax extends Partitioner {

    @CommandLine.Option(names = {"--hypergraph-minmax:epsilon"}, defaultValue = "0.1", fallbackValue = "0.1", arity = "1", description = {"BetaImbalance percent used to calculate s variable in Paper"})
    public float betaImbalance;

    /**
     * {@inheritDoc}
     */
    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream) {
        Preconditions.checkState(partitions > 0);
        Preconditions.checkNotNull(inputDataStream);
        return inputDataStream.process(new Partitioner(this.partitions, this.betaImbalance)).name("HyperGraphMinMax").setParallelism(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isResponsibleFor(String partitionerName) {
        return partitionerName.equals("hypergraph-minmax");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void parseCmdArgs(String[] cmdArgs) {
        new CommandLine(this).setUnmatchedArgumentsAllowed(true).parseArgs(cmdArgs);
    }

    /**
     * Actual Min-Max partitioning function
     */
    public static class Partitioner extends ProcessFunction<GraphOp, GraphOp> {
        private final int partitions;

        private final float betaImbalance;

        public AtomicInteger totalNumberOfHEdges = new AtomicInteger(0);
        public AtomicInteger totalNumberOfReplicas = new AtomicInteger(0);
        private transient ConcurrentHashMap<String, List<Short>> hyperEdgePartitionTable;
        private transient ConcurrentHashMap<String, Short> vertexMasterTable;
        private transient String[] mark;
        private transient short[] pids;
        private transient int[] indx;
        private transient int[] save;
        private transient int[] parts;
        private transient int minParts;

        public Partitioner(int partitions, float betaImbalance) {
            this.partitions = partitions;
            this.betaImbalance = betaImbalance;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            hyperEdgePartitionTable = new ConcurrentHashMap<>();
            vertexMasterTable = new ConcurrentHashMap<>();
            save = new int[partitions];
            mark = new String[partitions];
            pids = new short[partitions];
            indx = new int[partitions];
            parts = new int[partitions];
            minParts = Arrays.stream(parts).min().getAsInt();
            getRuntimeContext().getMetricGroup().addGroup("partitioner").gauge("Replication Factor", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    int totalVertices = totalNumberOfHEdges.get();
                    int totalReplicas = totalNumberOfReplicas.get();
                    if (totalVertices == 0) return 0;
                    return (int) ((float) totalReplicas / totalVertices * 1000);
                }
            });
        }

        public short partitionSubHyperGraph(HyperEgoGraph graph) {
            int active = -1;
            String vertexId = graph.getCentralVertex().getId();
            for (HyperEdge hyperEdge : graph.getHyperEdges()) {
                List<Short> netParts = hyperEdgePartitionTable.getOrDefault(hyperEdge.getId(), Collections.emptyList());
                for (Short i : netParts) {
                    if (mark[i] == null || !mark[i].equals(vertexId)) {
                        mark[i] = vertexId;
                        active++;
                        pids[active] = i;
                        save[active] = 1;
                        indx[i] = active;
                    } else {
                        save[indx[i]] = save[indx[i]] + 1;
                    }
                }
            }
            int saved = -1;
            short p = (short) ThreadLocalRandom.current().nextInt(0, partitions);
            float s = Math.max(1, betaImbalance * ((float) vertexMasterTable.size() / partitions));
            for (int j = 0; j <= active; j++) {
                int i = pids[j];
                if (parts[i] - minParts < s) {
                    if (save[j] > saved) {
                        saved = save[j];
                        p = (short) i;
                    }
                }
            }
            parts[p]++;
            minParts = Arrays.stream(parts).min().getAsInt();
            return p;
        }

        public void updatePartitionTableAndAssignMaster(HyperEdge hyperEdge, short part) {
            hyperEdgePartitionTable.compute(hyperEdge.getId(), (key, val) -> {
                // Update hyperedge part table and master part
                // Increment the part weights according to hyperedge additions to part
                if (val == null) {
                    totalNumberOfHEdges.incrementAndGet();
                    hyperEdge.masterPart = part;
                    return new ArrayList<>(List.of(part));
                } else {
                    if (!val.contains(part)) {
                        val.add(part);
                        totalNumberOfReplicas.incrementAndGet();
                    }
                    hyperEdge.masterPart = val.get(0);
                    return val;
                }
            });
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            if (value.element.getType() == ElementType.GRAPH) {
                HyperEgoGraph graph = (HyperEgoGraph) value.element;
                short part = partitionSubHyperGraph(graph);
                vertexMasterTable.putIfAbsent(graph.getCentralVertex().getId(), part);
                graph.getCentralVertex().masterPart = vertexMasterTable.get(graph.getCentralVertex().getId());
                graph.getHyperEdges().forEach(item -> updatePartitionTableAndAssignMaster(item, part));
                out.collect(value.setPartId(part));
            } else {
                throw new IllegalStateException("MinMax Partitioner only accepts HyperEgoGraphs as input");
            }
        }
    }
}
