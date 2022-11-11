package partitioner;

import elements.GraphOp;
import elements.HEdge;
import elements.HGraph;
import elements.Vertex;
import elements.enums.ElementType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class HyperGraphMinMax extends BasePartitioner {

    @Override
    public SingleOutputStreamOperator<GraphOp> partition(DataStream<GraphOp> inputDataStream, boolean fineGrainedResourceManagementEnabled) {
        return inputDataStream.process(new Partitioner(this.partitions)).name("HyperGraphMinMax").setParallelism(1);
    }

    public static class Partitioner extends ProcessFunction<GraphOp, GraphOp> {
        private final int partitions;
        private final int s;
        private transient ConcurrentHashMap<String, List<Short>> n2p;
        private transient ConcurrentHashMap<String, List<Short>> vertex2p;
        private transient String[] mark;
        private transient short[] pids;
        private transient int[] indx;
        private transient int[] save;
        private transient int[] parts;
        private transient int minParts;

        public Partitioner(int partitions) {
            this(partitions, 10);
        }

        public Partitioner(int partitions, int s) {
            this.partitions = partitions;
            this.s = s;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            n2p = new ConcurrentHashMap<>();
            vertex2p = new ConcurrentHashMap<>();
            save = new int[partitions];
            mark = new String[partitions];
            pids = new short[partitions];
            indx = new int[partitions];
            parts = new int[partitions];
            minParts = Arrays.stream(parts).min().getAsInt();
        }

        public short partitionSubHyperGraph(HGraph graph) {
            int active = 0;
            String vertexId = graph.getVertices().get(0).getId();
            for (HEdge hEdge : graph.gethEdges()) {
                List<Short> netParts = n2p.getOrDefault(hEdge.getId(), Collections.emptyList());
                for (Short i : netParts) {
                    if (mark[i] != null && !mark[i].equals(vertexId)) {
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
            for (int j = 1; j <= active; j++) {
                int i = pids[j];
                if (parts[i] - minParts < s) {
                    if (save[j] > saved) {
                        saved = save[j];
                        p = (short) i;
                    }
                }
            }


            return p;
        }

        @Override
        public void processElement(GraphOp value, ProcessFunction<GraphOp, GraphOp>.Context ctx, Collector<GraphOp> out) throws Exception {
            if (value.element.elementType() == ElementType.GRAPH) {
                HGraph graph = (HGraph) value.element;
                short part = partitionSubHyperGraph(graph); // Get the correct part
                for (Vertex vertex : graph.getVertices()) {
                    // Update vertex part table and master part
                    vertex2p.compute(vertex.getId(), (key, val) -> {
                        if (val == null) {
                            vertex.master = part;
                            return new ArrayList<>(List.of(part));
                        } else {
                            if (!val.contains(part)) val.add(part);
                            vertex.master = val.get(0);
                            return val;
                        }
                    });
                }
                for (HEdge hEdge : graph.gethEdges()) {
                    n2p.compute(hEdge.getId(), (key, val) -> {
                        // Update hyperedge part table and master part
                        // Increment the part weights according to hyperedge additions to part
                        if (val == null) {
                            hEdge.master = part;
                            parts[part]++;
                            return new ArrayList<>(List.of(part));
                        } else {
                            if (!val.contains(part)) {
                                parts[part]++;
                                val.add(part);
                            }
                            hEdge.master = val.get(0);
                            return val;
                        }
                    });
                }
                value.setPartId(part);
                out.collect(value);
                minParts = Arrays.stream(parts).min().getAsInt(); // Update the min parts
            }
        }
    }
}
