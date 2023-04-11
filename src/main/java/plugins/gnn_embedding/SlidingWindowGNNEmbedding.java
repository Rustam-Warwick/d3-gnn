package plugins.gnn_embedding;

import elements.DirectedEdge;
import elements.Vertex;
import it.unimi.dsi.fastutil.objects.Object2LongLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/**
 * Windowing plugin that evicts active vertices periodically
 * Contrary to {@link SessionWindowGNNEmbeddings} it does constantly delay the times and wait for window expiration
 */
public class SlidingWindowGNNEmbedding extends WindowedGNNEmbedding {

    public final long intraLayerWindowSizeMs;

    public final long interLayerWindowSizeMs;

    public SlidingWindowGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, long intraLayerWindowSizeMs, long interLayerWindowSizeMs) {
        super(modelName, trainableVertexEmbeddings);
        this.intraLayerWindowSizeMs = intraLayerWindowSizeMs;
        this.interLayerWindowSizeMs = interLayerWindowSizeMs;
    }

    public SlidingWindowGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, long windowSizeMs) {
        this(modelName, trainableVertexEmbeddings, windowSizeMs, windowSizeMs);
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {
        getRuntimeContext().getStorage().deleteEdge(directedEdge);
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partReduceMaps = intraLayerMaps.get(getRuntimeContext().getCurrentPart());
        partReduceMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore) -> new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partReduceMaps.f2.put(directedEdge.getDestId(), getRuntimeContext().currentTimestamp());
        if(!partReduceMaps.f0.containsKey(directedEdge.getDestId())) {
            long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + intraLayerWindowSizeMs;
            long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
            partReduceMaps.f0.put(directedEdge.getDestId(), updateTime);
            getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        }
    }

    @Override
    public void interLayerWindow(Vertex v) {
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = interLayerMaps.get(getPart());
        maps.f1.put(v.getId(), getRuntimeContext().currentTimestamp());
        if(!maps.f0.containsKey(v.getId())) {
            long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + interLayerWindowSizeMs;
            long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
            maps.f0.put(v.getId(), updateTime);
            getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        }
    }
}
