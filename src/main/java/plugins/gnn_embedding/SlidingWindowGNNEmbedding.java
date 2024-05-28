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
 * By default intraLayerWindow is half the size of interLayer ones
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
        this(modelName, trainableVertexEmbeddings, windowSizeMs / 2, windowSizeMs);
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partIntraLayerMaps = intraLayerMaps.get(getPart());
        partIntraLayerMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore) -> new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partIntraLayerMaps.f2.mergeLong(directedEdge.getDestId(), getRuntimeContext().currentTimestamp(), Math::max);
        if (!partIntraLayerMaps.f0.containsKey(directedEdge.getDestId())) {
            long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + intraLayerWindowSizeMs;
            long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
            partIntraLayerMaps.f0.put(directedEdge.getDestId(), updateTime);
            getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        }
    }

    @Override
    public void interLayerWindow(Vertex v) {
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> partInterLayerMaps = interLayerMaps.get(getPart());
        partInterLayerMaps.f1.mergeLong(v.getId(), getRuntimeContext().currentTimestamp(), Math::max);
        if (!partInterLayerMaps.f0.containsKey(v.getId())) {
            long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + interLayerWindowSizeMs;
            long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
            partInterLayerMaps.f0.put(v.getId(), updateTime);
            getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        }
    }

    @Override
    public boolean hasOrderedTimestamps() {
        return true;
    }
}
