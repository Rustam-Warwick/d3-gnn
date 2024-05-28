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
 * Windowing plugins that evict the windowing results once a per-vertex session has expired
 * In other words, once that vertex has been inactive for > some session duration
 * By default intraLayerWindow is half the size of interLayer ones
 */
public class SessionWindowGNNEmbeddings extends WindowedGNNEmbedding {

    public final long intraLayerSessionDurationMs;

    public final long interLayerSessionDurationMs;

    public SessionWindowGNNEmbeddings(String modelName, boolean trainableVertexEmbeddings, long intraLayerSessionDurationMs, long interLayerSessionDurationMs) {
        super(modelName, trainableVertexEmbeddings);
        this.intraLayerSessionDurationMs = intraLayerSessionDurationMs;
        this.interLayerSessionDurationMs = interLayerSessionDurationMs;
    }

    public SessionWindowGNNEmbeddings(String modelName, boolean trainableVertexEmbeddings, long sessionDuration) {
        this(modelName, trainableVertexEmbeddings, sessionDuration / 2, sessionDuration);
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + intraLayerSessionDurationMs;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partIntraLayerMaps = intraLayerMaps.get(getPart());
        partIntraLayerMaps.f0.removeLong(directedEdge.getDestId());
        partIntraLayerMaps.f0.put(directedEdge.getDestId(), updateTime);
        partIntraLayerMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore) -> new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partIntraLayerMaps.f2.mergeLong(directedEdge.getDestId(), getRuntimeContext().currentTimestamp(), Math::max);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }


    @Override
    public void interLayerWindow(Vertex v) {
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + interLayerSessionDurationMs;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> partInterLayerMaps = interLayerMaps.get(getPart());
        partInterLayerMaps.f0.removeLong(v.getId());
        partInterLayerMaps.f0.put(v.getId(), updateTime);
        partInterLayerMaps.f1.mergeLong(v.getId(), getRuntimeContext().currentTimestamp(), Math::max);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }

    @Override
    public boolean hasOrderedTimestamps() {
        return true;
    }
}
