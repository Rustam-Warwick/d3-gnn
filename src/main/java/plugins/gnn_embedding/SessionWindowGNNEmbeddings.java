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
 */
public class SessionWindowGNNEmbeddings extends WindowedGNNEmbedding{

    public final long intraLayerSessionDuration;

    public final long interLayerSessionDuration;

    public SessionWindowGNNEmbeddings(String modelName, boolean trainableVertexEmbeddings, long intraLayerSessionDuration, long interLayerSessionDuration) {
        super(modelName, trainableVertexEmbeddings);
        this.intraLayerSessionDuration = intraLayerSessionDuration;
        this.interLayerSessionDuration = interLayerSessionDuration;
    }

    public SessionWindowGNNEmbeddings(String modelName, boolean trainableVertexEmbeddings, long sessionDuration) {
        this(modelName, trainableVertexEmbeddings, sessionDuration, sessionDuration);
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + intraLayerSessionDuration;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        getRuntimeContext().getStorage().deleteEdge(directedEdge);
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partReduceMaps = intraLayerMaps.get(getRuntimeContext().getCurrentPart());
        partReduceMaps.f0.removeLong(directedEdge.getDestId());
        partReduceMaps.f0.put(directedEdge.getDestId(), updateTime);
        partReduceMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore) -> new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partReduceMaps.f2.put(directedEdge.getDestId(), getRuntimeContext().currentTimestamp());
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }


    @Override
    public void interLayerWindow(Vertex v) {
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + interLayerSessionDuration;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> maps = interLayerMaps.get(getPart());
        maps.f0.removeLong(v.getId());
        maps.f0.put(v.getId(), updateTime);
        maps.f1.put(v.getId(), getRuntimeContext().currentTimestamp());
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }
}
