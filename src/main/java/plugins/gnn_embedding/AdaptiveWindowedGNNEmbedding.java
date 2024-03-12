package plugins.gnn_embedding;

import elements.DirectedEdge;
import elements.Vertex;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.tmshared.TMSharedExpMovingAverageCountMinSketch;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;

import java.util.List;

/**
 * Adaptive windowing based on exponential-mean of the vertex forward messages
 */
public class AdaptiveWindowedGNNEmbedding extends WindowedGNNEmbedding {

    protected final static double defaultMomentum = 0.5;
    protected final long intraLayerSessionDurationMinMs;
    protected final long interLayerSessionDurationMinMs;
    protected final long movingAverageIntervalMs;
    protected final double momentum;
    protected transient TMSharedExpMovingAverageCountMinSketch interLayerExpMean;
    protected transient TMSharedExpMovingAverageCountMinSketch intraLayerExpMean;

    public AdaptiveWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, long intraLayerSessionDurationMinMs, long interLayerSessionDurationMinMs, long movingAverageIntervalMs, double momentum) {
        super(modelName, trainableVertexEmbeddings);
        this.intraLayerSessionDurationMinMs = intraLayerSessionDurationMinMs;
        this.interLayerSessionDurationMinMs = interLayerSessionDurationMinMs;
        this.movingAverageIntervalMs = movingAverageIntervalMs;
        this.momentum = momentum;
    }

    public AdaptiveWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, long sessionDurationMin, long movingAverageIntervalMs) {
        this(modelName, trainableVertexEmbeddings, sessionDurationMin / 2,sessionDurationMin, movingAverageIntervalMs, defaultMomentum);
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        interLayerExpMean = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_interLayerExpMean", TypeInformation.of(TMSharedExpMovingAverageCountMinSketch.class), () -> new TMSharedExpMovingAverageCountMinSketch(0.001, 0.99, movingAverageIntervalMs, momentum)));
        intraLayerExpMean = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_intraLayerExpMean", TypeInformation.of(TMSharedExpMovingAverageCountMinSketch.class), () -> new TMSharedExpMovingAverageCountMinSketch(0.001, 0.99, movingAverageIntervalMs, momentum)));
    }

    @Override
    public void intraLayerWindow(DirectedEdge directedEdge) {
        intraLayerExpMean.add(directedEdge.getDestId(), 1);
        long estimatedCount = intraLayerExpMean.estimateCount(directedEdge.getDestId()) + 1;
        long sessionDuration = Math.max(intraLayerSessionDurationMinMs, movingAverageIntervalMs / estimatedCount);
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + sessionDuration;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        getRuntimeContext().getStorage().deleteEdge(directedEdge);
        Tuple3<Object2LongLinkedOpenHashMap<String>, Object2ObjectOpenHashMap<String, List<String>>, Object2LongOpenHashMap<String>> partIntraLayerMaps = intraLayerMaps.get(getPart());
        partIntraLayerMaps.f0.mergeLong(directedEdge.getDestId(), updateTime, Math::max);
        partIntraLayerMaps.f1.computeIfAbsent(directedEdge.getDestId(), (ignore) -> new ObjectArrayList<>()).add(directedEdge.getSrcId());
        partIntraLayerMaps.f2.mergeLong(directedEdge.getDestId(), getRuntimeContext().currentTimestamp(), Math::max);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }


    @Override
    public void interLayerWindow(Vertex v) {
        interLayerExpMean.add(v.getId(), 1);
        long estimatedCount = interLayerExpMean.estimateCount(v.getId()) + 1;
        long sessionDuration = Math.max(interLayerSessionDurationMinMs, movingAverageIntervalMs / estimatedCount);
        long updateTime = getRuntimeContext().getTimerService().currentProcessingTime() + sessionDuration;
        long timerTime = (long) (Math.ceil((updateTime) / TIMER_COALESCING) * TIMER_COALESCING);
        Tuple2<Object2LongLinkedOpenHashMap<String>, Object2LongOpenHashMap<String>> partInterLayerMaps = interLayerMaps.get(getPart());
        partInterLayerMaps.f0.mergeLong(v.getId(), updateTime, Math::max);
        partInterLayerMaps.f1.mergeLong(v.getId(), getRuntimeContext().currentTimestamp(), Math::max);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
    }

    @Override
    public boolean hasOrderedTimestamps() {
        return false;
    }
}
