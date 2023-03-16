package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import elements.GraphOp;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.features.Tensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.taskshared.TaskSharedPerPartMapState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GNN Embedding Layer that forwards messages only on pre-defined sessioned intervals
 */
public class SessionWindowedGNNEmbedding extends StreamingGNNEmbedding {

    public final int sessionInterval; // Window Interval for graph element updates in milliseconds

    public transient Map<Short, HashMap<String, Long>> BATCH; // Map for storing processingTimes

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    public SessionWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int sessionInterval) {
        super(modelName, trainableVertexEmbeddings);
        this.sessionInterval = sessionInterval;
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        BATCH = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("BATCH", Types.GENERIC(Map.class), TaskSharedPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> BATCH.put(part, new HashMap<>()));
        windowThroughput = new SimpleCounter();
        getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
    }

    public void forward(Vertex v) {
        long currentProcessingTime = getRuntimeContext().getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + sessionInterval;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 100.0) * 100);
        HashMap<String, Long> PART_BATCH = BATCH.get(getRuntimeContext().getCurrentPart());
        PART_BATCH.put(v.getId(), thisElementUpdateTime);
        getRuntimeContext().getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    public void evictUpUntil(long timestamp){
        HashMap<String, Long> PART_BATCH = BATCH.get(getRuntimeContext().getCurrentPart());
        NDList features = new NDList();
        NDList aggregators = new NDList();
        List<Vertex> vertices = new ArrayList<>();
        PART_BATCH.forEach((key, val) -> {
            if (val <= timestamp) {
                Vertex v = getRuntimeContext().getStorage().getVertex(key);
                features.add((NDArray) (v.getFeature("f")).getValue());
                aggregators.add((NDArray) (v.getFeature("agg")).getValue());
                vertices.add(v);
            }
        });
        if (vertices.isEmpty()) return;
        NDList batchedInput = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
        NDArray batchedUpdates = UPDATE(batchedInput, false).get(0);
        for (int i = 0; i < vertices.size(); i++) {
            PART_BATCH.remove(vertices.get(i).getId());
            Vertex messageVertex = vertices.get(i);
            Tensor updateTensor = new Tensor("f", batchedUpdates.get(i), false, messageVertex.getMasterPart());
            updateTensor.id.f0 = ElementType.VERTEX;
            updateTensor.id.f1 = messageVertex.getId();
            getRuntimeContext().output(new GraphOp(Op.COMMIT, updateTensor.getMasterPart(), updateTensor));
            throughput.inc();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<PartNumber, VoidNamespace> timer) throws Exception {
        super.onProcessingTime(timer);
        evictUpUntil(timer.getTimestamp());
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.EnterTraining){
            getRuntimeContext().runForAllLocalParts(()-> {
                if(BATCH.containsKey(getRuntimeContext().getCurrentPart())) evictUpUntil(Long.MAX_VALUE);
            });
        }
    }
}
