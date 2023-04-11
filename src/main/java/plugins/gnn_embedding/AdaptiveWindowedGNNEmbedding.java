//package plugins.gnn_embedding;
//
//import ai.djl.ndarray.BaseNDManager;
//import ai.djl.ndarray.NDList;
//import elements.Vertex;
//import elements.enums.EdgeType;
//import elements.features.Aggregator;
//import elements.features.InPlaceMeanAggregator;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.tmshared.TMSharedExpMovingAverageCountMinSketch;
//import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
//
//import java.util.Arrays;
//
///**
// * Adaptive windowing based on exponential-mean of the vertex forward messages
// */
//public class AdaptiveWindowedGNNEmbedding extends WindowedGNNEmbedding {
//
//    protected final long movingAverageIntervalMs;
//    protected final double momentum;
//    protected transient TMSharedExpMovingAverageCountMinSketch forwardExpMean;
//    protected transient double messageComputationTimeMs;
//
//    protected transient double aggregationTimeMs;
//
//    public AdaptiveWindowedGNNEmbedding(String modelName, boolean trainableVertexEmbeddings, int minSessionIntervalMs, long movingAverageIntervalMs, double momentum) {
//        super(modelName, trainableVertexEmbeddings, minSessionIntervalMs);
//        this.movingAverageIntervalMs = movingAverageIntervalMs;
//        this.momentum = momentum;
//    }
//
//    @Override
//    public void open(Configuration params) throws Exception {
//        super.open(params);
//        forwardExpMean = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("window_forwardExpMean", TypeInformation.of(TMSharedExpMovingAverageCountMinSketch.class), () -> new TMSharedExpMovingAverageCountMinSketch(0.001, 0.99, movingAverageIntervalMs, momentum)));
//        estimateComputationTimes();
//    }
//
//    /**
//     * Estimate the computation times for the next layer
//     */
//    protected void estimateComputationTimes() {
//        NDList messageList = new NDList(BaseNDManager.getManager().zeros(modelServer.getInputShapes()[0]));
//        messageList.delay();
//        Aggregator<?> aggregator = new InPlaceMeanAggregator("agg", BaseNDManager.getManager().zeros(modelServer.getOutputShapes()[0]), true);
//        aggregator.delay();
//        long[] messageTimes = new long[50];
//        long[] aggregationTimes = new long[50];
//        for (int i = 0; i < messageTimes.length; i++) {
//            long tmp = System.nanoTime();
//            NDList message = MESSAGE(messageList, false);
//            messageTimes[i] = System.nanoTime() - tmp;
//            tmp = System.nanoTime();
//            aggregator.replace(message, message);
//            aggregationTimes[i] = System.nanoTime() - tmp;
//        }
//        messageComputationTimeMs = Arrays.stream(messageTimes).asDoubleStream().average().getAsDouble() / 1_000_000.0;
//        aggregationTimeMs = Arrays.stream(aggregationTimes).asDoubleStream().average().getAsDouble() / 1_000_000.0;
//    }
//
//    @Override
//    public void forward(Vertex v) {
//        forwardExpMean.add(v.getId(), 1);
//        long vertexCount = forwardExpMean.estimateCount(v.getId());
//        if (vertexCount == 0) super.forward(v);
//        else {
//            // There are some statistics
//            long nextEstimatedForward = movingAverageIntervalMs / vertexCount;
//            long nextLayerWorkload = (long) (messageComputationTimeMs + getRuntimeContext().getStorage().getIncidentEdgeCount(v, EdgeType.OUT) * aggregationTimeMs);
//            if (nextEstimatedForward > nextLayerWorkload)
//                super.forward(v);
//            else
//                interLayerWindow(v, getRuntimeContext().getTimerService().currentProcessingTime() + Math.min(timerInterval, nextLayerWorkload));
//        }
//    }
//
//}
