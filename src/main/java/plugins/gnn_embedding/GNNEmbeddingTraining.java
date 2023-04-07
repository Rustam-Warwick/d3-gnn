package plugins.gnn_embedding;

import ai.djl.ndarray.*;
import ai.djl.nn.gnn.AggregatorVariant;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import elements.features.Aggregator;
import elements.features.Parts;
import elements.features.Tensor;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.tmshared.TMSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.tmshared.TMSharedStateDescriptor;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * Plugin that manages the training of {@link BaseGNNEmbedding}
 * Currently only supports: <strong>MEAN and SUM</strong> aggregator
 * </p>
 */
public class GNNEmbeddingTraining extends BaseGNNEmbedding {

    public transient Map<Short, NDArraysAggregator> part2FeatureGradientAgg;

    public transient Map<Short, NDArraysAggregator> part2AggregatorGradientAgg;

    protected transient NDList reuseFeaturesNDList;

    protected transient NDList reuseAggregatorNDList;

    protected transient Tuple3<ElementType, Object, String> reuseFeatureKey;

    protected transient Tuple3<ElementType, Object, String> reuseAggregatorKey;

    protected transient Tuple3<ElementType, Object, String> reusePartsKey;

    protected transient Short2ObjectOpenHashMap<IntArrayList> reusePart2Indices;

    protected transient IntArrayList reuseDestIndexList;

    protected transient IntArrayList reuseSrcIndexList;

    protected transient Object2IntOpenHashMap<String> reuseVertexToIndexMap;

    protected transient List<String> reuseVertexIdList;

    public GNNEmbeddingTraining(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "trainer", trainableVertexEmbeddings);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        reuseFeatureKey = new Tuple3<>(ElementType.VERTEX, null, "f");
        reusePartsKey = new Tuple3<>(ElementType.VERTEX, null, "p");
        reuseAggregatorKey = new Tuple3<>(ElementType.VERTEX, null, "agg");
        reusePart2Indices = new Short2ObjectOpenHashMap<>();
        reuseFeaturesNDList = new NDList();
        reuseAggregatorNDList = new NDList();
        reuseDestIndexList = new IntArrayList();
        reuseSrcIndexList = new IntArrayList();
        reuseVertexToIndexMap = new Object2IntOpenHashMap<>();
        reuseVertexIdList = new ArrayList<>();

        part2FeatureGradientAgg = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("part2FeatureGradientAgg", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        part2AggregatorGradientAgg = getRuntimeContext().getTaskSharedState(new TMSharedStateDescriptor<>("part2AggregatorGradientAgg", Types.GENERIC(Map.class), TMSharedGraphPerPartMapState::new));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2FeatureGradientAgg.put(part, new NDArraysAggregator()));
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2AggregatorGradientAgg.put(part, new NDArraysAggregator()));
    }

    @RemoteFunction(triggerUpdate = false)
    public void collect(String[] vertexIds, NDArray batchedGradients) {
        part2FeatureGradientAgg.get(getPart()).aggregate(vertexIds, batchedGradients);
    }

    @RemoteFunction(triggerUpdate = false)
    public void collectAggregators(String[] vertexIds, NDArray batchedGradients) {
        part2AggregatorGradientAgg.get(getPart()).aggregate(vertexIds, batchedGradients);
    }

    /**
     * <p>
     * First part of training for UPDATE
     *  <ul>
     *     <li>All vertices here are in their master parts</li>
     *      <li>All master vertices participating in train have their features. No need to double check existence </li>
     *      <li>All master vertices here have their aggregators</li>
     *  </ul>
     * </p>
     */
    public void backwardFirstPhaseMeanOrSum() {

        // 1. Check early termination and cleanup reuse objects

        NDArraysAggregator gradientAggregator = part2FeatureGradientAgg.get(getPart());
        if (gradientAggregator.isEmpty()) return;
        reuseFeaturesNDList.clear();
        reuseAggregatorNDList.clear();
        reusePart2Indices.forEach((key, val) -> val.clear());

        // 2. Populate data

        int[] meanAggregatorValues = modelServer.getBlock().getAgg() == AggregatorVariant.MEAN ? new int[gradientAggregator.keys.length] : null;
        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (int i = 0; i < gradientAggregator.keys.length; i++) {
                reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, gradientAggregator.keys[i], "f").getValue());
                Aggregator<?> val = (Aggregator<?>) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, gradientAggregator.keys[i], "agg");
                reuseAggregatorNDList.add(val.getValue());
                if (meanAggregatorValues != null) meanAggregatorValues[i] = val.getReducedCount();
                if (val.getReducedCount() > 0) {
                    reusePart2Indices.computeIfAbsent(getPart(), (ignore) -> new IntArrayList()).add(i);
                    if (getRuntimeContext().getStorage().containsAttachedFeature(ElementType.VERTEX, gradientAggregator.keys[i], "p")) {
                        for (Short replicaPart : ((Parts) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, gradientAggregator.keys[i], "p")).getValue()) {
                            reusePart2Indices.computeIfAbsent(replicaPart, (ignore) -> new IntArrayList()).add(i);
                        }
                    }
                }
                objectPoolScope.refresh();
            }
        }

        // 3. Backward pass

        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        NDArray batchedAggregators = NDArrays.stack(reuseAggregatorNDList);
        reuseFeaturesNDList.clear(); // Reuse for input
        reuseFeaturesNDList.add(batchedFeatures);
        reuseFeaturesNDList.add(batchedAggregators);
        if (!getRuntimeContext().isFirst()) batchedFeatures.setRequiresGradient(true);
        batchedAggregators.setRequiresGradient(true);
        NDList updatesBatched = UPDATE(reuseFeaturesNDList, true);
        synchronized (modelServer.getModel()) {
            // Synchronize backward calls since TaskLocal
            JniUtils.backward((PtNDArray) updatesBatched.get(0), (PtNDArray) gradientAggregator.batchedNDArray, false, false);
        }

        // 4. Send dl/dm(l-1) if not the last layer already

        if (!getRuntimeContext().isFirst()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "collect",
                    getPart(),
                    OutputTags.BACKWARD_OUTPUT_TAG,
                    gradientAggregator.keys,
                    batchedFeatures.getGradient()
            );
        }

        // 5. Collect and send aggregation messages

        final String[] keys = gradientAggregator.keys; // Take this out since we will need it afterwards
        final NDArray aggGradients = batchedAggregators.getGradient();
        if (meanAggregatorValues != null)
            aggGradients.divi(BaseNDManager.getManager().create(meanAggregatorValues).expandDims(1)); // Divide for message gradients
        reusePart2Indices.forEach((part, list) -> {
            if (list.isEmpty()) return;
            int[] intList = new int[list.size()];
            String[] vIds = new String[intList.length];
            for (int i = 0; i < intList.length; i++) {
                intList[i] = list.getInt(i);
                vIds[i] = keys[intList[i]];
            }
            NDArray batchedAggGrads = aggGradients.get(BaseNDManager.getManager().create(intList));
            Rmi.buildAndRun(getId(), getType(), "collectAggregators", part, OutputTags.ITERATE_OUTPUT_TAG, vIds, batchedAggGrads);
        });

        //6. Cleanup

        gradientAggregator.clear();
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * Second part of training responsible for MESSAGE & AGGREGATOR gradients
     */
    public void backwardSecondPhaseMeanOrSum() {

        // 1. Check early termination and clear the reuse objects

        NDArraysAggregator gradientAggregator = part2AggregatorGradientAgg.get(getPart());
        if (gradientAggregator.isEmpty()) return;
        reuseFeaturesNDList.clear();
        reusePart2Indices.forEach((key, val) -> val.clear());
        reuseDestIndexList.clear();
        reuseSrcIndexList.clear();
        reuseVertexToIndexMap.clear();
        reuseVertexIdList.clear();

        //2. Collect data

        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (int i = 0; i < gradientAggregator.keys.length; i++) {
                Vertex v = getRuntimeContext().getStorage().getVertex(gradientAggregator.keys[i]);
                try (BaseStorage.ObjectPoolScope innerObjectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
                    for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.IN, -1)) {
                        reuseFeatureKey.f1 = incidentEdge.getSrcId();
                        if (getRuntimeContext().getStorage().containsAttachedFeature(ElementType.VERTEX, incidentEdge.getSrcId(), "f")) {
                            reuseVertexToIndexMap.computeIfAbsent(incidentEdge.getSrcId(), (key) -> {
                                Tensor srcFeature = (Tensor) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, incidentEdge.getSrcId(), "f");
                                reuseFeaturesNDList.add(srcFeature.getValue());
                                reuseVertexIdList.add(incidentEdge.getSrcId());
                                reusePart2Indices.computeIfAbsent(srcFeature.getMasterPart(), (ignore) -> new IntArrayList()).add(reuseVertexIdList.size() - 1);
                                return reuseVertexIdList.size() - 1;
                            });
                            reuseDestIndexList.add(i);
                            reuseSrcIndexList.add(reuseVertexToIndexMap.getInt(incidentEdge.getSrcId()));
                        }
                        innerObjectPoolScope.refresh();
                    }
                }
                objectPoolScope.refresh();
            }
        }

        // 3. Backward pass

        if (!reuseVertexIdList.isEmpty()) {
            NDArray gradientPerEdge = gradientAggregator.batchedNDArray.get(BaseNDManager.getManager().create(reuseDestIndexList.toIntArray()));
            NDArray srcFeaturesBatched = NDArrays.stack(reuseFeaturesNDList);
            reuseFeaturesNDList.clear();
            reuseFeaturesNDList.add(srcFeaturesBatched);
            if (!getRuntimeContext().isFirst()) srcFeaturesBatched.setRequiresGradient(true);
            NDArray srcMessagesBatched = MESSAGE(reuseFeaturesNDList, true).get(0);
            NDArray edgeMessagesBatched = srcMessagesBatched.get(BaseNDManager.getManager().create(reuseSrcIndexList.toIntArray()));
            synchronized (modelServer.getModel()) {
                JniUtils.backward((PtNDArray) edgeMessagesBatched, (PtNDArray) gradientPerEdge, false, false);
            }
            if (!getRuntimeContext().isFirst()) {
                NDArray gradient = srcFeaturesBatched.getGradient();
                reusePart2Indices.forEach((part, list) -> {
                    if (list.isEmpty()) return;
                    int[] srcIndices = new int[list.size()];
                    String[] srcIds = new String[srcIndices.length];
                    for (int i = 0; i < srcIndices.length; i++) {
                        srcIndices[i] = list.getInt(i);
                        srcIds[i] = reuseVertexIdList.get(srcIndices[i]);
                    }
                    NDArray grad = gradient.get(BaseNDManager.getManager().create(srcIndices));
                    Rmi.buildAndRun(
                            getId(),
                            getType(),
                            "collect",
                            part,
                            OutputTags.BACKWARD_OUTPUT_TAG,
                            srcIds,
                            grad
                    );
                });
            }
        }

        // 4. Cleanup

        gradientAggregator.clear();
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * Zero phase that overlaps with model sync
     * Simply reset the aggregator values in storage
     */
    public void forwardResetPhase() {
        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (Feature agg : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "agg")) {
                ((Aggregator<?>) agg).reset();
                objectPoolScope.refresh();
            }
        }
        BaseNDManager.getManager().resumeAndDelay();
    }

    /**
     * First phase that computes and sends reduce messages to aggregators
     */
    public void forwardAggregatorPhase() {

        // 1. Clean the reuse data

        reuseFeaturesNDList.clear();
        reuseVertexToIndexMap.clear();
        reuseSrcIndexList.clear();
        reuseDestIndexList.clear(); // To be used as counts of aggregators
        reuseVertexIdList.clear();
        reusePart2Indices.forEach((key, val) -> val.clear());

        // 2. Collect data

        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (Vertex vertex : getRuntimeContext().getStorage().getVertices()) {
                boolean hasInEdges = false;
                try (BaseStorage.ObjectPoolScope innerObjetPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
                    for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges(vertex, EdgeType.IN, -1)) {
                        reuseFeatureKey.f1 = incidentEdge.getSrcId();
                        if (getRuntimeContext().getStorage().containsAttachedFeature(ElementType.VERTEX, incidentEdge.getSrcId(), "f")) {
                            reuseVertexToIndexMap.computeIfAbsent(incidentEdge.getSrcId(), (key) -> {
                                reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, incidentEdge.getSrcId(), "f").getValue());
                                return reuseFeaturesNDList.size() - 1;
                            });
                            reuseSrcIndexList.add(reuseVertexToIndexMap.getInt(incidentEdge.getSrcId()));
                            hasInEdges = true;
                        }
                        innerObjetPoolScope.refresh();
                    }
                }
                if (hasInEdges) {
                    reuseSrcIndexList.add(-1);
                    reuseVertexIdList.add(vertex.getId());
                    reusePart2Indices.computeIfAbsent(vertex.getMasterPart(), (ignore) -> new IntArrayList()).add(reuseVertexIdList.size() - 1);
                }
                objectPoolScope.refresh();
            }
        }

        // 3. Compute the messages

        if (reuseFeaturesNDList.isEmpty()) return;
        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        reuseFeaturesNDList.clear();
        reuseFeaturesNDList.add(batchedFeatures);
        NDArray batchedSrcMessages = MESSAGE(reuseFeaturesNDList, false).get(0);
        reuseFeaturesNDList.clear();

        // 4. Partial aggregate and collect this data

        int start = 0;
        final int[] dim = new int[]{0};
        for (int i = 0; i < reuseSrcIndexList.size(); i++) {
            if (reuseSrcIndexList.getInt(i) == -1) {
                int[] indices = new int[i - start];
                System.arraycopy(reuseSrcIndexList.elements(), start, indices, 0, indices.length);
                NDArray result = batchedSrcMessages.get(BaseNDManager.getManager().create(indices)).sum(dim);
                reuseFeaturesNDList.add(result);
                reuseDestIndexList.add(indices.length);
                start = i + 1;
            }
        }
        NDArray batchPartialAggregators = NDArrays.stack(reuseFeaturesNDList);

        // 5. Send messages

        reusePart2Indices.forEach((part, list) -> {
            if (list.isEmpty()) return;
            int[] indices = new int[list.size()];
            int[] counts = new int[list.size()];
            String[] vertexIds = new String[list.size()];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = list.getInt(i);
                counts[i] = reuseDestIndexList.getInt(indices[i]);
                vertexIds[i] = reuseVertexIdList.get(indices[i]);
            }
            NDArray partBatchedPartialAggregators = batchPartialAggregators.get(BaseNDManager.getManager().create(indices));
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "receiveBatchedReduceMessages",
                    part,
                    OutputTags.ITERATE_OUTPUT_TAG,
                    vertexIds,
                    counts,
                    partBatchedPartialAggregators
            );
        });

        // 6. Cleanup

        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * Paired with first forward phase to reduce the vertices
     */
    @RemoteFunction(triggerUpdate = false)
    public void receiveBatchedReduceMessages(String[] vertexIds, int[] counts, NDArray batchedPartialAggregators) {
        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (int i = 0; i < vertexIds.length; i++) {
                reuseAggregatorKey.f1 = vertexIds[i];
                reuseAggregatorNDList.clear();
                reuseAggregatorNDList.add(batchedPartialAggregators.get(i));
                Rmi.execute(getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, vertexIds[i], "agg"), "reduce", reuseAggregatorNDList, counts[i]);
                objectPoolScope.refresh();
            }
        }
    }

    /**
     * Second phase that UPDATES the MASTER vertices and sends to the next layer
     */
    public void forwardUpdatePhase() {
        reuseFeaturesNDList.clear();
        reuseAggregatorNDList.clear();
        reuseVertexIdList.clear();
        try (BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
                if (f.state() == ReplicaState.MASTER) {
                    // Master vertices with feature
                    reuseFeaturesNDList.add((NDArray) f.getValue());
                    reuseAggregatorKey.f1 = f.getAttachedElementId();
                    reuseAggregatorNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(ElementType.VERTEX, f.getAttachedElementId(), "agg").getValue());
                    reuseVertexIdList.add((String) f.getAttachedElementId());
                }
                objectPoolScope.refresh();
            }
        }
        if (reuseFeaturesNDList.isEmpty()) return;
        NDArray batchedFeatures = NDArrays.stack(reuseFeaturesNDList);
        NDArray batchedAggregators = NDArrays.stack(reuseAggregatorNDList);
        reuseFeaturesNDList.clear();
        reuseFeaturesNDList.add(batchedFeatures);
        reuseFeaturesNDList.add(batchedAggregators);
        NDArray updatesBatched = UPDATE(reuseFeaturesNDList, false).get(0);
        Tensor reuseFeature = new Tensor("f", null, false, getPart());
        reuseFeature.id.f0 = ElementType.VERTEX;
        for (int i = 0; i < reuseVertexIdList.size(); i++) {
            reuseFeature.id.f1 = reuseVertexIdList.get(i);
            reuseFeature.value = updatesBatched.get(i);
            getRuntimeContext().output(new GraphOp(Op.COMMIT, getRuntimeContext().getCurrentPart(), reuseFeature));
        }

        BaseNDManager.getManager().resumeAndDelay();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof TrainingSubCoordinator.BackwardPhaser) {
            if (((TrainingSubCoordinator.BackwardPhaser) evt).isSecondPhase)
                getRuntimeContext().runForAllLocalParts(this::backwardSecondPhaseMeanOrSum);
            else getRuntimeContext().runForAllLocalParts(this::backwardFirstPhaseMeanOrSum);
        } else if (evt instanceof TrainingSubCoordinator.ForwardPhaser) {
            switch (((TrainingSubCoordinator.ForwardPhaser) evt).iteration) {
                case 1:
                    getRuntimeContext().runForAllLocalParts(this::forwardResetPhase);
                    break;
                case 2:
                    getRuntimeContext().runForAllLocalParts(this::forwardAggregatorPhase);
                    break;
                case 3:
                    getRuntimeContext().runForAllLocalParts(this::forwardUpdatePhase);
                    break;
            }
        }
    }
}
