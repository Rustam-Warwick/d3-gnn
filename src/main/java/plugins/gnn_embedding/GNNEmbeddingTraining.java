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
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.state.taskshared.TaskSharedGraphPerPartMapState;
import org.apache.flink.runtime.state.taskshared.TaskSharedStateDescriptor;
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

    protected transient Short2ObjectOpenHashMap<IntList> reusePart2VertexIndices;

    protected transient IntList reuseDestIndexList;

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
        reusePart2VertexIndices = new Short2ObjectOpenHashMap<>();
        reuseFeaturesNDList = new NDList();
        reuseAggregatorNDList = new NDList();
        reuseDestIndexList = new IntArrayList();
        reuseVertexIdList = new ArrayList<>();
        part2FeatureGradientAgg = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("part2FeatureGradientAgg", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
        part2AggregatorGradientAgg = getRuntimeContext().getTaskSharedState(new TaskSharedStateDescriptor<>("part2AggregatorGradientAgg", Types.GENERIC(Map.class), TaskSharedGraphPerPartMapState::new));
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
     * First part of training responsible for getting triggerUpdate gradients and output gradients
     * <p>
     * Assumptions:
     *      <ul>
     *          <li>All vertices here are in their master parts</li>
     *          <li>All master vertices participating in train have their features </li>
     *          <li>All master vertices here have their aggregators</li>
     *      </ul>
     * </p>
     */
    public void backwardFirstPhaseMeanOrSum() {
        // 1. Check early termination and cleanup reuse objects
        NDArraysAggregator collectedGradients = part2FeatureGradientAgg.get(getPart());
        if (collectedGradients.isEmpty()) return;
        reuseFeaturesNDList.clear();
        reuseAggregatorNDList.clear();
        reusePart2VertexIndices.forEach((key, val) -> val.clear());

        // 2. Populate data
        int[] meanAggregatorValues = modelServer.getBlock().getAgg() == AggregatorVariant.MEAN ? new int[collectedGradients.keys.length] : null;
        try(BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (int i = 0; i < collectedGradients.keys.length; i++) {
                reuseFeatureKey.f1 = reusePartsKey.f1 = reuseAggregatorKey.f1 = collectedGradients.keys[i];
                reuseFeaturesNDList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuseFeatureKey).getValue());
                Aggregator<?> val = (Aggregator<?>) getRuntimeContext().getStorage().getAttachedFeature(reuseAggregatorKey);
                reuseAggregatorNDList.add(val.getValue());
                if (meanAggregatorValues != null) meanAggregatorValues[i] = val.getReducedCount();
                if (val.getReducedCount() > 0) {
                    reusePart2VertexIndices.computeIfAbsent(getPart(), (ignore) -> new IntArrayList()).add(i);
                    if (getRuntimeContext().getStorage().containsAttachedFeature(reusePartsKey)) {
                        for (Short replicaPart : ((Parts) getRuntimeContext().getStorage().getAttachedFeature(reusePartsKey)).getValue()) {
                            reusePart2VertexIndices.computeIfAbsent(replicaPart, (ignore) -> new IntArrayList()).add(i);
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
            JniUtils.backward((PtNDArray) updatesBatched.get(0), (PtNDArray) collectedGradients.batchedNDArray, false, false);
        }

        // 4. Send dl/dm(l-1) if not the last layer already
        if (!getRuntimeContext().isFirst()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "collect",
                    getPart(),
                    OutputTags.BACKWARD_OUTPUT_TAG,
                    collectedGradients.keys,
                    batchedFeatures.getGradient()
            );
        }

        // 5. Collect and send aggregation messages
        final String[] keys = collectedGradients.keys; // Take this out since we will need it afterwards
        NDArray aggGradients = batchedAggregators.getGradient();
        if (meanAggregatorValues != null) aggGradients.divi(BaseNDManager.getManager().create(meanAggregatorValues).expandDims(1)); // Divide for message gradients
        reusePart2VertexIndices.forEach((part, list) -> {
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
        collectedGradients.clear();
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * Second part of training responsible for getting output gradients
     * <p>
     * Batch compute gradients for output function as well as previous layer updates
     * Previous layer updates are needed only if this is not First layer of GNN
     * </p>
     */
    public void backwardSecondPhaseMeanOrSum() {
        // 1. Check early termination and clear the reuse objects
        NDArraysAggregator collectedGradients = part2AggregatorGradientAgg.get(getPart());
        if (collectedGradients.isEmpty()) return;
        reuseFeaturesNDList.clear();
        reusePart2VertexIndices.forEach((key, val) -> val.clear());
        reuseDestIndexList.clear();
        reuseVertexIdList.clear();

        //2. Collect data
        try(BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (int i = 0; i < collectedGradients.keys.length; i++) {
                Vertex v = getRuntimeContext().getStorage().getVertex(collectedGradients.keys[i]);
                try(BaseStorage.ObjectPoolScope innerObjectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
                    for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.IN)) {
                        reuseFeatureKey.f1 = incidentEdge.getSrcId();
                        if (getRuntimeContext().getStorage().containsAttachedFeature(reuseFeatureKey)) {
                            Tensor srcFeature = (Tensor) getRuntimeContext().getStorage().getAttachedFeature(reuseFeatureKey);
                            reusePart2VertexIndices.computeIfAbsent(srcFeature.getMasterPart(), (ignore) -> new IntArrayList()).add(reuseFeaturesNDList.size());
                            reuseFeaturesNDList.add(srcFeature.getValue());
                            reuseDestIndexList.add(i);
                            reuseVertexIdList.add(incidentEdge.getSrcId());
                        }
                        innerObjectPoolScope.refresh();
                    }
                }
                objectPoolScope.refresh();
            }
        }

        // 3. Backward pass
        if (!reuseVertexIdList.isEmpty()) {
            NDArray gradientPerMessage = collectedGradients.batchedNDArray.get(BaseNDManager.getManager().create(reuseDestIndexList.toIntArray()));
            NDArray srcFeaturesBatched = NDArrays.stack(reuseFeaturesNDList);
            reuseFeaturesNDList.clear();
            reuseFeaturesNDList.add(srcFeaturesBatched);
            if (!getRuntimeContext().isFirst()) srcFeaturesBatched.setRequiresGradient(true);
            NDList messagesBatched = MESSAGE(reuseFeaturesNDList, true);
            synchronized (modelServer.getModel()) {
                JniUtils.backward((PtNDArray) messagesBatched.get(0), (PtNDArray) gradientPerMessage, false, false);
            }
            if (!getRuntimeContext().isFirst()) {
                NDArray gradient = srcFeaturesBatched.getGradient();
                reusePart2VertexIndices.forEach((part, list) -> {
                    if(list.isEmpty()) return;
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
        collectedGradients.clear();
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * <p>
     * Zero phase that overlaps with mode sync
     * Simply reset the aggregator values in storage
     * </p>
     */
    public void forwardResetPhase() {
        try(BaseStorage.ObjectPoolScope objectPoolScope = getRuntimeContext().getStorage().openObjectPoolScope()) {
            for (Feature agg : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "agg")) {
                ((Aggregator<?>) agg).reset();
                objectPoolScope.refresh();
            }
        }
    }

    /**
     * Do the first aggregation cycle of the inference.
     * <p>
     * Reset local aggregators, Collect local vertices, batch messages going to a vertex and send RMI reduce messages
     * </p>
     */
    public void forwardAggregatorPhase() {
        Object2IntOpenHashMap<String> srcVertex2PosMap = new Object2IntOpenHashMap<>();
        Short2ObjectOpenHashMap<ObjectArrayList<Tuple2<String, IntArrayList>>> part2AggregatingVertices = new Short2ObjectOpenHashMap<>();
        NDList srcFeatures = new NDList();
        Tuple3<ElementType, Object, String> featureAccessKeyReuse = Tuple3.of(ElementType.VERTEX, null, "f");
        IntArrayList reuseList = new IntArrayList();
        for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
            try (BaseStorage.ObjectPoolScope ignored = getRuntimeContext().getStorage().openObjectPoolScope()) {
                for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges((Vertex) f.getElement(), EdgeType.IN)) {
                    // Add vertex in-edges to srcVertex2PosMap and add their id to reuse
                    if (!srcVertex2PosMap.containsKey(incidentEdge.getSrcId())) {
                        srcVertex2PosMap.put(incidentEdge.getSrcId(), srcFeatures.size());
                        featureAccessKeyReuse.f1 = incidentEdge.getSrcId();
                        srcFeatures.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(featureAccessKeyReuse).getValue());
                    }
                    reuseList.add(srcVertex2PosMap.getInt(incidentEdge.getSrcId()));
                }
            }
            if (!reuseList.isEmpty()) {
                part2AggregatingVertices.compute(f.getMasterPart(), (part, list) -> {
                    if (list == null) list = new ObjectArrayList<>();
                    list.add(Tuple2.of((String) f.getAttachedElementId(), reuseList.clone()));
                    return list;
                });
                reuseList.clear();
            }
        }
        if (srcFeatures.isEmpty()) return;
        NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
        NDArray messages = MESSAGE(srcFeaturesBatched, false).get(0);
        part2AggregatingVertices.forEach((part, vertices) -> {
            for (Tuple2<String, IntArrayList> vertex : vertices) {
                NDArray message = messages.get(BaseNDManager.getManager().create(vertex.f1.elements())).sum(new int[]{0});
                Rmi.buildAndRun(
                        Tuple3.of(ElementType.VERTEX, vertex.f0, "agg"),
                        ElementType.ATTACHED_FEATURE,
                        "reduce",
                        part,
                        OutputTags.ITERATE_OUTPUT_TAG,
                        new NDList(message),
                        vertex.f1.size()
                );
            }
        });
        BaseNDManager.getManager().resumeAndDelay();
    }

    /**
     * Forward all local MASTER Vertices to the next layer
     */
    public void forwardUpdatePhase() {
        NDList features = new NDList();
        NDList aggregators = new NDList();
        List<String> vertexIds = new ArrayList<>();
        Tuple3<ElementType, Object, String> reuse = Tuple3.of(ElementType.VERTEX, null, "agg");
        for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
            if (f.getElement().state() == ReplicaState.MASTER) {
                // Master vertices with feature
                reuse.f1 = f.getAttachedElementId();
                features.add((NDArray) f.getValue());
                aggregators.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(reuse).getValue());
                vertexIds.add((String) f.getAttachedElementId());
            }
        }
        NDList inputsBatched = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
        NDArray updatesBatched = UPDATE(inputsBatched, false).get(0);
        Tensor reuse3 = new Tensor("f", null, false, (short) -1);
        reuse3.id.f0 = ElementType.VERTEX;
        for (int i = 0; i < vertexIds.size(); i++) {
            reuse3.id.f1 = vertexIds.get(i);
            reuse3.value = updatesBatched.get(i);
            getRuntimeContext().output(new GraphOp(Op.COMMIT, getRuntimeContext().getCurrentPart(), reuse3));
        }
        BaseNDManager.getManager().resumeAndDelay();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof TrainingSubCoordinator.BackwardPhaser) {
            if(((TrainingSubCoordinator.BackwardPhaser) evt).isSecondPhase) getRuntimeContext().runForAllLocalParts(modelServer.getBlock().getAgg() == AggregatorVariant.SUM || modelServer.getBlock().getAgg() == AggregatorVariant.MEAN ? this::backwardSecondPhaseMeanOrSum : null);
            else getRuntimeContext().runForAllLocalParts(modelServer.getBlock().getAgg() == AggregatorVariant.SUM || modelServer.getBlock().getAgg() == AggregatorVariant.MEAN ? this::backwardFirstPhaseMeanOrSum : null);
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
