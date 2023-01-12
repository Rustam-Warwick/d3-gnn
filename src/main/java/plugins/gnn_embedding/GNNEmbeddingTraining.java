package plugins.gnn_embedding;

import ai.djl.ndarray.*;
import ai.djl.ndarray.index.NDIndex;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.gnn.AggregatorVariant;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import elements.annotations.RemoteFunction;
import elements.enums.EdgeType;
import elements.enums.ElementType;
import elements.enums.Op;
import elements.enums.ReplicaState;
import elements.features.Aggregator;
import elements.features.CountTensorHolder;
import elements.features.Parts;
import elements.features.Tensor;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.GraphStorage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *      Plugin that manages the training of {@link BaseGNNEmbeddings}
 * </p>
 */
public class GNNEmbeddingTraining extends BaseGNNEmbeddings {

    public transient Map<Short, NDArraysAggregator> part2GradientAggregators;

    public GNNEmbeddingTraining(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "trainer", trainableVertexEmbeddings);
    }

    @Override
    public synchronized void open(Configuration parameters) throws Exception {
        super.open(parameters);
        part2GradientAggregators = part2GradientAggregators == null ? new Short2ObjectOpenHashMap<>(): part2GradientAggregators;
        getRuntimeContext().getThisOperatorParts().forEach(part -> part2GradientAggregators.put(part, new NDArraysAggregator()));
    }

    /**
     * Collect vertex -> dLoss/doutput, where vertices are masters in this part
     */
    @RemoteFunction(triggerUpdate = false)
    public void collect(String[] vertexIds, NDArray batchedGradients) {
        part2GradientAggregators.get(getPart()).aggregate(vertexIds, batchedGradients);
    }

    /**
     * First part of training responsible for getting triggerUpdate gradients and output gradients
     * <p>
     * Assumes to contain only gradients for master vertices with aggregators allocated already
     * </p>
     */
    public void backwardFirstPhaseWithMeanAggregator() {
        // ------------- Fast return check

        NDArraysAggregator collectedGradients = part2GradientAggregators.get(getPart());
        if(collectedGradients.isEmpty()) return;

        // ---------- Prepare data

        NDList featuresList = new NDList(collectedGradients.keys.length);
        NDList aggregatorList = new NDList(collectedGradients.keys.length);
        int[] meanAggregatorValues = new int[collectedGradients.keys.length];
        Tuple3<ElementType, Object, String> featureReuseKey = new Tuple3<>(ElementType.VERTEX, null, "f");
        Tuple3<ElementType, Object, String> aggReuseKey = new Tuple3<>(ElementType.VERTEX, null, "agg");
        Tuple3<ElementType, Object, String> partsReuseKey = new Tuple3<>(ElementType.VERTEX, null, "p");
        Short2ObjectOpenHashMap<IntList> part2Vertices = new Short2ObjectOpenHashMap<>(new short[]{getPart()}, new IntList[]{new IntArrayList()});
        for (int i = 0; i < collectedGradients.keys.length; i++) {
            final int vertexIndex = i;
            featureReuseKey.f1 = collectedGradients.keys[i];
            aggReuseKey.f1 = collectedGradients.keys[i];
            partsReuseKey.f1 = collectedGradients.keys[i];
            featuresList.add((NDArray) getRuntimeContext().getStorage().getAttachedFeature(featureReuseKey).getValue());
            CountTensorHolder val = (CountTensorHolder) getRuntimeContext().getStorage().getAttachedFeature(aggReuseKey).value;
            aggregatorList.add(val.val);
            meanAggregatorValues[i] = val.count;
            if(val.count > 0 && getRuntimeContext().getStorage().containsAttachedFeature(partsReuseKey)) {
                    part2Vertices.get(getPart()).add(vertexIndex);
                    for (Short replicaPart : ((Parts) getRuntimeContext().getStorage().getAttachedFeature(partsReuseKey)).getValue()) {
                    part2Vertices.compute(replicaPart, (key, vertexIndices)->{
                        if(vertexIndices == null) vertexIndices = new IntArrayList();
                        vertexIndices.add(vertexIndex);
                        return vertexIndices;
                    });
                }
            }
        }

        NDList batchedInputs = new NDList(NDArrays.stack(featuresList), NDArrays.stack(aggregatorList));
        batchedInputs.get(0).setRequiresGradient(true);
        batchedInputs.get(1).setRequiresGradient(true);

        // --------------- Backward pass

        NDList batchedPredictions = ((GNNBlock) modelServer.getModel().getBlock()).update(modelServer.getParameterStore(), batchedInputs, true);

        synchronized (this){
            // Synchronize backward calls since TaskLocal
            JniUtils.backward((PtNDArray) batchedPredictions.get(0), (PtNDArray) collectedGradients.batchedNDArray, false, false);
        }

        NDList backwardGradients = new NDList(batchedInputs.get(0).getGradient(), batchedInputs.get(1).getGradient());

        // -------------- Send backward messages if it exists and clear gradients

        if (!getRuntimeContext().isFirst()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "collect",
                    getPart(),
                    OutputTags.BACKWARD_OUTPUT_TAG,
                    collectedGradients.keys,
                    backwardGradients.get(0)
            );
        }

        collectedGradients.clear(); // Clear so that the same guy is re-used for storing aggregator gradients


        // ------------------Collect Aggregation messages + Backward messages(If not the first layer)

        backwardGradients.get(1).divi(BaseNDManager.getManager().create(meanAggregatorValues).expandDims(1)); // Divide to get message gradiets

        part2Vertices.forEach((part, list)->{
            String[] vIds = list.intStream().mapToObj(pos -> collectedGradients.keys[pos]).toArray(String[]::new);
            NDArray batchedAggGrads = backwardGradients.get(1).get(BaseNDManager.getManager().create(list.toIntArray()));
            Rmi.buildAndRun(getId(), getType(), "collect", part, OutputTags.ITERATE_OUTPUT_TAG, vIds, batchedAggGrads);
        });
        
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * Second part of training responsible for getting output gradients
     * <p>
     * Batch compute gradients for output function as well as previous layer updates
     * Previous layer updates are needed only if this is not First layer of GNN
     * </p>
     */
    public void backwardSecondPhase() {
            if(true) return;
            if (!part2GradientAggregators.containsKey(getPart())) return;
            NDArrayCollector<String> collectedAggregators = null;
            if (collectedAggregators.isEmpty()) return;

            NDList features = new NDList(collectedAggregators.size());
            NDList gradients = new NDList(collectedAggregators.size());
            List<Vertex> vertices = new ArrayList<>(collectedAggregators.size());
            for (Map.Entry<String, NDArray> stringNDArrayEntry : collectedAggregators.entrySet()) {
                Vertex v = getRuntimeContext().getStorage().getVertex(stringNDArrayEntry.getKey());
                vertices.add(v);
                features.add((NDArray) v.getFeature("f").getValue());
                gradients.add(stringNDArrayEntry.getValue());
            }
            NDList batchedFeatures = new NDList(NDArrays.stack(features));
            batchedFeatures.get(0).setRequiresGradient(true);
            NDList batchedMessageGradients = new NDList(NDArrays.stack(gradients));
            NDList messages = MESSAGE(batchedFeatures, true);
            JniUtils.backward((PtNDArray) messages.get(0), (PtNDArray) batchedMessageGradients.get(0), false, false);
            NDArray batchedFeatureGradients = batchedFeatures.get(0).getGradient();
            if (!getRuntimeContext().isFirst()) {
                HashMap<Short, NDArrayCollector<String>> backwardGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
                for (int i = 0; i < vertices.size(); i++) {
                    backwardGradsPerPart.computeIfAbsent(vertices.get(i).getMasterPart(), (key) -> new NDArrayCollector<>(false));
                    backwardGradsPerPart.get(vertices.get(i).getMasterPart()).put(vertices.get(i).getId(), batchedFeatureGradients.get(i));
                }
                for (Map.Entry<Short, NDArrayCollector<String>> entry : backwardGradsPerPart.entrySet()) {
                    Rmi.buildAndRun(
                            getId(),
                            getType(),
                            "collect",
                            entry.getKey(),
                            OutputTags.BACKWARD_OUTPUT_TAG,
                            entry.getValue()
                    );
                }
            }
            collectedAggregators.clear();
    }

    /**
     * <p>
     *     Zero phase that overlaps with mode sync
     *     Simply reset the aggregator values in storage
     * </p>
     */
    public void forwardZeroPhase(){
        for (Feature agg : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "agg")) {
            ((Aggregator<?>) agg).reset();
        }
    }

    /**
     * Do the first aggregation cycle of the inference.
     * <p>
     * Reset local aggregators, Collect local vertices, batch messages going to a vertex and send RMI reduce messages
     * </p>
     */
    public void forwardFirstPhase() {
        Object2IntOpenHashMap<String> srcVertex2PosMap = new Object2IntOpenHashMap<>();
        HashMap<Tuple2<String, Short>, IntArrayList> destVertex2SrcIndicesMap = new HashMap<>();
        NDList srcFeatures = new NDList();
        Tuple3<ElementType, Object, String> reuse = Tuple3.of(ElementType.VERTEX, null, "f");
        IntArrayList reuse2 = new IntArrayList();
        for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
            for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges((Vertex) f.getElement(), EdgeType.IN)) {
                if (!srcVertex2PosMap.containsKey(incidentEdge.getSrcId())) {
                    srcVertex2PosMap.put(incidentEdge.getSrcId(), srcFeatures.size());
                    srcFeatures.add((NDArray) f.getValue());
                }
                reuse2.add(srcVertex2PosMap.getInt(incidentEdge.getSrcId()));
            }
            if(!reuse2.isEmpty()){
                destVertex2SrcIndicesMap.put(Tuple2.of((String) f.getAttachedElementId(), f.getMasterPart()), reuse2.clone());
                reuse2.clear();
            }
        }
        if (srcFeatures.isEmpty()) return;
        NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
        NDArray messages = MESSAGE(srcFeaturesBatched, false).get(0);
        destVertex2SrcIndicesMap.forEach((vInfo, list) -> {
            NDArray message = messages.get(BaseNDManager.getManager().create(list.elements())).sum(new int[]{0});
            Rmi.buildAndRun(
                    Tuple3.of(ElementType.VERTEX, vInfo.f0, "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "reduce",
                    vInfo.f1,
                    OutputTags.ITERATE_OUTPUT_TAG,
                    new NDList(message),
                    list.size()
            );
        });
        BaseNDManager.getManager().resumeAndDelay();
    }

    /**
     * Forward all local MASTER Vertices to the next layer
     */
    public void forwardSecondPhase() {
        NDList features = new NDList();
        NDList aggregators = new NDList();
        List<String> vertexIds = new ArrayList<>();
        Tuple3<ElementType, Object, String> reuse = Tuple3.of(ElementType.VERTEX, null, "agg");
        for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
            if(f.getElement().state() == ReplicaState.MASTER){
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
            getRuntimeContext().output(new GraphOp(Op.UPDATE, getRuntimeContext().getCurrentPart(), reuse3));
        }
        BaseNDManager.getManager().resumeAndDelay();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if(evt instanceof TrainingSubCoordinator.BackwardPhaser){
            if(!((TrainingSubCoordinator.BackwardPhaser) evt).isSecondPhase){
                try(GraphStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()){getRuntimeContext().runForAllLocalParts(modelServer.getBlock().getAgg() == AggregatorVariant.MEAN?this::backwardFirstPhaseWithMeanAggregator:null);}
            }
            else getRuntimeContext().runForAllLocalParts(this::backwardSecondPhase);
        }
        else if(evt instanceof TrainingSubCoordinator.ForwardPhaser){
            switch (((TrainingSubCoordinator.ForwardPhaser) evt).iteration){
                case 1:
                    try(GraphStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardZeroPhase);}
                    break;
                case 2:
                    try(GraphStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardFirstPhase);}
                    break;
                case 3:
                    try(GraphStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardSecondPhase);}
                    break;
            }
        }
    }
}
