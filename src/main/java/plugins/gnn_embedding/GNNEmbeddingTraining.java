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
import elements.features.CountTensorHolder;
import elements.features.Parts;
import elements.features.Tensor;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.shorts.Short2ObjectOpenHashMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import org.apache.flink.streaming.api.operators.graph.TrainingSubCoordinator;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *      Plugin that manages the training of {@link BaseGNNEmbeddings}
 *      Currently only supports: <strong>MEAN and SUM</strong> aggregator
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
     * Collect gradients per vertex string, where vertices are masters in this part
     */
    @RemoteFunction(triggerUpdate = false)
    public void collect(String[] vertexIds, NDArray batchedGradients) {
        part2GradientAggregators.get(getPart()).aggregate(vertexIds, batchedGradients);
    }

    /**
     * First part of training responsible for getting triggerUpdate gradients and output gradients
     * <p>
     *      Assumptions:
     *      <ul>
     *          <li>All vertices here are in their master parts</li>
     *          <li>All master vertices participating in train have their features </li>
     *          <li>All master vertices here have their aggregators</li>
     *      </ul>
     * </p>
     */
    public void backwardFirstPhaseMeanOrSum() {
        // ------------- Fast return check
        NDArraysAggregator collectedGradients = part2GradientAggregators.get(getPart());
        if(collectedGradients.isEmpty()) return;

        // ---------- Prepare data

        NDList featuresList = new NDList(collectedGradients.keys.length);
        NDList aggregatorList = new NDList(collectedGradients.keys.length);
        int[] meanAggregatorValues = modelServer.getBlock().getAgg() == AggregatorVariant.MEAN ? new int[collectedGradients.keys.length]: null;
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
            if(meanAggregatorValues!=null) meanAggregatorValues[i] = val.count;
            if(val.count > 0) {
                    part2Vertices.get(getPart()).add(vertexIndex);
                    if(getRuntimeContext().getStorage().containsAttachedFeature(partsReuseKey)){
                        for (Short replicaPart : ((Parts) getRuntimeContext().getStorage().getAttachedFeature(partsReuseKey)).getValue()) {
                            part2Vertices.compute(replicaPart, (key, vertexIndices) -> {
                                if (vertexIndices == null) vertexIndices = new IntArrayList();
                                vertexIndices.add(vertexIndex);
                                return vertexIndices;
                            });
                        }
                    }
            }
        }

        NDList batchedInputs = new NDList(NDArrays.stack(featuresList), NDArrays.stack(aggregatorList));
        if(!getRuntimeContext().isFirst()) batchedInputs.get(0).setRequiresGradient(true);
        batchedInputs.get(1).setRequiresGradient(true);

        // --------------- Backward pass
        NDList updatesBatched = UPDATE(batchedInputs, true);

        synchronized (this){
            // Synchronize backward calls since TaskLocal
            JniUtils.backward((PtNDArray) updatesBatched.get(0), (PtNDArray) collectedGradients.batchedNDArray, false, false);
        }


        // -------------- Send backward messages if it exists and clear gradients

        if (!getRuntimeContext().isFirst()) {
            Rmi.buildAndRun(
                    getId(),
                    getType(),
                    "collect",
                    getPart(),
                    OutputTags.BACKWARD_OUTPUT_TAG,
                    collectedGradients.keys,
                    batchedInputs.get(0).getGradient()
            );
        }

        String[] keys = collectedGradients.keys; // Take this out since we will need it afterwards
        collectedGradients.clear();

        NDArray aggGradients = batchedInputs.get(1).getGradient();

        // ------------------Collect Aggregation messages + Backward messages(If not the first layer)

        if(meanAggregatorValues != null)
            aggGradients.divi(BaseNDManager.getManager().create(meanAggregatorValues).expandDims(1));

        part2Vertices.forEach((part, list)->{
            if(list.isEmpty()) return;
            int[] intList = new int[list.size()];
            String[] vIds = new String[intList.length];
            for (int i = 0; i < intList.length; i++) {
                intList[i] = list.getInt(i);
                vIds[i] = keys[intList[i]];
            }
            NDArray batchedAggGrads = aggGradients.get(BaseNDManager.getManager().create(intList));
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
    public void backwardSecondPhaseMeanOrSum() {
        NDArraysAggregator collectedGradients = part2GradientAggregators.get(getPart());
        if(collectedGradients.isEmpty()) return;
        Tuple3<ElementType, Object, String> featureAccessKey = Tuple3.of(ElementType.VERTEX, null, "f");
        Object2IntOpenHashMap<String> srcIndex = new Object2IntOpenHashMap<>();
        Short2ObjectOpenHashMap<IntList> part2SrcIndices = new Short2ObjectOpenHashMap<>();
        NDList srcFeatures = new NDList();
        ObjectArrayList<String> srcVertexIds = new ObjectArrayList<>();
        NDArray resultingGradient = null;
        for (int i = 0; i < collectedGradients.keys.length ; i++) {
            for (DirectedEdge incidentEdge : getRuntimeContext().getStorage().getIncidentEdges(getRuntimeContext().getStorage().getVertex(collectedGradients.keys[i]), EdgeType.IN)) {
                int index = srcIndex.getOrDefault(incidentEdge.getSrcId(), -1);
                if(index == -1){
                    // Src does not exist yet
                    featureAccessKey.f1 = incidentEdge.getSrcId();
                    try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {
                        if (getRuntimeContext().getStorage().containsAttachedFeature(featureAccessKey)) {
                            // Src has a feature in storage
                            Tensor srcFeature = (Tensor) getRuntimeContext().getStorage().getAttachedFeature(featureAccessKey);
                            srcIndex.put(incidentEdge.getSrcId(), srcFeatures.size());
                            part2SrcIndices.compute(srcFeature.getMasterPart(), (part, list) -> {
                                        if (list == null) list = new IntArrayList();
                                        list.add(srcFeatures.size());
                                        return list;
                                    }
                            );
                            srcFeatures.add(srcFeature.getValue());
                            srcVertexIds.add(incidentEdge.getSrcId());
                            if (resultingGradient == null)
                                resultingGradient = collectedGradients.batchedNDArray.get(i).expandDims(0);
                            else
                                resultingGradient = resultingGradient.concat(collectedGradients.batchedNDArray.get(i).expandDims(0), 0);
                        }
                    }
                }else{
                    // Src was already added
                    resultingGradient.get(index).addi(collectedGradients.batchedNDArray.get(i));
                }
            }
        }

        if(!srcFeatures.isEmpty()) {
//
            NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
            if (!getRuntimeContext().isFirst()) srcFeaturesBatched.get(0).setRequiresGradient(true);
            NDList messagesBatched = MESSAGE(srcFeaturesBatched, true);

            synchronized (this) {
                JniUtils.backward((PtNDArray) messagesBatched.get(0), (PtNDArray) resultingGradient, false, false);
            }

            if (!getRuntimeContext().isFirst()) {
                NDArray gradient = srcFeaturesBatched.get(0).getGradient();
                part2SrcIndices.forEach((part, list) -> {
                    int[] srcIndices = new int[list.size()];
                    String[] srcIds = new String[srcIndices.length];
                    for (int i = 0; i < srcIndices.length; i++) {
                        srcIndices[i] = list.getInt(i);
                        srcIds[i] = srcVertexIds.get(srcIndices[i]);
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
        collectedGradients.clear();
        BaseNDManager.getManager().resumeAndDelay();

    }

    /**
     * <p>
     *     Zero phase that overlaps with mode sync
     *     Simply reset the aggregator values in storage
     * </p>
     */
    public void forwardResetPhase(){
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
    public void forwardAggregatorPhase() {
        Object2IntOpenHashMap<String> srcVertex2PosMap = new Object2IntOpenHashMap<>();
        Short2ObjectOpenHashMap<ObjectArrayList<Tuple2<String, IntArrayList>>> part2AggregatingVertices = new Short2ObjectOpenHashMap<>();
        NDList srcFeatures = new NDList();
        Tuple3<ElementType, Object, String> featureAccessKeyReuse = Tuple3.of(ElementType.VERTEX, null, "f");
        IntArrayList reuseList = new IntArrayList();
        for (Feature f : getRuntimeContext().getStorage().getAttachedFeatures(ElementType.VERTEX, "f")) {
            try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()){
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
            if(!reuseList.isEmpty()){
                part2AggregatingVertices.compute(f.getMasterPart(), (part, list)->{
                    if(list == null) list = new ObjectArrayList<>();
                    list.add(Tuple2.of((String) f.getAttachedElementId(), reuseList.clone()));
                    return list;
                });
                reuseList.clear();
            }
        }
        if (srcFeatures.isEmpty()) return;
        NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
        NDArray messages = MESSAGE(srcFeaturesBatched, false).get(0);
        part2AggregatingVertices.forEach((part, vertices)->{
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
                try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()){getRuntimeContext().runForAllLocalParts(modelServer.getBlock().getAgg() == AggregatorVariant.SUM || modelServer.getBlock().getAgg() == AggregatorVariant.MEAN?this::backwardFirstPhaseMeanOrSum :null);}
            }
            else try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()){getRuntimeContext().runForAllLocalParts(modelServer.getBlock().getAgg() == AggregatorVariant.SUM || modelServer.getBlock().getAgg() == AggregatorVariant.MEAN?this::backwardSecondPhaseMeanOrSum :null);}
        }
        else if(evt instanceof TrainingSubCoordinator.ForwardPhaser){
            switch (((TrainingSubCoordinator.ForwardPhaser) evt).iteration){
                case 1:
                    try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardResetPhase);}
                    break;
                case 2:
                    try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardAggregatorPhase);}
                    break;
                case 3:
                    try(BaseStorage.ReuseScope ignored = getRuntimeContext().getStorage().openReuseScope()) {getRuntimeContext().runForAllLocalParts(this::forwardUpdatePhase);}
                    break;
            }
        }
    }
}
