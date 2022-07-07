package plugins.embedding_layer;

import aggregators.BaseAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.*;
import features.Tensor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Plugin that manages the training of MixedGNNEmbeddingLayer
 */
public class MixedGNNEmbeddingLayerTraining extends Plugin {

    public final String modelName; // Name of the model to be hold by this
    public transient MixedGNNEmbeddingLayer embeddingLayer;
    public transient Batchifier batchifier;
    public int numOutputChannels; // Num Output channels on the next layer. Used to see if all messages have been received
    public int syncMessages; // #Messages sent from the next layer for synchronization
    public int iterationSyncMessages; // #Messages sent from this layer for synchronization

    public MixedGNNEmbeddingLayerTraining(String modelName) {
        super(String.format("%s-trainer", modelName));
        this.modelName = modelName;
    }

    @Override
    public void open() throws Exception {
        super.open();
        embeddingLayer = (MixedGNNEmbeddingLayer) this.storage.getPlugin(String.format("%s-inferencer", modelName));
        numOutputChannels = storage.layerFunction.getNumberOfOutChannels(null); // Number of expect sync messages from the next layer operator
        batchifier = new StackBatchifier();
    }
    // INITIALIZATION DONE

    /**
     * Collect vertex -> dLoss/doutput, where vertices are masters in this part
     *
     * @param gradients Gradients for VJP backward iteration
     */
    @RemoteFunction
    public void collect(HashMap<String, NDArray> gradients) {
        Feature<?, ?> feature = getFeature("collectedGradients");
        if (feature == null) setFeature("collectedGradients", new Feature(gradients, true, getPartId()));
        else {
            Feature<HashMap<String, NDArray>, HashMap<String, NDArray>> collectedGradients = (Feature<HashMap<String, NDArray>, HashMap<String, NDArray>>) feature;
            gradients.forEach((key, grad) -> {
                collectedGradients.getValue().computeIfPresent(key, (s, item) -> (item.add(grad))); // Add to the gradient if it is already here
                collectedGradients.getValue().putIfAbsent(key, grad); // Otherwise add it to the hashmap
            });
            storage.updateFeature(collectedGradients);
        }
    }

    /**
     * Collect aggregator messages where Aggregator -> dLoss/dAgg
     */
    @RemoteFunction
    public void collectAggregators(HashMap<BaseAggregator<?>, NDArray> aggGrads) {
        Feature<?, ?> feature = getFeature("collectedAggregators");
        if (feature == null) setFeature("collectedAggregators", new Feature(aggGrads, true, getPartId()));
        else {
            Feature<HashMap<BaseAggregator<?>, NDArray>, HashMap<BaseAggregator<?>, NDArray>> collectedAggregators = (Feature<HashMap<BaseAggregator<?>, NDArray>, HashMap<BaseAggregator<?>, NDArray>>) feature;
            collectedAggregators.getValue().putAll(aggGrads); // Note that overlaps are impossible here since these messages are already coming from master parts
            storage.updateFeature(collectedAggregators);
        }
    }

    /**
     * Train the first part which is the update function
     */
    public void trainFirstPartStart() {
        Feature<HashMap<String, NDArray>, HashMap<String, NDArray>> collectedGradients = (Feature<HashMap<String, NDArray>, HashMap<String, NDArray>>) getFeature("collectedGradients");
        if (collectedGradients != null && !collectedGradients.getValue().isEmpty()) {
            // 1. Compute the gradients
            List<Vertex> vertices = new ArrayList<>();
            List<NDList> inputs = new ArrayList<>();
            List<NDList> gradients = new ArrayList<>();
            try {
                for (Map.Entry<String, NDArray> entry : collectedGradients.getValue().entrySet()) {
                    Vertex v = storage.getVertex(entry.getKey());
                    NDArray tmpFeature = (NDArray) v.getFeature("feature").getValue();
                    NDArray tmpAgg = (NDArray) v.getFeature("agg").getValue();
                    tmpFeature.setRequiresGradient(true);
                    tmpAgg.setRequiresGradient(true);
                    inputs.add(new NDList(tmpFeature, tmpAgg));
                    gradients.add(new NDList(entry.getValue()));
                    vertices.add(v);
                }
                NDList batchedInputs = batchifier.batchify(inputs.toArray(new NDList[0]));
                NDList batchedPredictions = embeddingLayer.UPDATE(batchedInputs, true);
                NDList batchedGradients = batchifier.batchify(gradients.toArray(new NDList[0]));
                JniUtils.backward((PtNDArray) batchedPredictions.get(0), (PtNDArray) batchedGradients.get(0), false, false);

                // 2. Collect Aggregation messages + Backward messages(If not the first layer)
                HashMap<Short, HashMap<BaseAggregator<?>, NDArray>> aggGradsPerPart = new HashMap<>(); // per part agg
                HashMap<String, NDArray> backwardGrads = storage.layerFunction.isFirst() ? null : new HashMap<>();
                aggGradsPerPart.put(getPartId(), new HashMap<>());
                for (Vertex v : vertices) {
                    if (backwardGrads != null) {
                        backwardGrads.put(v.getId(), ((NDArray) v.getFeature("feature").getValue()).getGradient());
                    }
                    BaseAggregator<?> tmpAgg = (BaseAggregator<?>) v.getFeature("agg");
                    aggGradsPerPart.get(getPartId()).put(tmpAgg, tmpAgg.getValue().getGradient());
                    v.replicaParts().forEach(item -> {
                        aggGradsPerPart.putIfAbsent(item, new HashMap<>());
                        aggGradsPerPart.get(item).put(tmpAgg, tmpAgg.getValue().getGradient());
                    });
                }
                // 3. Send agg messages
                for (Map.Entry<Short, HashMap<BaseAggregator<?>, NDArray>> entry : aggGradsPerPart.entrySet()) {
                    new RemoteInvoke()
                            .addDestination(entry.getKey())
                            .noUpdate()
                            .method("collectAggregators")
                            .toElement(getId(), elementType())
                            .withArgs(entry.getValue())
                            .where(MessageDirection.ITERATE)
                            .buildAndRun(storage);
                }

                // 4. Send backward messages if it exists
                if (Objects.nonNull(backwardGrads)) {
                    new RemoteInvoke()
                            .addDestination(getPartId()) // Only masters will be here anyway
                            .noUpdate()
                            .method("collect")
                            .toElement(getId(), elementType())
                            .where(MessageDirection.BACKWARD)
                            .withArgs(backwardGrads)
                            .buildAndRun(storage);
                }
            } catch (Exception e) {
                // Pass
            } finally {
                // 5. Clean the collected gradients Feature, make require gradeitns back to false again
                inputs.forEach(item -> {
                    item.get(0).setRequiresGradient(false);
                    item.get(1).setRequiresGradient(false);
                });
                collectedGradients.getValue().clear();
                storage.updateFeature(collectedGradients);
            }
        }
        if (isLastReplica()) {
            Rmi iterationSynchronize = new Rmi(getId(), "iterationSynchronize", new Object[]{}, elementType(), false, null);
            storage.layerFunction.broadcastMessage(new GraphOp(Op.RMI, null, iterationSynchronize, null, MessageCommunication.BROADCAST), MessageDirection.ITERATE);
        }
    }

    /**
     * Train the second part which is the messages and edges
     */
    public void trainSecondPartStart() {
        Feature<HashMap<BaseAggregator<?>, NDArray>, HashMap<BaseAggregator<?>, NDArray>> collectedAggregators = (Feature<HashMap<BaseAggregator<?>, NDArray>, HashMap<BaseAggregator<?>, NDArray>>) getFeature("collectedAggregators");
        if (collectedAggregators != null && !collectedAggregators.getValue().isEmpty()) {
            // 1. Compute the gradients
            // 1.1 Fill up those 2 data structures
            HashMap<BaseAggregator<?>, List<Vertex>> reverseEdgeList = new HashMap<>(); // Agg -> All in Vertices with Features
            List<Vertex> srcVertices = new ArrayList<>(); // Vertices samereference as the above data structure for batching
            try {
                for (Map.Entry<BaseAggregator<?>, NDArray> entry : collectedAggregators.getValue().entrySet()) {
                    entry.getKey().setStorage(storage);
                    Vertex v = (Vertex) entry.getKey().getElement();
                    Iterable<Edge> inEdges = storage.getIncidentEdges(v, EdgeType.IN);
                    for (Edge inEdge : inEdges) {
                        if (srcVertices.contains(inEdge.src)) {
                            // Src vertex is there can safely add
                            reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
                            reverseEdgeList.get(entry.getKey()).add(srcVertices.get(srcVertices.indexOf(inEdge.src)));
                        } else if (inEdge.src.getFeature("feature") != null) {
                            // Src vertex not in the list but feature is here
                            reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
                            ((NDArray) (inEdge.src.getFeature("feature").getValue())).setRequiresGradient(true); // Cache
                            inEdge.src.setStorage(null); // Remove from storage to not save in the backend
                            srcVertices.add(inEdge.src);
                            reverseEdgeList.get(entry.getKey()).add(inEdge.src);
                        }
                    }
                }
                // 1.2. Get all the messages for all in-vertices of the aggregators
                List<NDList> srcFeatures = srcVertices.stream().map(item -> new NDList((NDArray) item.getFeature("feature").getValue())).collect(Collectors.toList());
                NDList batchedSrcFeatures = batchifier.batchify(srcFeatures.toArray(new NDList[0]));
                NDList batchedSrcMessages = embeddingLayer.MESSAGE(batchedSrcFeatures, true);
                NDList[] srcMessages = batchifier.unbatchify(batchedSrcMessages);

                for (int i = 0; i < srcMessages.length; i++) {
                    srcVertices.get(i).setFeature("message", new Tensor(srcMessages[i].get(0)));
                }

                // 1.3. Compute/Accumulate the gradient on each source vertex
                for (Map.Entry<BaseAggregator<?>, List<Vertex>> baseAggregatorListEntry : reverseEdgeList.entrySet()) {
                    List<NDList> tmpMessages = baseAggregatorListEntry.getValue().stream().map(item -> new NDList((NDArray) item.getFeature("message").getValue())).collect(Collectors.toList());
                    NDList tmpBatchedMessages = batchifier.batchify(tmpMessages.toArray(new NDList[0]));
                    NDArray batchedGradients = baseAggregatorListEntry.getKey().grad(collectedAggregators.getValue().get(baseAggregatorListEntry.getKey()), tmpBatchedMessages.get(0));
                    NDList[] tmpUnBatchedGradients = batchifier.unbatchify(new NDList(batchedGradients));
                    for (int i = 0; i < tmpUnBatchedGradients.length; i++) {
                        Vertex v = baseAggregatorListEntry.getValue().get(i);
                        if (v.getFeature("gradient") == null) {
                            v.setFeature("gradient", new Tensor(tmpUnBatchedGradients[i].get(0)));
                        } else {
                            ((NDArray) v.getFeature("gradient").getValue()).addi(tmpUnBatchedGradients[i].get(0)); // Accumulate the gradients
                        }
                    }
                }

                // 1.4. Batchify and compute the backward pass w.r.t. the inputs
                List<NDList> srcGradients = srcVertices.stream().map(item -> new NDList((NDArray) item.getFeature("gradient").getValue())).collect(Collectors.toList());
                NDList batchedSrcGradients = batchifier.batchify(srcGradients.toArray(new NDList[0]));
                JniUtils.backward((PtNDArray) batchedSrcMessages.get(0), (PtNDArray) batchedSrcGradients.get(0), false, false);

                // 2. Send those Vertices back with gradients only if this is not the first layer
                if (!storage.layerFunction.isFirst()) {
                    HashMap<Short, HashMap<String, NDArray>> perPartGradients = new HashMap<>(); // per part agg
                    for (Vertex v : srcVertices) {
                        perPartGradients.putIfAbsent(v.masterPart(), new HashMap<>());
                        perPartGradients.get(v.masterPart()).put(v.getId(), ((NDArray) v.getFeature("feature").getValue()).getGradient());
                    }
                    for (Map.Entry<Short, HashMap<String, NDArray>> entry : perPartGradients.entrySet()) {
                        new RemoteInvoke()
                                .addDestination(entry.getKey())
                                .noUpdate()
                                .method("collect")
                                .toElement(getId(), elementType())
                                .withArgs(entry.getValue())
                                .where(MessageDirection.BACKWARD)
                                .buildAndRun(storage);
                    }
                }
            } catch (Exception e) {
                // Pass
            } finally {
                // 3. Cleanup
                srcVertices.forEach(item -> ((NDArray) item.getFeature("feature").getValue()).setRequiresGradient(false));
                collectedAggregators.getValue().clear();
                storage.updateFeature(collectedAggregators);
            }
        }
        if (isLastReplica()) {
            if (!storage.layerFunction.isFirst()) {
                Rmi synchronize = new Rmi(getId(), "synchronize", new Object[]{}, elementType(), false, null);
                storage.layerFunction.broadcastMessage(new GraphOp(Op.RMI, null, synchronize, null, MessageCommunication.BROADCAST), MessageDirection.BACKWARD);
            }
            embeddingLayer.modelServer.getParameterStore().sync(); // This operator index is fully ready to update the model
        }

    }

    @RemoteFunction
    public void synchronize() {
        if (state() == ReplicaState.MASTER) {
            ++syncMessages;
        }
        if (syncMessages == numOutputChannels) {
            trainFirstPartStart();
            if (isLastReplica()) syncMessages = 0;
        }
    }

    @RemoteFunction
    public void iterationSynchronize() {
        if (state() == ReplicaState.MASTER) {
            ++iterationSyncMessages;
        }
        if (iterationSyncMessages == storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks()) {
            trainSecondPartStart();
            if (isLastReplica()) iterationSyncMessages = 0;
        }
    }


}
