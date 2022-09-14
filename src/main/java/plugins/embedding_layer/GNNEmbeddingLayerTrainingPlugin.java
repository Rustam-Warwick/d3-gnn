package plugins.embedding_layer;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.RemoteInvoke;
import features.MeanGradientCollector;
import features.Tensor;
import operators.events.InferenceBarrier;
import operators.events.LocalTrainBarrier;
import operators.events.StartTraining;
import operators.events.TrainBarrier;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import plugins.ModelServer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Plugin that manages the training of MixedGNNEmbeddingLayer
 */
public class GNNEmbeddingLayerTrainingPlugin extends Plugin {

    public final String modelName;

    public transient ModelServer modelServer;

    public transient GNNEmbeddingPlugin embeddingPlugin;

    public transient Batchifier batchifier;

    public int startTrainingSyncMessages; // #Synd Messages sent for starting training

    public int inferenceSyncMessages; // #Synd Messages sent for starting training

    public GNNEmbeddingLayerTrainingPlugin(String modelName) {
        super(String.format("%s-trainer", modelName));
        this.modelName = modelName;
    }

    @Override
    public void open() throws Exception {
        super.open();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        embeddingPlugin = (GNNEmbeddingPlugin) storage.getPlugin(String.format("%s-inferencer", modelName));
        batchifier = new StackBatchifier();
        storage.layerFunction.runForAllLocalParts(() -> {
            setFeature("collectedGradients", new MeanGradientCollector(Tuple2.of(new HashMap<>(), new HashMap()), true, null));
            setFeature("collectedAggregators", new MeanGradientCollector(Tuple2.of(new HashMap<>(), new HashMap()), true, null));
        });
    }
    // INITIALIZATION DONE

    /**
     * Collect vertex -> dLoss/doutput, where vertices are masters in this part
     *
     * @param gradients Gradients for VJP backward iteration
     */
    @RemoteFunction
    public void collect(HashMap<String, NDArray> gradients) {
        MeanGradientCollector<String> feature = (MeanGradientCollector<String>) getFeature("collectedGradients");
        feature.merge(gradients);
    }

    /**
     * Collect aggregator messages where Aggregator -> dLoss/dAgg
     */
    @RemoteFunction
    public void collectAggregators(HashMap<BaseAggregator<?>, NDArray> aggGrads) {
        MeanGradientCollector<BaseAggregator<?>> feature = (MeanGradientCollector<BaseAggregator<?>>) getFeature("collectedAggregators");
        feature.merge(aggGrads);
    }

    /**
     * Train the update function,
     * Since we stop the stream, collectedVertexGradients should have both agg and features
     */
    public void trainUpdateFunction() {
        MeanGradientCollector<String> collectedGradients = (MeanGradientCollector<String>) getFeature("collectedGradients");
        if (!collectedGradients.getValue().isEmpty()) {
            // 1. Prepare data for update model inputs(feature, aggregator)
            Tuple6<Vertex, NDArray, NDArray, NDArray, NDArray, NDArray>[] data = new Tuple6[collectedGradients.getValue().size()];// <Vertex, featre, featureGrad, agg, aggGrad, InputGrad>
            int i = 0;
            for (Map.Entry<String, NDArray> entry : collectedGradients.getValue().entrySet()) {
                Vertex v = storage.getVertex(entry.getKey());
                NDArray tmpFeature = (NDArray) v.getFeature("feature").getValue();
                NDArray tmpAgg = (NDArray) v.getFeature("agg").getValue();
                tmpFeature.setRequiresGradient(true);
                tmpAgg.setRequiresGradient(true);
                data[i++] = Tuple6.of(v, tmpFeature, null, tmpAgg, null, entry.getValue());
            }
            NDList[] inputs = new NDList[data.length];
            NDList[] grads = new NDList[data.length];
            for (i = 0; i < data.length; i++) {
                inputs[i] = new NDList(data[i].f1, data[i].f3);
                grads[i] = new NDList(data[i].f5);
            }
            NDList batchedInputs = batchifier.batchify(inputs);
            NDList batchedGradients = batchifier.batchify(grads);

            try {

                // 2. Backward pass
                NDList batchedPredictions = ((GNNBlock) modelServer.getModel().getBlock()).getUpdateBlock().forward(modelServer.getParameterStore(), batchedInputs, true);
                JniUtils.backward((PtNDArray) batchedPredictions.get(0), (PtNDArray) batchedGradients.get(0), false, false);

                // 3. Collect Aggregation messages + Backward messages(If not the first layer)
                HashMap<Short, HashMap<BaseAggregator<?>, NDArray>> aggGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
                HashMap<String, NDArray> backwardGrads = storage.layerFunction.isFirst() ? null : new HashMap<>();
                aggGradsPerPart.put(getPartId(), new HashMap<>());
                for (i = 0; i < data.length; i++) {
                    data[i].f2 = data[i].f1.getGradient();
                    data[i].f4 = data[i].f3.getGradient();
                    if (backwardGrads != null) {
                        backwardGrads.put(data[i].f0.getId(), data[i].f2);
                    }
                    aggGradsPerPart.get(getPartId()).put((BaseAggregator<?>) data[i].f0.getFeature("agg"), data[i].f4);
                    for (Short replicaPart : data[i].f0.replicaParts()) {
                        aggGradsPerPart.putIfAbsent(replicaPart, new HashMap<>());
                        aggGradsPerPart.get(replicaPart).put((BaseAggregator<?>) data[i].f0.getFeature("agg"), data[i].f4);
                    }
                }
                // 3. Send agg messages
//                for (Map.Entry<Short, HashMap<BaseAggregator<?>, NDArray>> entry : aggGradsPerPart.entrySet()) {
//                    new RemoteInvoke()
//                            .addDestination(entry.getKey())
//                            .noUpdate()
//                            .method("collectAggregators")
//                            .toElement(getId(), elementType())
//                            .withArgs(entry.getValue())
//                            .where(MessageDirection.ITERATE)
//                            .buildAndRun(storage);
//                }
//
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
                e.printStackTrace();
            } finally {
                for (Tuple6<Vertex, NDArray, NDArray, NDArray, NDArray, NDArray> datum : data) {
                    datum.f1.setRequiresGradient(false);
                    datum.f3.setRequiresGradient(false);
                }
                batchedGradients.close();
                batchedInputs.close();
                collectedGradients.clean();
            }
        }
    }

    /**
     * Train the second part which is the messages and edges
     */
    public void trainSecondPartStart() {
        MeanGradientCollector<BaseAggregator<?>> collectedAggregators = (MeanGradientCollector<BaseAggregator<?>>) getFeature("collectedAggregators");
        if (!collectedAggregators.getValue().isEmpty()) {
            System.out.println("INSIDE");
//            // 1. Compute the gradients
//            // 1.1 Fill up those 2 data structures
            HashMap<BaseAggregator<?>, List<Vertex>> reverseEdgeList = new HashMap<>(); // Agg -> All in Vertices with Features
            List<Vertex> srcVertices = new ArrayList<>(); // Vertices samereference as the above data structure for batching
            try {
                for (Map.Entry<BaseAggregator<?>, NDArray> entry : collectedAggregators.getValue().entrySet()) {
                    entry.getKey().setStorage(storage);
                    Vertex v = (Vertex) entry.getKey().getElement();
                    Iterable<Edge> inEdges = storage.getIncidentEdges(v, EdgeType.IN);
                    for (Edge inEdge : inEdges) {
                        if (srcVertices.contains(inEdge.getSrc())) {
                            // Src vertex is there can safely add
                            reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
                            reverseEdgeList.get(entry.getKey()).add(srcVertices.get(srcVertices.indexOf(inEdge.getSrc())));
                        } else if (inEdge.getSrc().getFeature("feature") != null) {
                            // Src vertex not in the list but feature is here
                            reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
                            ((NDArray) (inEdge.getSrc().getFeature("feature").getValue())).setRequiresGradient(true); // Cache
                            inEdge.getSrc().setStorage(null); // Remove from storage to not save in the backend
                            srcVertices.add(inEdge.getSrc());
                            reverseEdgeList.get(entry.getKey()).add(inEdge.getSrc());
                        }
                    }
                }
                // 1.2. Get all the messages for all in-vertices of the aggregators
                List<NDList> srcFeatures = srcVertices.stream().map(item -> new NDList((NDArray) item.getFeature("feature").getValue())).collect(Collectors.toList());
                NDList batchedSrcFeatures = batchifier.batchify(srcFeatures.toArray(new NDList[0]));

                NDList batchedSrcMessages = ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), batchedSrcFeatures, true);
                NDList[] srcMessages = batchifier.unbatchify(batchedSrcMessages);

                for (int i = 0; i < srcMessages.length; i++) {
                    srcVertices.get(i).setFeature("message", new Tensor(srcMessages[i].get(0)));
                }

                // 1.3. Compute/Accumulate the gradient on each source vertex
                for (Map.Entry<BaseAggregator<?>, List<Vertex>> baseAggregatorListEntry : reverseEdgeList.entrySet()) {
                    List<NDList> tmpMessages = baseAggregatorListEntry.getValue().stream().map(item -> new NDList((NDArray) item.getFeature("message").getValue())).collect(Collectors.toList());
                    NDList tmpBatchedMessages = batchifier.batchify(tmpMessages.toArray(new NDList[0]));
                    NDArray batchedGradients = baseAggregatorListEntry.getKey().grad(collectedAggregators.getValue().get(baseAggregatorListEntry.getKey()), tmpBatchedMessages);
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
    }

    /**
     *
     */
    public void inferenceFirstPartStart() {
        // 2. Clear Aggregators + InReduce all the existing edges
        HashMap<Vertex, List<String>> inEdges = new HashMap<>();
        LinkedHashMap<String, NDList> featureMap = new LinkedHashMap<>();
        for (Vertex v : storage.getVertices()) {
            // 1. Clear the aggregators
            if (v.state() == ReplicaState.MASTER && v.containsFeature("agg") != null) {
                ((BaseAggregator<?>) v.getFeature("agg")).reset();
            }

            Iterable<Edge> localInEdges = storage.getIncidentEdges(v, EdgeType.IN);
            List<String> tmp = new ArrayList<>();
            for (Edge localInEdge : localInEdges) {
                if (featureMap.containsKey(localInEdge.getSrc().getId()) || localInEdge.getSrc().containsFeature("feature")) {
                    tmp.add(localInEdge.getSrc().getId());
                    featureMap.putIfAbsent(localInEdge.getSrc().getId(), new NDList((NDArray) localInEdge.getSrc().getFeature("feature").getValue()));
                }
            }
            if (!tmp.isEmpty()) inEdges.put(v, tmp);
        }
        NDList inputs = batchifier.batchify(featureMap.values().toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(embeddingPlugin.MESSAGE(inputs, false));
        int i = 0;
        for (String key : featureMap.keySet()) {
            featureMap.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }

        for (Map.Entry<Vertex, List<String>> v : inEdges.entrySet()) {
            List<NDArray> inFeatures = v.getValue().stream().map(item -> featureMap.get(item).get(0)).collect(Collectors.toList());
            NDArray message = MeanAggregator.bulkReduce(inFeatures.toArray(new NDArray[0]));
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("agg", v.getKey().getId()), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(v.getKey().masterPart())
                    .withArgs(message, inFeatures.size())
                    .buildAndRun(storage);
        }
    }

    public void inferenceSecondPartStart() {
        for (Vertex v : storage.getVertices()) {
            if (embeddingPlugin.updateReady(v)) {
                NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
                NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
                NDArray update = embeddingPlugin.UPDATE(new NDList(ft, agg), false).get(0);
                Vertex messageVertex = v.copy();
                messageVertex.setFeature("feature", new Tensor(update));
                storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex), MessageDirection.FORWARD);
            }
        }
    }

    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        try {
            if (event instanceof StartTraining) {
                // Iteration StartTraining message twice to get the pending inference results, then emit to next operator
                StartTraining evt = (StartTraining) event;
                evt.setBroadcastCount((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks());
                if (++startTrainingSyncMessages < 3) {
                    storage.layerFunction.broadcastMessage(new GraphOp(evt), MessageDirection.ITERATE);
                } else {
                    embeddingPlugin.stop();
                    storage.layerFunction.broadcastMessage(new GraphOp(evt), MessageDirection.FORWARD);
                    startTrainingSyncMessages = 0;
                }
            } else if (event instanceof TrainBarrier) {
                storage.layerFunction.runForAllLocalParts(this::trainUpdateFunction);
                storage.layerFunction.broadcastMessage(new GraphOp(new LocalTrainBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.ITERATE);
            } else if (event instanceof LocalTrainBarrier) {
                storage.layerFunction.runForAllLocalParts(this::trainSecondPartStart);
                if (!storage.layerFunction.isFirst()) {
                    storage.layerFunction.broadcastMessage(new GraphOp(new TrainBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.BACKWARD);
                } else {
                    // Start Inference
                    inferenceSyncMessages++;
                    modelServer.getParameterStore().sync();
                    storage.layerFunction.broadcastMessage(new GraphOp(new InferenceBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.ITERATE);
                }
            } else if (event instanceof InferenceBarrier) {
                // first 2 for updating the model, then reset agg and send new messages
                inferenceSyncMessages++;
                if (inferenceSyncMessages == 1) {
                    modelServer.getParameterStore().sync();
                } else if (inferenceSyncMessages == 3) {
                    // Ready to do agg
                    storage.layerFunction.runForAllLocalParts(this::inferenceFirstPartStart);
                } else {
                    // Ready to forward
                    storage.layerFunction.runForAllLocalParts(this::inferenceSecondPartStart);
                }

                if (inferenceSyncMessages < 4) {
                    storage.layerFunction.broadcastMessage(new GraphOp(new InferenceBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.ITERATE);
                } else {
                    storage.layerFunction.broadcastMessage(new GraphOp(new InferenceBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.FORWARD);
                    inferenceSyncMessages = 0;
                    embeddingPlugin.start();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
