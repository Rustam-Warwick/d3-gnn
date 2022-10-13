package plugins.embedding_layer;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
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
import features.Tensor;
import helpers.GradientCollector;
import operators.events.BackwardBarrier;
import operators.events.BaseOperatorEvent;
import operators.events.ForwardBarrier;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Plugin that manages the training of GNNEmbeddingLayer
 */
public class GNNEmbeddingTrainingPlugin extends BaseGNNEmbeddingPlugin {


    public int numForwardSyncMessages; // #Synd Messages sent for starting batched inference

    public int numTrainingSyncMessages; // # Sync messages sent for backward messages

    public transient Map<Short, Tuple2<GradientCollector<String>, GradientCollector<String>>> collectors;

    public GNNEmbeddingTrainingPlugin(String modelName) {
        super(modelName, "trainer");
    }

    public GNNEmbeddingTrainingPlugin(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "trainer", trainableVertexEmbeddings);
    }

    @Override
    public void open() throws Exception {
        super.open();
        collectors = new HashMap<>(storage.layerFunction.getWrapperContext().getThisOperatorParts().size());
    }

    /**
     * Collect vertex -> dLoss/doutput, where vertices are masters in this part
     *
     * @param gradients Gradients for VJP backward iteration
     */
    @RemoteFunction
    public void collect(HashMap<String, NDArray> gradients) {
        collectors.putIfAbsent(getPartId(), Tuple2.of(new GradientCollector<>(), new GradientCollector<>()));
        collectors.get(getPartId()).f0.merge(gradients);
    }

    /**
     * Collect aggregator messages where Aggregator -> dLoss/dAgg
     */
    @RemoteFunction
    public void collectAggregators(HashMap<String, NDArray> aggGrads) {
        collectors.putIfAbsent(getPartId(), Tuple2.of(new GradientCollector<>(), new GradientCollector<>()));
        collectors.get(getPartId()).f1.merge(aggGrads);
    }

    /**
     * Train the update function,
     * Since we stop the stream, collectedVertexGradients should have both agg and features
     */
    public void trainFirstPart() {
        if (!collectors.containsKey(getPartId())) return;
        GradientCollector<String> collectedGradients = collectors.get(getPartId()).f0;
        if (collectedGradients.isEmpty()) return;

        // ---------- Prepare data for update model inputs(feature, aggregator)

        ArrayList<Vertex> vertices = new ArrayList(collectedGradients.size());
        NDList featuresList = new NDList(collectedGradients.size());
        NDList aggregatorList = new NDList(collectedGradients.size());
        NDList inputGradientsList = new NDList(collectedGradients.size());
        for (Map.Entry<String, NDArray> entry : collectedGradients.entrySet()) {
            Vertex v = storage.getVertex(entry.getKey());
            vertices.add(v);
            featuresList.add((NDArray) v.getFeature("f").getValue());
            aggregatorList.add((NDArray) v.getFeature("agg").getValue());
            inputGradientsList.add(entry.getValue());
        }

        NDList batchedInputs = new NDList(NDArrays.stack(featuresList), NDArrays.stack(aggregatorList));
        NDList batchedInputGradients = new NDList(NDArrays.stack(inputGradientsList));
        batchedInputs.get(0).setRequiresGradient(true);
        batchedInputs.get(1).setRequiresGradient(true);

        // --------------- Backward pass

        NDList batchedPredictions = ((GNNBlock) modelServer.getModel().getBlock()).getUpdateBlock().forward(modelServer.getParameterStore(), batchedInputs, true);

        JniUtils.backward((PtNDArray) batchedPredictions.get(0), (PtNDArray) batchedInputGradients.get(0), false, false);

        NDList gradients = new NDList(batchedInputs.get(0).getGradient(), batchedInputs.get(1).getGradient());
        // ------------------Collect Aggregation messages + Backward messages(If not the first layer)

        HashMap<Short, GradientCollector<String>> aggGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
        HashMap<String, NDArray> backwardGrads = storage.layerFunction.isFirst() ? null : new GradientCollector<>();
        for (int i = 0; i < collectedGradients.size(); i++) {
            NDArray previousFeatureGrad = gradients.get(0).get(i);
            NDArray aggGrad = gradients.get(1).get(i);
            if (backwardGrads != null) {
                backwardGrads.put(vertices.get(i).getId(), previousFeatureGrad);
            }
            if(((BaseAggregator<?>) vertices.get(i).getFeature("agg")).reduceCount() > 0) {
                for (Short replicaPart : vertices.get(i).replicaParts()) {
                    aggGradsPerPart.putIfAbsent(replicaPart, new GradientCollector<>());
                    aggGradsPerPart.get(getPartId()).put(vertices.get(i).getId(), aggregatorList.get(i).stack(aggGrad));
                }

                aggGradsPerPart.putIfAbsent(getPartId(), new GradientCollector<>());
                aggGradsPerPart.get(getPartId()).put(vertices.get(i).getId(), aggregatorList.get(i).stack(aggGrad));
            }
        }
        // ---------- Send AGG messages
        for (Map.Entry<Short, GradientCollector<String>> entry : aggGradsPerPart.entrySet()) {
            new RemoteInvoke()
                    .addDestination(entry.getKey())
                    .noUpdate()
                    .method("collectAggregators")
                    .toElement(getId(), elementType())
                    .withArgs(entry.getValue())
                    .where(MessageDirection.ITERATE)
                    .buildAndRun(storage);
        }

        // -------------- Send backward messages if it exists
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

        collectedGradients.clearPrepone();
    }
    public void trainSecondPart(){
        if (!collectors.containsKey(getPartId())) return;
        GradientCollector<String> collectedAggregators = collectors.get(getPartId()).f1;
        if (collectedAggregators.isEmpty()) return;

    }
//    /**
//     * Train the second part which is the messages and edges
//     */
//    public void trainSecondPart() {
//        if (!collectors.containsKey(getPartId())) return;
//        GradientCollector<BaseAggregator<?>> collectedAggregators = collectors.get(getPartId()).f1;
//        if (collectedAggregators.isEmpty()) return;
//
//        // 1. Compute the gradients
//        // 1.1 Fill up those 2 data structures
//        HashMap<BaseAggregator<?>, List<Vertex>> reverseEdgeList = new HashMap<>(); // Agg -> All in Vertices with Features
//        List<Vertex> srcVertices = new ArrayList<>(); // Vertices samereference as the above data structure for batching
//        for (Map.Entry<BaseAggregator<?>, NDArray> entry : collectedAggregators.entrySet()) {
//            entry.getKey().setStorage(storage);
//            Vertex v = (Vertex) entry.getKey().getElement();
//            Iterable<Edge> inEdges = storage.getIncidentEdges(v, EdgeType.IN);
//            for (Edge inEdge : inEdges) {
//                if (srcVertices.contains(inEdge.getSrc())) {
//                    // Src vertex is there can safely add
//                    reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
//                    reverseEdgeList.get(entry.getKey()).add(srcVertices.get(srcVertices.indexOf(inEdge.getSrc())));
//                } else if (inEdge.getSrc().getFeature("f") != null) {
//                    // Src vertex not in the list but feature is here
//                    reverseEdgeList.putIfAbsent(entry.getKey(), new ArrayList<>());
//                    ((NDArray) (inEdge.getSrc().getFeature("f").getValue())).setRequiresGradient(true); // Cache
//                    inEdge.getSrc().setStorage(null); // Remove from storage to not save in the backend
//                    srcVertices.add(inEdge.getSrc());
//                    reverseEdgeList.get(entry.getKey()).add(inEdge.getSrc());
//                }
//            }
//        }
//        // 1.2. Get all the messages for all in-vertices of the aggregators
//        List<NDList> srcFeatures = srcVertices.stream().map(item -> new NDList((NDArray) item.getFeature("f").getValue())).collect(Collectors.toList());
//        NDList batchedSrcFeatures = batchifier.batchify(srcFeatures.toArray(new NDList[0]));
//
//        NDList batchedSrcMessages = ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), batchedSrcFeatures, true);
//        NDList[] srcMessages = batchifier.unbatchify(batchedSrcMessages);
//
//        for (int i = 0; i < srcMessages.length; i++) {
//            srcVertices.get(i).setFeature("message", new Tensor(srcMessages[i].get(0)));
//        }
//
//        // 1.3. Compute/Accumulate the gradient on each source vertex
//        for (Map.Entry<BaseAggregator<?>, List<Vertex>> baseAggregatorListEntry : reverseEdgeList.entrySet()) {
//            List<NDList> tmpMessages = baseAggregatorListEntry.getValue().stream().map(item -> new NDList((NDArray) item.getFeature("message").getValue())).collect(Collectors.toList());
//            NDList tmpBatchedMessages = batchifier.batchify(tmpMessages.toArray(new NDList[0]));
//            NDArray batchedGradients = baseAggregatorListEntry.getKey().grad(collectedAggregators.get(baseAggregatorListEntry.getKey()), tmpBatchedMessages);
//            NDList[] tmpUnBatchedGradients = batchifier.unbatchify(new NDList(batchedGradients));
//            for (int i = 0; i < tmpUnBatchedGradients.length; i++) {
//                Vertex v = baseAggregatorListEntry.getValue().get(i);
//                if (v.getFeature("gradient") == null) {
//                    v.setFeature("gradient", new Tensor(tmpUnBatchedGradients[i].get(0)));
//                } else {
//                    ((NDArray) v.getFeature("gradient").getValue()).addi(tmpUnBatchedGradients[i].get(0)); // Accumulate the gradients
//                }
//            }
//        }
//
//        // 1.4. Batchify and compute the backward pass w.r.t. the inputs
//        List<NDList> srcGradients = srcVertices.stream().map(item -> new NDList((NDArray) item.getFeature("gradient").getValue())).collect(Collectors.toList());
//        NDList batchedSrcGradients = batchifier.batchify(srcGradients.toArray(new NDList[0]));
//        JniUtils.backward((PtNDArray) batchedSrcMessages.get(0), (PtNDArray) batchedSrcGradients.get(0), false, false);
//
//        // 2. Send those Vertices back with gradients only if this is not the first layer
//        if (!storage.layerFunction.isFirst()) {
//            HashMap<Short, HashMap<String, NDArray>> perPartGradients = new HashMap<>(); // per part agg
//            for (Vertex v : srcVertices) {
//                perPartGradients.putIfAbsent(v.masterPart(), new HashMap<>());
//                perPartGradients.get(v.masterPart()).put(v.getId(), ((NDArray) v.getFeature("f").getValue()).getGradient());
//            }
//            for (Map.Entry<Short, HashMap<String, NDArray>> entry : perPartGradients.entrySet()) {
//                new RemoteInvoke()
//                        .addDestination(entry.getKey())
//                        .noUpdate()
//                        .method("collect")
//                        .toElement(getId(), elementType())
//                        .withArgs(entry.getValue())
//                        .where(MessageDirection.BACKWARD)
//                        .buildAndRun(storage);
//            }
//        }
//    }

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
                if (featureMap.containsKey(localInEdge.getSrc().getId()) || localInEdge.getSrc().containsFeature("f")) {
                    tmp.add(localInEdge.getSrc().getId());
                    featureMap.putIfAbsent(localInEdge.getSrc().getId(), new NDList((NDArray) localInEdge.getSrc().getFeature("f").getValue()));
                }
            }
            if (!tmp.isEmpty()) inEdges.put(v, tmp);
        }
        Batchifier batchifier = new StackBatchifier();
        NDList inputs = batchifier.batchify(featureMap.values().toArray(new NDList[0]));
        NDList[] messages = batchifier.unbatchify(MESSAGE(inputs, false));
        int i = 0;
        for (String key : featureMap.keySet()) {
            featureMap.put(key, messages[i++]); // Put the updates to the same sourceVertex
        }

        for (Map.Entry<Vertex, List<String>> v : inEdges.entrySet()) {
            List<NDArray> inFeatures = v.getValue().stream().map(item -> featureMap.get(item).get(0)).collect(Collectors.toList());
            NDArray message = MeanAggregator.bulkReduce(inFeatures.toArray(new NDArray[0]));
            new RemoteInvoke()
                    .toElement(Feature.encodeAttachedFeatureId("agg", v.getKey().getId(), ElementType.VERTEX), ElementType.FEATURE)
                    .where(MessageDirection.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .addDestination(v.getKey().masterPart())
                    .withArgs(new NDList(message), inFeatures.size())
                    .buildAndRun(storage);
        }
    }

    public void inferenceSecondPartStart() {
        for (Vertex v : storage.getVertices()) {
            if (updateReady(v)) {
                NDArray ft = (NDArray) (v.getFeature("f")).getValue();
                NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
                NDArray update = UPDATE(new NDList(ft, agg), false).get(0);
                Vertex messageVertex = v.copy();
                messageVertex.setFeature("f", new Tensor(update));
                storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex), MessageDirection.FORWARD);
            }
        }
    }

    @Override
    public void onOperatorEvent(BaseOperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof BackwardBarrier) {
            if (++numTrainingSyncMessages == 1) {
                storage.layerFunction.runForAllLocalParts(this::trainFirstPart);
                storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.ITERATE)), MessageDirection.ITERATE);
            } else {
                storage.layerFunction.runForAllLocalParts(this::trainSecondPart);
                if (!storage.layerFunction.isFirst())
                    storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.BACKWARD)), MessageDirection.BACKWARD);
                modelServer.getParameterStore().sync();
                numTrainingSyncMessages = 0;
            }
        } else if (event instanceof ForwardBarrier) {
            // first 2 for updating the model, then reset agg and send new messages
            if (++numForwardSyncMessages == 1) {
                storage.layerFunction.runForAllLocalParts(this::inferenceFirstPartStart);
            } else if (numForwardSyncMessages == 2) {
                // Ready to forward
                storage.layerFunction.runForAllLocalParts(this::inferenceSecondPartStart);
            }
            if (numForwardSyncMessages < 2) {
                storage.layerFunction.broadcastMessage(new GraphOp(new ForwardBarrier(MessageDirection.ITERATE)), MessageDirection.ITERATE);
            } else {
                storage.layerFunction.broadcastMessage(new GraphOp(new ForwardBarrier(MessageDirection.FORWARD)), MessageDirection.FORWARD);
                numForwardSyncMessages = 0;
                // @todo add embedding plugin start
            }
        }
    }
}
