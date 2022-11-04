package plugins.gnn_embedding;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrayCollector;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteFunction;
import elements.iterations.Rmi;
import features.Aggregator;
import features.MeanAggregator;
import features.Tensor;
import operators.events.BackwardBarrier;
import operators.events.BaseOperatorEvent;
import operators.events.ForwardBarrier;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava30.com.google.common.primitives.Longs;

import java.util.*;

/**
 * Plugin that manages the training of GNNEmbeddingLayer
 */
public class GNNEmbeddingTrainingPlugin extends BaseGNNEmbeddingPlugin {
    public int numForwardSyncMessages; // #Sync Messages sent for starting batched inference

    public int numTrainingSyncMessages; // #Sync messages sent for backward messages

    public transient Map<Short, Tuple2<NDArrayCollector<String>, NDArrayCollector<String>>> collectors;

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
        if (gradients.isEmpty()) return;
        collectors.computeIfAbsent(getPartId(), (key) -> Tuple2.of(new NDArrayCollector<>(true), new NDArrayCollector<>(true)));
        collectors.get(getPartId()).f0.putAll(gradients);
    }

    /**
     * Collect aggregator messages where Aggregator -> dLoss/dAgg
     */
    @RemoteFunction
    public void collectAggregators(HashMap<String, NDArray> gradients) {
        if (gradients.isEmpty()) return;
        collectors.computeIfAbsent(getPartId(), (key) -> Tuple2.of(new NDArrayCollector<>(true), new NDArrayCollector<>(true)));
        NDArrayCollector<String> collector = collectors.get(getPartId()).f1;
        for (Map.Entry<String, NDArray> stringNDArrayEntry : gradients.entrySet()) {
            Vertex v = storage.getVertex(stringNDArrayEntry.getKey());
            for (DEdge incidentDEdge : storage.getIncidentEdges(v, EdgeType.IN)) {
                collector.put(incidentDEdge.getSrc().getId(), stringNDArrayEntry.getValue());
            }
        }
    }

    /**
     * First part of training resposible for getting update gradients and message gradients
     * <p>
     * Assumes to contain only gradients for master vertices with aggregators allocated already
     * </p>
     */
    public void trainFirstPart() {
        try {
            if (!collectors.containsKey(getPartId())) return;
            NDArrayCollector<String> collectedGradients = collectors.get(getPartId()).f0;
            if (collectedGradients.isEmpty()) return;

            // ---------- Prepare data for update model inputs(feature, aggregator)

            ArrayList<Vertex> vertices = new ArrayList<>(collectedGradients.size());
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

            HashMap<Short, NDArrayCollector<String>> aggGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
            HashMap<String, NDArray> backwardGrads = storage.layerFunction.isFirst() ? null : new NDArrayCollector<>(false);
            for (int i = 0; i < collectedGradients.size(); i++) {
                NDArray previousFeatureGrad = gradients.get(0).get(i);
                NDArray aggGrad = gradients.get(1).get(i);
                Aggregator aggregator = (Aggregator) vertices.get(i).getFeature("agg");
                if (backwardGrads != null) {
                    backwardGrads.put(vertices.get(i).getId(), previousFeatureGrad);
                }
                if (aggregator.reduceCount() > 0) {
                    NDArray messagesGrad = aggregator.grad(aggGrad);
                    for (Short replicaPart : vertices.get(i).replicaParts()) {
                        aggGradsPerPart.computeIfAbsent(replicaPart, (key) -> new NDArrayCollector<>(false));
                        aggGradsPerPart.get(replicaPart).put(vertices.get(i).getId(), messagesGrad);
                    }

                    aggGradsPerPart.computeIfAbsent(getPartId(), (key) -> new NDArrayCollector<>(false));
                    aggGradsPerPart.get(getPartId()).put(vertices.get(i).getId(), messagesGrad);
                }
            }

            // ---------- Send AGG messages

            for (Map.Entry<Short, NDArrayCollector<String>> entry : aggGradsPerPart.entrySet()) {
                Rmi.buildAndRun(
                        new Rmi(getId(), "collectAggregators", elementType(), new Object[]{entry.getValue()}, false), storage,
                        entry.getKey(),
                        MessageDirection.ITERATE
                );
            }

            // -------------- Send backward messages if it exists

            if (Objects.nonNull(backwardGrads)) {

                Rmi.buildAndRun(
                        new Rmi(getId(), "collect", elementType(), new Object[]{backwardGrads}, false), storage,
                        getPartId(),
                        MessageDirection.BACKWARD
                );
            }

            collectedGradients.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Second part of training responsible for getting message gradients
     * <p>
     * Batch compute gradients for message function as well as previous layer updates
     * Previous layer updates are needed only if this is not First layer of GNN
     * </p>
     */
    public void trainSecondPart() {
        try {
            if (!collectors.containsKey(getPartId())) return;
            NDArrayCollector<String> collectedAggregators = collectors.get(getPartId()).f1;
            if (collectedAggregators.isEmpty()) return;

            NDList features = new NDList(collectedAggregators.size());
            NDList gradients = new NDList(collectedAggregators.size());
            List<Vertex> vertices = new ArrayList<>(collectedAggregators.size());
            for (Map.Entry<String, NDArray> stringNDArrayEntry : collectedAggregators.entrySet()) {
                Vertex v = storage.getVertex(stringNDArrayEntry.getKey());
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
            if (!storage.layerFunction.isFirst()) {
                HashMap<Short, NDArrayCollector<String>> backwardGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
                for (int i = 0; i < vertices.size(); i++) {
                    backwardGradsPerPart.computeIfAbsent(vertices.get(i).masterPart(), (key) -> new NDArrayCollector<>(false));
                    backwardGradsPerPart.get(vertices.get(i).masterPart()).put(vertices.get(i).getId(), batchedFeatureGradients.get(i));
                }
                for (Map.Entry<Short, NDArrayCollector<String>> entry : backwardGradsPerPart.entrySet()) {
                    Rmi.buildAndRun(
                            new Rmi(getId(), "collect", elementType(), new Object[]{entry.getValue()}, false), storage,
                            entry.getKey(),
                            MessageDirection.BACKWARD
                    );
                }
            }
            collectedAggregators.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Do the first aggregation cycle of the inference.
     * <p>
     * Reset local aggregators, Collect local vertices, batch messages going to a vertex and send RMI reduce messages
     * </p>
     */
    public void inferenceFirstPartStart() {
        HashMap<Vertex, Integer> srcVertices = new HashMap<>();
        HashMap<Vertex, List<Integer>> destVertices = new HashMap<>();
        NDList srcFeatures = new NDList();
        for (Vertex vertex : storage.getVertices()) {
            if (vertex.state() == ReplicaState.MASTER) ((Aggregator) vertex.getFeature("agg")).reset();
            Iterable<DEdge> localInEdges = storage.getIncidentEdges(vertex, EdgeType.IN);
            for (DEdge localInDEdge : localInEdges) {
                if (!srcVertices.containsKey(localInDEdge.getSrc())) {
                    srcVertices.put(localInDEdge.getSrc(), srcFeatures.size());
                    srcFeatures.add((NDArray) localInDEdge.getSrc().getFeature("f").getValue());
                }
                destVertices.compute(vertex, (v, l) -> {
                    if (l == null) {
                        return new ArrayList<>(List.of(srcVertices.get(localInDEdge.getSrc())));
                    }
                    l.add(srcVertices.get(localInDEdge.getSrc()));
                    return l;
                });
            }
        }
        if (srcFeatures.isEmpty()) return;
        NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
        NDArray messages = MESSAGE(srcFeaturesBatched, false).get(0);
        destVertices.forEach((v, list) -> {
            NDArray message = MeanAggregator.bulkReduce(messages.get("{}, :", LifeCycleNDManager.getInstance().create(Longs.toArray(list))));
            Rmi.buildAndRun(
                    new Rmi(Feature.encodeFeatureId("agg", v.getId(), ElementType.VERTEX), "reduce", ElementType.ATTACHED_FEATURE, new Object[]{new NDList(message), list.size()}, true), storage,
                    v.masterPart(),
                    MessageDirection.ITERATE
            );
        });
    }

    /**
     * Forward all local MASTER Vertices to the next layer
     */
    public void inferenceSecondPartStart() {
        NDList features = new NDList();
        NDList aggregators = new NDList();
        List<String> vertexIds = new ArrayList<>();
        for (Vertex v : storage.getVertices()) {
            if (v.state() == ReplicaState.MASTER) {
                features.add((NDArray) v.getFeature("f").getValue());
                aggregators.add((NDArray) v.getFeature("agg").getValue());
                vertexIds.add(v.getId());
            }
        }
        NDList inputsBatched = new NDList(NDArrays.stack(features), NDArrays.stack(aggregators));
        NDArray updatesBatched = UPDATE(inputsBatched, false).get(0);
        for (int i = 0; i < vertexIds.size(); i++) {
            Tensor updateTensor = new Tensor("f", updatesBatched.get(i), false, (short) -1);
            updateTensor.attachedTo = Tuple3.of(ElementType.VERTEX, vertexIds.get(i), null);
            storage.layerFunction.message(new GraphOp(Op.COMMIT, storage.layerFunction.getCurrentPart(), updateTensor), MessageDirection.FORWARD);
        }
    }

    /**
     * Manager training and batched inference script
     * <p>
     * BackwardBarrier -> FirstTrainingPart then Second Training Part and sync model
     * ForwardBarrier -> Skip one iteration for propegating features -> Inference FirstPart(reduce) -> Inference Second Part(forward)
     * </p>
     */
    @Override
    public void onOperatorEvent(BaseOperatorEvent event) {
        super.onOperatorEvent(event);
        if (event instanceof BackwardBarrier) {
            if (++numTrainingSyncMessages == 1) {
                storage.layerFunction.runForAllLocalParts(this::trainFirstPart);
                storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.ITERATE)), MessageDirection.ITERATE);
            } else {
                storage.layerFunction.runForAllLocalParts(this::trainSecondPart);
                if (storage.layerFunction.isFirst())
                    storage.layerFunction.broadcastMessage(new GraphOp(new ForwardBarrier(MessageDirection.ITERATE)), MessageDirection.ITERATE);
                else
                    storage.layerFunction.broadcastMessage(new GraphOp(new BackwardBarrier(MessageDirection.BACKWARD)), MessageDirection.BACKWARD);
                numTrainingSyncMessages = 0;
                modelServer.getParameterStore().sync();
            }
        } else if (event instanceof ForwardBarrier) {
            // first 2 for updating the model, then reset agg and send new messages
            if (++numForwardSyncMessages == 2) {
                storage.layerFunction.runForAllLocalParts(this::inferenceFirstPartStart);
            } else if (numForwardSyncMessages == 3) {
                // Ready to forward
                storage.layerFunction.runForAllLocalParts(this::inferenceSecondPartStart);
            }
            if (numForwardSyncMessages < 3) {
                storage.layerFunction.broadcastMessage(new GraphOp(new ForwardBarrier(MessageDirection.ITERATE)), MessageDirection.ITERATE);
            } else {
                storage.layerFunction.broadcastMessage(new GraphOp(new ForwardBarrier(MessageDirection.FORWARD)), MessageDirection.FORWARD);
                numForwardSyncMessages = 0;
                // @todo add embedding plugin start
            }
        }
    }
}
