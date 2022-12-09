package plugins.gnn_embedding;

import ai.djl.ndarray.*;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import elements.*;
import elements.annotations.RemoteFunction;
import elements.enums.*;
import elements.features.Aggregator;
import elements.features.MeanAggregator;
import elements.features.Tensor;
import org.apache.flink.streaming.api.operators.graph.OutputTags;
import operators.events.BackwardBarrier;
import operators.events.ForwardBarrier;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.shaded.guava30.com.google.common.primitives.Longs;

import java.util.*;

/**
 * Plugin that manages the training of GNNEmbeddingLayer
 */
public class GNNEmbeddingTrainingPlugin extends BaseGNNEmbeddingPlugin {
    public int numForwardSyncMessages; // #Sync Messages sent for starting batched inference

    public int numTrainingSyncMessages; // #Sync messages sent for backward messages

    public transient Map<Short, Tuple2<NDArrayCollector<String>, NDArrayCollector<String>>> collectors;

    public GNNEmbeddingTrainingPlugin(String modelName, boolean trainableVertexEmbeddings) {
        super(modelName, "trainer", trainableVertexEmbeddings);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    /**
     * Collect vertex -> dLoss/doutput, where vertices are masters in this part
     * @param gradients Gradients for VJP backward iteration
     */
    @RemoteFunction(triggerUpdate = false)
    public void collect(HashMap<String, NDArray> gradients) {
        if (gradients.isEmpty()) return;
        collectors.computeIfAbsent(getPart(), (key) -> Tuple2.of(new NDArrayCollector<>(true), new NDArrayCollector<>(true)));
        collectors.get(getPart()).f0.putAll(gradients);
    }

    /**
     * Collect aggregator messages where Aggregator -> dLoss/dAgg
     */
    @RemoteFunction(triggerUpdate = false)
    public void collectAggregators(HashMap<String, NDArray> gradients) {
        if (gradients.isEmpty()) return;
        collectors.computeIfAbsent(getPart(), (key) -> Tuple2.of(new NDArrayCollector<>(true), new NDArrayCollector<>(true)));
        NDArrayCollector<String> collector = collectors.get(getPart()).f1;
        for (Map.Entry<String, NDArray> stringNDArrayEntry : gradients.entrySet()) {
            Vertex v = getRuntimeContext().getStorage().getVertex(stringNDArrayEntry.getKey());
            for (DirectedEdge incidentDirectedEdge : getRuntimeContext().getStorage().getIncidentEdges(v, EdgeType.IN)) {
                collector.put(incidentDirectedEdge.getSrc().getId(), stringNDArrayEntry.getValue());
            }
        }
    }

    /**
     * First part of training resposible for getting triggerUpdate gradients and output gradients
     * <p>
     * Assumes to contain only gradients for master vertices with aggregators allocated already
     * </p>
     */
    public void trainFirstPart() {
        try {
            if (!collectors.containsKey(getPart())) return;
            NDArrayCollector<String> collectedGradients = collectors.get(getPart()).f0;
            if (collectedGradients.isEmpty()) return;

            // ---------- Prepare data for triggerUpdate model inputs(feature, aggregator)

            ArrayList<Vertex> vertices = new ArrayList<>(collectedGradients.size());
            NDList featuresList = new NDList(collectedGradients.size());
            NDList aggregatorList = new NDList(collectedGradients.size());
            NDList inputGradientsList = new NDList(collectedGradients.size());
            for (Map.Entry<String, NDArray> entry : collectedGradients.entrySet()) {
                Vertex v = getRuntimeContext().getStorage().getVertex(entry.getKey());
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

            NDList batchedPredictions = ((GNNBlock) modelServer.getModel().getBlock()).update(modelServer.getParameterStore(), batchedInputs, true);

            JniUtils.backward((PtNDArray) batchedPredictions.get(0), (PtNDArray) batchedInputGradients.get(0), false, false);

            NDList gradients = new NDList(batchedInputs.get(0).getGradient(), batchedInputs.get(1).getGradient());

            // ------------------Collect Aggregation messages + Backward messages(If not the first layer)

            HashMap<Short, NDArrayCollector<String>> aggGradsPerPart = new HashMap<>(); // per part aggregator with its gradient
            HashMap<String, NDArray> backwardGrads = getRuntimeContext().isFirst() ? null : new NDArrayCollector<>(false);
            for (int i = 0; i < collectedGradients.size(); i++) {
                NDArray previousFeatureGrad = gradients.get(0).get(i);
                NDArray aggGrad = gradients.get(1).get(i);
                Aggregator aggregator = (Aggregator) vertices.get(i).getFeature("agg");
                if (backwardGrads != null) {
                    backwardGrads.put(vertices.get(i).getId(), previousFeatureGrad);
                }
                if (aggregator.reducedCount() > 0) {
                    NDArray messagesGrad = aggregator.grad(aggGrad);
                    for (Short replicaPart : vertices.get(i).getReplicaParts()) {
                        aggGradsPerPart.computeIfAbsent(replicaPart, (key) -> new NDArrayCollector<>(false));
                        aggGradsPerPart.get(replicaPart).put(vertices.get(i).getId(), messagesGrad);
                    }

                    aggGradsPerPart.computeIfAbsent(getPart(), (key) -> new NDArrayCollector<>(false));
                    aggGradsPerPart.get(getPart()).put(vertices.get(i).getId(), messagesGrad);
                }
            }

            // ---------- Send AGG messages

            for (Map.Entry<Short, NDArrayCollector<String>> entry : aggGradsPerPart.entrySet()) {
                Rmi.buildAndRun(
                        getId(),
                        getType(),
                        "collectAggregators",
                        entry.getKey(),
                        OutputTags.ITERATE_OUTPUT_TAG,
                        entry.getValue()
                );
            }

            // -------------- Send backward messages if it exists

            if (Objects.nonNull(backwardGrads)) {

                Rmi.buildAndRun(
                        getId(),
                        getType(),
                        "collect",
                        getPart(),
                        OutputTags.BACKWARD_OUTPUT_TAG,
                        backwardGrads
                );
            }

            collectedGradients.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Second part of training responsible for getting output gradients
     * <p>
     * Batch compute gradients for output function as well as previous layer updates
     * Previous layer updates are needed only if this is not First layer of GNN
     * </p>
     */
    public void trainSecondPart() {
        try {
            if (!collectors.containsKey(getPart())) return;
            NDArrayCollector<String> collectedAggregators = collectors.get(getPart()).f1;
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
        for (Vertex vertex : getRuntimeContext().getStorage().getVertices()) {
            if (vertex.state() == ReplicaState.MASTER) ((Aggregator) vertex.getFeature("agg")).reset();
            Iterable<DirectedEdge> localInEdges = getRuntimeContext().getStorage().getIncidentEdges(vertex, EdgeType.IN);
            for (DirectedEdge localInDirectedEdge : localInEdges) {
                if (!srcVertices.containsKey(localInDirectedEdge.getSrc())) {
                    srcVertices.put(localInDirectedEdge.getSrc(), srcFeatures.size());
                    srcFeatures.add((NDArray) localInDirectedEdge.getSrc().getFeature("f").getValue());
                }
                destVertices.compute(vertex, (v, l) -> {
                    if (l == null) {
                        return new ArrayList<>(List.of(srcVertices.get(localInDirectedEdge.getSrc())));
                    }
                    l.add(srcVertices.get(localInDirectedEdge.getSrc()));
                    return l;
                });
            }
        }
        if (srcFeatures.isEmpty()) return;
        NDList srcFeaturesBatched = new NDList(NDArrays.stack(srcFeatures));
        NDArray messages = MESSAGE(srcFeaturesBatched, false).get(0);
        destVertices.forEach((v, list) -> {
            NDArray message = MeanAggregator.bulkReduce(messages.get("{}, :", BaseNDManager.getManager().create(Longs.toArray(list))));
            Rmi.buildAndRun(
                    Feature.encodeAttachedFeatureId(ElementType.VERTEX, v.getId(), "agg"),
                    ElementType.ATTACHED_FEATURE,
                    "reduce",
                    v.getMasterPart(),
                    OutputTags.BACKWARD_OUTPUT_TAG,
                    new NDList(message)
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
        for (Vertex v : getRuntimeContext().getStorage().getVertices()) {
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
            updateTensor.ids = Tuple3.of(ElementType.VERTEX, vertexIds.get(i), null);
            getRuntimeContext().output(new GraphOp(Op.COMMIT, getRuntimeContext().getCurrentPart(), updateTensor));
        }
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        super.handleOperatorEvent(evt);
        if (evt instanceof BackwardBarrier) {
            if (++numTrainingSyncMessages == 1) {
                getRuntimeContext().runForAllLocalParts(this::trainFirstPart);
                getRuntimeContext().broadcast(new GraphOp(new BackwardBarrier(MessageDirection.ITERATE)), OutputTags.ITERATE_OUTPUT_TAG);
            } else {
                getRuntimeContext().runForAllLocalParts(this::trainSecondPart);
                if (getRuntimeContext().isFirst())
                    getRuntimeContext().broadcast(new GraphOp(new ForwardBarrier(MessageDirection.ITERATE)), OutputTags.ITERATE_OUTPUT_TAG);
                else
                    getRuntimeContext().broadcast(new GraphOp(new BackwardBarrier(MessageDirection.BACKWARD)), OutputTags.BACKWARD_OUTPUT_TAG);
                numTrainingSyncMessages = 0;
                modelServer.getParameterStore().sync();
            }
        } else if (evt instanceof ForwardBarrier) {
            // first 2 for updating the model, then reset agg and send new messages
            if (++numForwardSyncMessages == 2) {
                getRuntimeContext().runForAllLocalParts(this::inferenceFirstPartStart);
            } else if (numForwardSyncMessages == 3) {
                // Ready to forward
                getRuntimeContext().runForAllLocalParts(this::inferenceSecondPartStart);
            }
            if (numForwardSyncMessages < 3) {
                getRuntimeContext().broadcast(new GraphOp(new ForwardBarrier(MessageDirection.ITERATE)), OutputTags.ITERATE_OUTPUT_TAG);
            } else {
                getRuntimeContext().broadcast(new GraphOp(new ForwardBarrier(MessageDirection.FORWARD)));
                numForwardSyncMessages = 0;
                // @todo add embedding plugin start
            }
        }
    }

}
