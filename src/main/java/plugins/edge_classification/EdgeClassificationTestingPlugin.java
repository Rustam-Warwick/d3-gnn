package plugins.edge_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import operators.events.InferenceBarrier;
import operators.events.StartTraining;
import operators.events.StopTraining;
import operators.events.TrainBarrier;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import plugins.ModelServer;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class EdgeClassificationTestingPlugin extends Plugin {

    public final String modelName;

    public final int MAX_BATCH_SIZE; // Overall batch size across output layers
    public final SerializableLoss loss; // Loss function
    public transient int LOCAL_BATCH_SIZE; // Estimate batch size for this operator
    public transient ModelServer modelServer; // Model Server Attached
    public transient StackBatchifier batchifier; // Helper for batching data

    public int BATCH_COUNT = 0;


    public EdgeClassificationTestingPlugin(String modelName, SerializableLoss loss, int MAX_BATCH_SIZE) {
        super(String.format("%s-trainer", modelName));
        this.MAX_BATCH_SIZE = MAX_BATCH_SIZE;
        this.loss = loss;
        this.modelName = modelName;
    }

    public EdgeClassificationTestingPlugin(String modelName, SerializableLoss loss) {
        this(modelName, loss, 1024);
    }

    @Override
    public void open() throws Exception {
        super.open();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        LOCAL_BATCH_SIZE = MAX_BATCH_SIZE / storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
        batchifier = new StackBatchifier();
        storage.layerFunction.runForAllLocalParts(() -> {
            setFeature("trainSrcVertices", new Feature<>(new HashMap<String, HashMap<String, Byte>>(LOCAL_BATCH_SIZE), true, null)); // Ready for training
            setFeature("trainDestVertices", new Feature<>(new HashMap<String, HashMap<String, Byte>>(LOCAL_BATCH_SIZE), true, null)); // Pending for Features label is here
            setFeature("readyTrainingEdges", new Feature<>(new HashSet<String>(LOCAL_BATCH_SIZE), true, null)); // Pending for Features label is here
        });
    }

    /**
     * Add value to the batch. If filled send event to the coordinator
     */
    public void incrementBatchCount() {
        BATCH_COUNT++;
        if (BATCH_COUNT == LOCAL_BATCH_SIZE) {
            storage.layerFunction.operatorEventMessage(new StartTraining());
        }
    }

    public void mergeTrainingDataState(@Nonnull Edge e) {
        // 1. Add edge to the waiting list
        Feature<HashSet<String>, HashSet<String>> readyEdges = (Feature<HashSet<String>, HashSet<String>>) getFeature("readyTrainingEdges");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> srcVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainSrcVertices");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> destVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainDestVertices");
        if (e.getSrc().containsFeature("feature") && e.getDest().containsFeature("feature")) {
            // All dependecies are here,
            readyEdges.getValue().add(e.getId());
            storage.updateFeature(readyEdges);
            incrementBatchCount();
            return;
        } else if (e.getSrc().containsFeature("feature") || e.getDest().containsFeature("feature")) {
            // Either sides are not here yet
            srcVertexState.getValue().compute(e.getSrc().getId(), (src, value) -> {
                if (value == null) value = new HashMap<>(5);
                value.put(e.getDest().getId(), (byte) 1);
                return value;
            });

            destVertexState.getValue().compute(e.getDest().getId(), (dest, value) -> {
                if (value == null) value = new HashMap<>(5);
                value.put(e.getSrc().getId(), (byte) 1);
                return value;
            });
        } else {
            srcVertexState.getValue().compute(e.getSrc().getId(), (src, value) -> {
                if (value == null) value = new HashMap<>(5);
                value.put(e.getDest().getId(), (byte) 0);
                return value;
            });

            destVertexState.getValue().compute(e.getDest().getId(), (dest, value) -> {
                if (value == null) value = new HashMap<>(5);
                value.put(e.getSrc().getId(), (byte) 0);
                return value;
            });
        }

        storage.updateFeature(srcVertexState);
        storage.updateFeature(destVertexState);
    }

    public void mergeTrainingDataState(@Nonnull Vertex v) {
        Feature<HashSet<String>, HashSet<String>> readyEdges = (Feature<HashSet<String>, HashSet<String>>) getFeature("readyTrainingEdges");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> srcVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainSrcVertices");
        Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>> destVertexState = (Feature<HashMap<String, HashMap<String, Byte>>, HashMap<String, HashMap<String, Byte>>>) getFeature("trainDestVertices");
        srcVertexState.getValue().computeIfPresent(v.getId(), (src, value) -> {
            value.replaceAll((dest, state) -> {
                state++;
                if (state == 2) {
                    // Both are here
                    incrementBatchCount();
                    destVertexState.getValue().get(dest).remove(src);
                    readyEdges.getValue().add(Edge.encodeEdgeId(src, dest));
                    return null;
                }

                return state;
            });

            if (value.isEmpty()) return null;
            return value;
        });

        destVertexState.getValue().computeIfPresent(v.getId(), (dest, value) -> {
            value.replaceAll((src, state) -> {
                state++;
                if (state == 2) {
                    // Both are here
                    incrementBatchCount();
                    srcVertexState.getValue().get(src).remove(dest);
                    readyEdges.getValue().add(Edge.encodeEdgeId(src, dest));
                    return null;
                }

                return state;
            });

            if (value.isEmpty()) return null;
            return value;
        });
        storage.updateFeature(readyEdges);
        storage.updateFeature(srcVertexState);
        storage.updateFeature(destVertexState);
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.EDGE && "trainLabel".equals(feature.getName())) {
                // Training label arrived
                mergeTrainingDataState((Edge) feature.getElement());
            } else if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                mergeTrainingDataState((Vertex) feature.getElement());
            }
        }
    }

    /**
     * Starts training when data has arrived
     */
    public void startTraining() {
        Feature<HashSet<String>, HashSet<String>> readyEdges = (Feature<HashSet<String>, HashSet<String>>) getFeature("readyTrainingEdges");
        if (!readyEdges.getValue().isEmpty()) {
            // 1. Collect Data
            List<NDList> inputs = new ArrayList<>();
            List<NDList> labels = new ArrayList<>();
            List<Edge> edges = new ArrayList<>();
            for (String eId : readyEdges.getValue()) {
                Edge e = storage.getEdge(eId);
                ((NDArray) e.getSrc().getFeature("feature").getValue()).setRequiresGradient(true);
                ((NDArray) e.getDest().getFeature("feature").getValue()).setRequiresGradient(true);
                inputs.add(new NDList((NDArray) e.getSrc().getFeature("feature").getValue(), (NDArray) e.getDest().getFeature("feature").getValue()));
                labels.add(new NDList((NDArray) e.getFeature("trainLabel").getValue()));
                edges.add(e);
            }
            // 2. Local BackProp
            NDList batchedInputs = batchifier.batchify(inputs.toArray(NDList[]::new));
            NDList batchedLabels = batchifier.batchify(labels.toArray(NDList[]::new));

            try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start(batchedInputs, batchedLabels)) {
                NDList predictions = (modelServer.getModel().getBlock()).forward(modelServer.getParameterStore(), batchedInputs, true);
                NDArray meanLoss = loss.evaluate(batchedLabels, predictions);
                JniUtils.backward((PtNDArray) meanLoss, (PtNDArray) LifeCycleNDManager.getInstance().ones(new Shape()), false, false);

                // 3. Collect Vertex Gradients
                HashMap<Short, HashMap<String, NDArray>> backwardPartGrads = new HashMap<>();
                edges.forEach(item -> {
                    backwardPartGrads.compute(item.getSrc().masterPart(), (part, value) -> {
                        if (value == null) value = new HashMap<>();
                        value.compute(item.getSrc().getId(), (srcId, gradient) -> {
                            if (gradient == null)
                                return ((NDArray) item.getSrc().getFeature("feature").getValue()).getGradient();
                            else
                                gradient.addi(((NDArray) item.getSrc().getFeature("feature").getValue()).getGradient());
                            return gradient;
                        });
                        return value;
                    });

                    backwardPartGrads.compute(item.getDest().masterPart(), (part, value) -> {
                        if (value == null) value = new HashMap<>();
                        value.compute(item.getDest().getId(), (destId, gradient) -> {
                            if (gradient == null)
                                return ((NDArray) item.getDest().getFeature("feature").getValue()).getGradient();
                            else
                                gradient.addi(((NDArray) item.getDest().getFeature("feature").getValue()).getGradient());
                            return gradient;
                        });
                        return value;
                    });
                });

                // 4. Send Vertex Gradients
                backwardPartGrads.forEach((key, grads) -> {
                    new RemoteInvoke()
                            .addDestination(key) // Only masters will be here anyway
                            .noUpdate()
                            .method("collect")
                            .toElement(getId(), elementType())
                            .where(MessageDirection.BACKWARD)
                            .withArgs(grads)
                            .buildAndRun(storage);
                });
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                // Cleanup
                edges.forEach(e -> {
                    ((NDArray) e.getSrc().getFeature("feature").getValue()).setRequiresGradient(false);
                    ((NDArray) e.getDest().getFeature("feature").getValue()).setRequiresGradient(false);
                });
                readyEdges.getValue().clear();
                storage.updateFeature(readyEdges);
            }
        }
    }

    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        try {
            if (event instanceof StartTraining) {
                storage.layerFunction.runForAllLocalParts(this::startTraining);
                modelServer.getParameterStore().sync();
                storage.layerFunction.broadcastMessage(new GraphOp(new TrainBarrier((short) storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks())), MessageDirection.BACKWARD);
            } else if (event instanceof InferenceBarrier) {
                storage.layerFunction.operatorEventMessage(new StopTraining());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
