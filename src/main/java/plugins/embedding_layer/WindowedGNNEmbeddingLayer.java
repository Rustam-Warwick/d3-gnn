package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import org.apache.flink.util.Preconditions;
import plugins.ModelServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class WindowedGNNEmbeddingLayer extends Plugin {
    public final String modelName; // Model name to identify the ParameterStore

    public final boolean externalFeatures; // Do we expect external features or have to initialize features on the first layer

    public final int windowInterval; // Window Interval for graph element updates in milliseconds

    public transient ModelServer modelServer; // ParameterServer Plugin

    public transient Batchifier batchifier; // Batchifier for the windowed data

    public WindowedGNNEmbeddingLayer(String modelName, boolean externalFeatures) {
        this(modelName, externalFeatures, 1000);
    }

    public WindowedGNNEmbeddingLayer(String modelName, boolean externalFeatures, int windowInterval){
        super(String.format("%s-inferencer", modelName));
        this.externalFeatures = externalFeatures;
        this.modelName = modelName;
        this.windowInterval = windowInterval;
    }

    @Override
    public void open() {
        super.open();
        assert storage != null;
        batchifier = new StackBatchifier();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        try {
            storage.layerFunction.getWrapperContext().applyToAllKeys(()->{
                Feature<HashMap<String, Long>, HashMap<String, Long>> elementUpdates = new Feature<>("elementUpdates", new HashMap<>(),true, storage.layerFunction.getCurrentPart());
                elementUpdates.setStorage(storage);
                elementUpdates.create();
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     *
     * @param element Vertex to be initialized
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = BaseNDManager.threadNDManager.get().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(aggStart, true));

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = BaseNDManager.threadNDManager.get().ones(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("feature", new Tensor(embeddingRandom));
            }
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.VERTEX) {
            initVertex((Vertex) element); // Initialize the agg and the Feature if it is the first layer
        } else if (element.elementType() == ElementType.EDGE) {
            Edge edge = (Edge) element;
            if (messageReady(edge)) {
                NDList msg = MESSAGE(new NDList((NDArray) edge.src.getFeature("feature").getValue()), false);
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msg.get(0), 1)
                        .buildAndRun(storage);
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo!=null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                reduceOutEdges((Vertex) feature.getElement());
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            } else if (feature.attachedTo!=null && feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if (feature.attachedTo!= null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
            if (storage.layerFunction.isFirst() && feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
        }
    }

    /**
     * Push the embedding of this vertex to the next layer
     * After first layer, this is only fushed if agg and features are in sync
     *
     * @param v Vertex
     */
    @SuppressWarnings("all")
    public void forward(Vertex v) {
        long currentProcessingTime = storage.layerFunction.getTimerService().currentProcessingTime();
        long thisElementUpdateTime = currentProcessingTime + windowInterval;
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 1000.0) * 1000);
        Feature<HashMap<String, Long>, HashMap<String, Long>> elementUpdates = (Feature<HashMap<String, Long>, HashMap<String, Long>>) storage.getFeature("elementUpdates");
        elementUpdates.getValue().put(v.getId(), thisElementUpdateTime);
        storage.updateElement(elementUpdates);
        storage.layerFunction.getTimerService().registerProcessingTimeTimer(timerTime);
    }

    /**
     * Actually send the elements
     * @param timestamp firing timestamp
     */
    @Override
    public void onTimer(long timestamp) {
        super.onTimer(timestamp);
        Feature<HashMap<String, Long>, HashMap<String, Long>> elementUpdates = (Feature<HashMap<String, Long>, HashMap<String, Long>>) storage.getFeature("elementUpdates");
        List<NDList> inputs = new ArrayList<>();
        List<Vertex> vertices = new ArrayList<>();
        elementUpdates.getValue().forEach((key, val)->{
            if(val <= timestamp){
                // Send it
                Vertex v = storage.getVertex(key);
                if(updateReady(v)){
                    NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
                    NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
                    inputs.add(new NDList(ft, agg));
                    vertices.add(v);
                }
            }
        });
        if(inputs.isEmpty())return;
        NDList batch_inputs = batchifier.batchify(inputs.toArray(NDList[]::new));
        NDList batch_updates = UPDATE(batch_inputs, false);
        NDList[] updates = batchifier.unbatchify(batch_updates);
        for (int i = 0; i < updates.length; i++) {
            elementUpdates.getValue().remove(vertices.get(i).getId());
            Vertex messageVertex = vertices.get(i).copy();
            messageVertex.setFeature("feature", new Tensor(updates[i].get(0)));
            storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex), MessageDirection.FORWARD);
        }
        storage.updateFeature(elementUpdates);
    }

    /**
     * Given vertex reduce all of its out edges
     *
     * @param v Vertex
     */
    public void reduceOutEdges(Vertex v) {
        Preconditions.checkNotNull(v);
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(v, EdgeType.OUT);
        NDArray msg = null;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                if (Objects.isNull(msg)) {
                    msg = MESSAGE(new NDList((NDArray) v.getFeature("feature").getValue()), false).get(0);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msg, 1)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        Preconditions.checkNotNull(newFeature.getElement());
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                if (Objects.isNull(msgOld)) {
                    msgOld = MESSAGE(new NDList(oldFeature.getValue()), false).get(0);
                    msgNew = MESSAGE(new NDList(newFeature.getValue()), false).get(0);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("replace")
                        .hasUpdate()
                        .addDestination(edge.dest.masterPart())
                        .withArgs(msgNew, msgOld)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to TempManager
     *
     * @param feature  Source Feature list
     * @param training training enabled
     * @return Next layer feature
     */
    public NDList UPDATE(NDList feature, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getUpdateBlock().forward(modelServer.getParameterStore(), feature, training);
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param features Source vertex Features or Batch
     * @param training Should we construct the training graph
     * @return Message Tensor to be send to the aggregator
     */
    public NDList MESSAGE(NDList features, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), features, training);
    }

    /**
     * @param edge Edge
     * @return Is the Edge ready to pass on the message
     */
    public boolean messageReady(Edge edge) {
        return Objects.nonNull(edge.src.getFeature("feature"));
    }

    /**
     * @param vertex Vertex
     * @return Is the Vertex ready to be updated
     */
    public boolean updateReady(Vertex vertex) {
        return vertex != null && vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && Objects.nonNull(vertex.getFeature("agg"));
    }
}
