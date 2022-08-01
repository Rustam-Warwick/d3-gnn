package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import ai.djl.translate.Batchifier;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import operators.BaseWrapperOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.ExceptionUtils;
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

    private transient Counter throughput; // Throughput counter, only used for last layer

    private transient Counter windowThroughput; // Throughput counter, only used for last layer

    private transient Counter latency; // Throughput counter, only used for last layer

    public WindowedGNNEmbeddingLayer(String modelName, boolean externalFeatures) {
        this(modelName, externalFeatures, 1000);
    }

    public WindowedGNNEmbeddingLayer(String modelName, boolean externalFeatures, int windowInterval) {
        super(String.format("%s-inferencer", modelName));
        this.externalFeatures = externalFeatures;
        this.modelName = modelName;
        this.windowInterval = windowInterval;
    }

    @Override
    public void open() throws Exception {
        super.open();
        assert storage != null;
        batchifier = new StackBatchifier();
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        throughput = new SimpleCounter();
        windowThroughput = new SimpleCounter();
        latency = new MovingAverageCounter(1000);
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("windowThroughput", new MeterView(windowThroughput));
        storage.layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
        try {
            storage.layerFunction.getWrapperContext().runForAllKeys(() -> {
                Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = new Feature<>("elementUpdates", new HashMap<>(), true, storage.layerFunction.getCurrentPart());
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
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getModel().describeInput().get(0).getValue(), modelServer.getModel().getDataType());
            element.setFeature("agg", new MeanAggregator(aggStart, true));

            if (!externalFeatures && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = LifeCycleNDManager.getInstance().ones(modelServer.getModel().describeInput().get(0).getValue(), modelServer.getModel().getDataType()); // Initialize to random value
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
                NDList msg = MESSAGE(new NDList((NDArray) edge.getSrc().getFeature("feature").getValue()), false);
                new RemoteInvoke()
                        .toElement(Feature.encodeAttachedFeatureId("agg", edge.getDest().getId()), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.getDest().masterPart())
                        .withArgs(msg.get(0), 1)
                        .buildAndRun(storage);
            }
        } else if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                reduceOutEdges((Vertex) feature.getElement());
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            } else if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
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
            if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName())) {
                updateOutEdges((Tensor) feature, (Tensor) oldFeature);
                if (updateReady((Vertex) feature.getElement())) forward((Vertex) feature.getElement());
            }
            if (feature.attachedTo != null && feature.attachedTo.f0 == ElementType.VERTEX && "agg".equals(feature.getName())) {
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
        long timerTime = (long) (Math.ceil((thisElementUpdateTime) / 500.0) * 500);
        Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = (Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>>) storage.getFeature("elementUpdates");
        elementUpdates.getValue().put(v.getId(), Tuple2.of(thisElementUpdateTime, storage.layerFunction.currentTimestamp()));
        storage.updateElement(elementUpdates);
        storage.layerFunction.getTimerService().registerProcessingTimeTimer(timerTime);
        windowThroughput.inc();
    }

    /**
     * Actually send the elements
     *
     * @param timestamp firing timestamp
     */
    @Override
    public void onTimer(long timestamp) {
        super.onTimer(timestamp);
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
            Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>> elementUpdates = (Feature<HashMap<String, Tuple2<Long, Long>>, HashMap<String, Tuple2<Long, Long>>>) storage.getFeature("elementUpdates");
            List<NDList> inputs = new ArrayList<>();
            List<Vertex> vertices = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            elementUpdates.getValue().forEach((key, val) -> {
                if (val.f0 <= timestamp) {
                    // Send it
                    Vertex v = storage.getVertex(key);
                    if (updateReady(v)) {
                        NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
                        NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
                        inputs.add(new NDList(ft, agg));
                        vertices.add(v);
                        timestamps.add(val.f1);
                    }
                }
            });
            if (inputs.isEmpty()) return;
            NDList batch_inputs = batchifier.batchify(inputs.toArray(NDList[]::new));
            NDList batch_updates = UPDATE(batch_inputs, false);
            NDList[] updates = batchifier.unbatchify(batch_updates);
            for (int i = 0; i < updates.length; i++) {
                throughput.inc();
                latency.inc(storage.layerFunction.getTimerService().currentProcessingTime() - timestamps.get(i));
                elementUpdates.getValue().remove(vertices.get(i).getId());
                Vertex messageVertex = vertices.get(i);
                Tensor updateTensor = new Tensor("feature", updates[i].get(0), false, messageVertex.masterPart());
                updateTensor.attachedTo = Tuple2.of(ElementType.VERTEX, messageVertex.getId());
                storage.layerFunction.message(new GraphOp(Op.COMMIT, updateTensor.masterPart(), updateTensor), MessageDirection.FORWARD, timestamps.get(i));
            }
            storage.updateFeature(elementUpdates);
        } catch (Exception e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
        }
    }

    /**
     * Given vertex reduce all of its out edges
     *
     * @param v Vertex
     */
    public void reduceOutEdges(Vertex v) {
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
            Preconditions.checkNotNull(v);
            Iterable<Edge> outEdges = this.storage.getIncidentEdges(v, EdgeType.OUT);
            NDArray msg = null;
            for (Edge edge : outEdges) {
                if (this.messageReady(edge)) {
                    if (Objects.isNull(msg)) {
                        msg = MESSAGE(new NDList((NDArray) v.getFeature("feature").getValue()), false).get(0);
                    }
                    new RemoteInvoke()
                            .toElement(Feature.encodeAttachedFeatureId("agg", edge.getDest().getId()), ElementType.FEATURE)
                            .where(MessageDirection.ITERATE)
                            .method("reduce")
                            .hasUpdate()
                            .addDestination(edge.getDest().masterPart())
                            .withArgs(msg, 1)
                            .buildAndRun(storage);
                }
            }
        } catch (Exception e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature Updaated new Feature
     * @param oldFeature Updated old Feature
     */
    public void updateOutEdges(Tensor newFeature, Tensor oldFeature) {
        try (LifeCycleNDManager.Scope ignored = LifeCycleNDManager.getInstance().getScope().start()) {
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
                            .toElement(Feature.encodeAttachedFeatureId("agg", edge.getDest().getId()), ElementType.FEATURE)
                            .where(MessageDirection.ITERATE)
                            .method("replace")
                            .hasUpdate()
                            .addDestination(edge.getDest().masterPart())
                            .withArgs(msgNew, msgOld)
                            .buildAndRun(storage);
                }
            }
        } catch (Exception e) {
            BaseWrapperOperator.LOG.error(ExceptionUtils.stringifyException(e));
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
        return Objects.nonNull(edge.getSrc().getFeature("feature"));
    }

    /**
     * @param vertex Vertex
     * @return Is the Vertex ready to be updated
     */
    public boolean updateReady(Vertex vertex) {
        return vertex != null && vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && Objects.nonNull(vertex.getFeature("agg"));
    }
}
