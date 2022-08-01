package plugins.embedding_layer;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.nn.gnn.GNNBlock;
import ai.djl.pytorch.engine.LifeCycleNDManager;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import features.Tensor;
import functions.metrics.MovingAverageCounter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.Preconditions;
import plugins.ModelServer;

import java.util.Objects;

public class StreamingGNNEmbeddingLayer extends Plugin implements GNNEmbeddingPlugin {
    public final String modelName; // Model name to identify the ParameterStore

    public final boolean createVertexEmbeddings; // Do we feed external features or should this generate by itself

    protected transient ModelServer modelServer; // ParameterServer Plugin

    protected boolean RUNNING = true; // Running is true of false

    protected transient Counter throughput; // Throughput counter, only used for last layer

    protected transient Counter latency; // Throughput counter, only used for last layer

    public StreamingGNNEmbeddingLayer() {
        super();
        modelName = null;
        createVertexEmbeddings = false;
    }

    public StreamingGNNEmbeddingLayer(String modelName) {
        this(modelName, false);
    }

    public StreamingGNNEmbeddingLayer(String modelName, boolean createVertexEmbeddings) {
        super(String.format("%s-inferencer", modelName));
        this.createVertexEmbeddings = createVertexEmbeddings;
        this.modelName = modelName;
    }

    @Override
    public void open() throws Exception {
        super.open();
        assert storage != null;
        modelServer = (ModelServer) storage.getPlugin(String.format("%s-server", modelName));
        throughput = new SimpleCounter();
        latency = new MovingAverageCounter(1000);
        storage.layerFunction.getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughput));
        storage.layerFunction.getRuntimeContext().getMetricGroup().counter("latency", latency);
    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     *
     * @param element Vertex to be initialized
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = LifeCycleNDManager.getInstance().zeros(modelServer.getInputShape().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(aggStart, true));
            if (createVertexEmbeddings && storage.layerFunction.isFirst()) {
                NDArray embeddingRandom = LifeCycleNDManager.getInstance().ones(modelServer.getInputShape().get(0).getValue()); // Initialize to random value
                // @todo Can make it as mean of some existing features to tackle the cold-start problem
                element.setFeature("feature", new Tensor(embeddingRandom));
            }
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (!RUNNING) return;
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
        if (!RUNNING) return;
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
        NDArray ft = (NDArray) (v.getFeature("feature")).getValue();
        NDArray agg = (NDArray) (v.getFeature("agg")).getValue();
        NDArray update = UPDATE(new NDList(ft, agg), false).get(0);
        Tensor tmp = new Tensor("feature", update, false, v.masterPart());
        tmp.attachedTo = Tuple2.of(ElementType.VERTEX, v.getId());
        throughput.inc();
        latency.inc(storage.layerFunction.getTimerService().currentProcessingTime() - storage.layerFunction.currentTimestamp());
        storage.layerFunction.message(new GraphOp(Op.COMMIT, tmp.masterPart(), tmp), MessageDirection.FORWARD);
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
                        .toElement(Feature.encodeAttachedFeatureId("agg", edge.getDest().getId()), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .addDestination(edge.getDest().masterPart())
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
                        .toElement(Feature.encodeAttachedFeatureId("agg", edge.getDest().getId()), ElementType.FEATURE)
                        .where(MessageDirection.ITERATE)
                        .method("replace")
                        .hasUpdate()
                        .addDestination(edge.getDest().masterPart())
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
    @Override
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
    @Override
    public NDList MESSAGE(NDList features, boolean training) {
        return ((GNNBlock) modelServer.getModel().getBlock()).getMessageBlock().forward(modelServer.getParameterStore(), features, training);

    }

    /**
     * @param edge Edge
     * @return Is the Edge ready to pass on the message
     */
    @Override
    public boolean messageReady(Edge edge) {
        return edge.getSrc().containsFeature("feature");
    }

    /**
     * @param vertex Vertex
     * @return Is the Vertex ready to be updated
     */
    @Override
    public boolean updateReady(Vertex vertex) {
        return vertex != null && vertex.state() == ReplicaState.MASTER && vertex.containsFeature("feature") && vertex.containsFeature("agg");
    }

    @Override
    public boolean usingTrainableEmbeddings() {
        return createVertexEmbeddings;
    }

    @Override
    public boolean usingBatchingOutput() {
        return false;
    }

    @Override
    public void stop() {
        RUNNING = false;
    }

    @Override
    public void start() {
        RUNNING = true;
    }
}
