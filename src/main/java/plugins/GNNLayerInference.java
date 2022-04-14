package plugins;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import elements.*;
import features.VTensor;
import helpers.JavaTensor;
import helpers.MyParameterStore;
import iterations.IterationType;
import iterations.RemoteInvoke;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class GNNLayerInference extends Plugin {
    public transient Model messageModel;
    public transient Model updateModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public transient Shape aggregatorShape;
    public transient Shape featureShape;
    public transient int MODEL_VERSION = 0;
    public transient boolean updatePending = false;

    public GNNLayerInference() {
        super("inferencer");
    }

    public abstract Model createMessageModel();

    public abstract Model createUpdateModel();


    @Override
    public void add() {
        super.add();
        this.storage.withPlugin(new GNNLayerTraining());
        this.messageModel = this.createMessageModel();
        this.updateModel = this.createUpdateModel();
        this.parameterStore.canonizeModel(this.messageModel);
        this.parameterStore.canonizeModel(this.updateModel);
        this.parameterStore.loadModel(this.messageModel);
        this.parameterStore.loadModel(this.updateModel);
    }

    @Override
    public void open() {
        super.open();
        this.messageModel = this.createMessageModel();
        this.updateModel = this.createUpdateModel();
        aggregatorShape = this.messageModel.describeOutput().get(0).getValue();
        featureShape = this.updateModel.describeInput().get(0).getValue();
        this.parameterStore.canonizeModel(this.messageModel);
        this.parameterStore.canonizeModel(this.updateModel);
        this.parameterStore.restoreModel(this.messageModel);
        this.parameterStore.restoreModel(this.updateModel);
        this.parameterStore.setNDManager(this.storage.manager.getLifeCycleManager());
    }

    @Override
    public void close() {
        super.close();
        this.messageModel.close();
        this.updateModel.close();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()) {
            case VERTEX: {
                initVertex((Vertex) element);
                reduceInEdges((Vertex) element);
                break;
            }
            case EDGE: {
                Edge edge = (Edge) element;
                if (messageReady(edge)) {
                    NDArray feature = ((VTensor) edge.src.getFeature("feature")).getValue();
                    NDArray msg = this.message(feature, false);
                    new RemoteInvoke()
                            .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                            .where(IterationType.ITERATE)
                            .method("reduce")
                            .hasUpdate()
                            .toDestination(edge.dest.masterPart())
                            .withArgs(MODEL_VERSION, msg, 1)
                            .buildAndRun(storage);
                }
                break;
            }
            case FEATURE: {
                Feature feature = (Feature) element;
                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        forward(parent);
                        reduceOutEdges((VTensor) feature);
                        break;
                    }
                }
                break;
            }
        }
    }

//    @Override
//    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
//        super.updateElementCallback(newElement, oldElement);
//        switch (newElement.elementType()) {
//            case FEATURE: {
//                Feature feature = (Feature) newElement;
//                Feature oldFeature = (Feature) oldElement;
//                switch (feature.getFieldName()) {
//                    case "feature": {
//                        Vertex parent = (Vertex) feature.getElement();
//                        forward(parent);
//                        updateOutEdges((VTensor) feature, (VTensor) oldFeature);
//                        break;
//                    }
//                    case "agg": {
//                        Vertex parent = (Vertex) feature.getElement();
//                        forward(parent);
//                        break;
//                    }
//                }
//            }
//        }
//    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     *
     * @param element
     */
    public void initVertex(Vertex element) {
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = this.storage.manager.getLifeCycleManager().zeros(aggregatorShape);
            element.setFeature("agg", new MeanAggregator(new JavaTensor(aggStart), true));

            if (this.storage.layerFunction.isFirst() && Objects.isNull(element.getFeature("feature"))) {
                NDArray embeddingRandom = this.storage.manager.getLifeCycleManager().randomNormal(featureShape);
                element.setFeature("feature", new VTensor(new Tuple2<>(new JavaTensor(embeddingRandom), this.MODEL_VERSION)));
            }
        }
    }

    public void forward(Vertex v) {
        if (updateReady(v)) {
            NDArray ft = ((VTensor) v.getFeature("feature")).getValue();
            NDArray agg = ((BaseAggregator<?>) v.getFeature("agg")).getValue();
            NDArray update = this.update(ft, agg, false);
            Vertex messageVertex = (Vertex) v.copy();
            messageVertex.setFeature("feature", new VTensor(new Tuple2<>(update, MODEL_VERSION)));
            if(storage.layerFunction.isLast()){
                storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex, IterationType.FORWARD));
            }else{
                storage.layerFunction.message(new GraphOp(Op.COMMIT, messageVertex.masterPart(), messageVertex.getFeature("feature"), IterationType.FORWARD));
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     *
     * @param newFeature
     * @param oldFeature
     */
    public void updateOutEdges(VTensor newFeature, VTensor oldFeature) {
//        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
//        NDArray msgOld = null;
//        NDArray msgNew = null;
//        for (Edge edge : outEdges) {
//            if (this.messageReady(edge)) {
//                if (Objects.isNull(msgOld)) {
//                    msgOld = this.message(oldFeature.getValue(), false);
//                    msgNew = this.message(newFeature.getValue(), false);
//                }
//                new RemoteInvoke()
//                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
//                        .where(IterationType.ITERATE)
//                        .method("replace")
//                        .hasUpdate()
//                        .toDestination(edge.dest.masterPart())
//                        .withArgs(MODEL_VERSION, msgNew, msgOld)
//                        .buildAndRun(storage);
//            }
//        }
    }

    /**
     * Given vertex reduce all the out edges aggregator values
     */
    public void reduceOutEdges(VTensor newFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msg = null;

        for (Edge edge : outEdges) {
            if (this.messageReady(edge)) {
                if (Objects.isNull(msg)) {
                    msg = this.message(newFeature.getValue(), false);
                }
                new RemoteInvoke()
                        .toElement(edge.dest.decodeFeatureId("agg"), ElementType.FEATURE)
                        .where(IterationType.ITERATE)
                        .method("reduce")
                        .hasUpdate()
                        .toDestination(edge.dest.masterPart())
                        .withArgs(MODEL_VERSION, msg, 1)
                        .buildAndRun(storage);
            }
        }
    }

    /**
     * Given Vertex reduce all the in-edges in bulk
     *
     * @param vertex
     */
    public void reduceInEdges(Vertex vertex) {
        Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
        List<NDArray> bulkReduceMessages = new ArrayList<>();
        for (Edge edge : inEdges) {
            if (this.messageReady(edge)) {
                NDArray msg = this.message(((VTensor) edge.src.getFeature("feature")).getValue(), false);
                bulkReduceMessages.add(msg);
            }
        }
        if (bulkReduceMessages.size() > 0) {
            NDArray msgs = MeanAggregator.bulkReduce(bulkReduceMessages.toArray(NDArray[]::new));
            new RemoteInvoke()
                    .toElement(vertex.decodeFeatureId("agg"), ElementType.FEATURE)
                    .where(IterationType.ITERATE)
                    .method("reduce")
                    .hasUpdate()
                    .toDestination(vertex.masterPart())
                    .withArgs(MODEL_VERSION, msgs, bulkReduceMessages.size())
                    .buildAndRun(storage);
        }
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to taskLifeCycleManager
     *
     * @param feature
     * @param agg
     * @param training
     * @return
     */
    public NDArray update(NDArray feature, NDArray agg, boolean training) {
        NDManager oldFeatureManager = feature.getManager();
        NDManager oldAggManager = agg.getManager();
        NDManager tmpManager = storage.manager.getTempManager().newSubManager();
        feature.attach(tmpManager);
        agg.attach(tmpManager);
        NDArray res = this.updateModel.getBlock().forward(this.parameterStore, new NDList(feature, agg), training).get(0);
        res.attach(storage.manager.getTempManager());
        feature.attach(oldFeatureManager);
        agg.attach(oldAggManager);
        tmpManager.close();
        return res;
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     *
     * @param feature
     * @param training
     * @return
     */
    public NDArray message(NDArray feature, boolean training) {
        NDManager oldManager = feature.getManager();
        NDManager tmpManager = storage.manager.getTempManager().newSubManager();
        feature.attach(tmpManager);
        NDArray res = this.messageModel.getBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        res.attach(storage.manager.getTempManager());
        feature.attach(oldManager);
        tmpManager.close();
        return res;
    }

    /**
     * Is the Edge ready to pass on the message
     *
     * @param edge
     * @return
     */
    public boolean messageReady(Edge edge) {
        return !updatePending && Objects.nonNull(edge.src.getFeature("feature")) && ((VTensor) edge.src.getFeature("feature")).isReady(MODEL_VERSION);
    }

    /**
     * Is the Vertex ready to be updated
     *
     * @param vertex
     * @return
     */
    public boolean updateReady(Vertex vertex) {
        return !updatePending && vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && ((VTensor) vertex.getFeature("feature")).isReady(MODEL_VERSION) && Objects.nonNull(vertex.getFeature("agg")) && ((BaseAggregator) vertex.getFeature("agg")).isReady(MODEL_VERSION);
    }
}
