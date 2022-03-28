package plugins;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.*;
import features.VTensor;
import helpers.JavaTensor;
import helpers.MyParameterStore;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple2;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class GNNLayerInference extends Plugin {
    public transient Model messageModel;
    public transient Model updateModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public boolean directedEdges = true;
    public int MODEL_VERSION = 0;

    public GNNLayerInference() {
        super("inferencer");
    }
    public GNNLayerInference(boolean directedEdges){
        this();
        this.directedEdges = directedEdges;
    }
    public abstract Model createMessageModel();

    public abstract Model createUpdateModel();

    @RemoteFunction
    public void forward(String elementId, Tuple2<NDArray, Integer> embedding) {
        if(embedding._2 >= this.MODEL_VERSION){
            Vertex vertex = this.storage.getVertex(elementId);
            if(Objects.isNull(vertex)){
                System.out.println("How come this is null");
                vertex = new Vertex(elementId, false, this.storage.currentKey);
                vertex.setStorage(this.storage);
                if (!vertex.createElement()) throw new AssertionError("Cannot create element in forward function");
            }
            if(Objects.isNull(vertex.getFeature("feature"))){
                vertex.setFeature("feature", new VTensor(embedding));
            }else{
                vertex.getFeature("feature").externalUpdate(new VTensor(embedding));
            }
        }

    }

    @Override
    public void setStorage(BaseStorage storage) {
        super.setStorage(storage);
    }

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
                this.initVertex((Vertex) element);
                break;
            }
            case EDGE: {
                Edge edge = (Edge) element;
                if(this.messageReady(edge)){
                    NDArray feature = ((VTensor) edge.src.getFeature("feature")).getValue();
                    NDArray msg = this.message(feature, false);
                    Rpc.call(edge.dest.getFeature("agg"), "reduce", MODEL_VERSION, storage.currentKey, msg, 1);
                }
                break;
            }
            case FEATURE: {
                Feature feature = (Feature) element;
                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray ft = ((VTensor)parent.getFeature("feature")).getValue();
                            NDArray agg = ((BaseAggregator<?>)parent.getFeature("agg")).getValue();
                            NDArray update = this.update(ft, agg, false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(update, this.MODEL_VERSION));
                        }
                        this.reduceOutEdges((VTensor) feature);
                        break;
                    }
                    case "agg": {
                        Vertex parent = (Vertex) feature.getElement();
                        this.reduceInEdges(parent);
                        break;
                    }
                }
                break;
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        switch(newElement.elementType()) {
            case FEATURE: {
                Feature feature = (Feature) newElement;
                Feature oldFeature = (Feature) oldElement;
                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray ft = ((VTensor)parent.getFeature("feature")).getValue();
                            NDArray agg = ((BaseAggregator<?>)parent.getFeature("agg")).getValue();
                            NDArray update = this.update(ft, agg, false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(update, this.MODEL_VERSION));
                        }

                        this.updateOutEdges((VTensor) feature, (VTensor) oldFeature);
                        break;
                    }

                    case "agg": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray ft = ((VTensor)parent.getFeature("feature")).getValue();
                            NDArray agg = ((BaseAggregator<?>)parent.getFeature("agg")).getValue();
                            NDArray update = this.update(ft, agg, false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(update, this.MODEL_VERSION));
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * Given newly created vertex init the aggregator and other values of it
     * @param element
     */
    public void initVertex(Vertex element){
        if (element.state() == ReplicaState.MASTER) {
            NDArray aggStart = this.storage.manager.getLifeCycleManager().zeros(this.messageModel.describeOutput().get(0).getValue());
            element.setFeature("agg", new MeanAggregator(new JavaTensor(aggStart), true));

            if (this.storage.isFirst() && Objects.isNull(element.getFeature("feature"))) {
                NDArray embeddingRandom = this.storage.manager.getLifeCycleManager().randomNormal(this.messageModel.describeInput().get(0).getValue());
                element.setFeature("feature", new VTensor(new Tuple2<>(new JavaTensor(embeddingRandom), this.MODEL_VERSION)));
            }
        }
    }

    /**
     * Given oldFeature value and new Feature value update the Out Edged aggregators
     * @param newFeature
     * @param oldFeature
     */
    public void updateOutEdges(VTensor newFeature, VTensor oldFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        for(Edge edge:outEdges){
            if(this.messageReady(edge)){
                if(Objects.isNull(msgOld)){
                    msgOld = this.message(oldFeature.getValue(), false);
                    msgNew = this.message(newFeature.getValue(), false);
                }
                Rpc.call(edge.dest.getFeature("agg"), "replace", MODEL_VERSION, storage.currentKey, msgNew, msgOld);
            }
        }
    }

    /**
     * Given vertex reduce all the out edges aggregator values
     */
    public void reduceOutEdges(VTensor newFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges((Vertex) newFeature.getElement(), EdgeType.OUT);
        NDArray msg = null;
        for(Edge edge: outEdges){
            if(this.messageReady(edge)){
                if(Objects.isNull(msg)){
                    msg = this.message(newFeature.getValue(), false);
                }
                Rpc.call(edge.dest.getFeature("agg"), "reduce",MODEL_VERSION, storage.currentKey, msg, 1);
            }
        }
    }

    /**
     * Given Vertex reduce all the in-edges in bulk
     * @param vertex
     */
    public void reduceInEdges(Vertex vertex) {
        Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
        List<NDArray> bulkReduceMessages = new ArrayList<>();
        for(Edge edge: inEdges){
            if(this.messageReady(edge)){
                NDArray msg = this.message(((VTensor)edge.src.getFeature("feature")).getValue(), false);
                bulkReduceMessages.add(msg);
            }
        }
        if(bulkReduceMessages.size() > 0){
            ((BaseAggregator)vertex.getFeature("agg")).bulkReduce(MODEL_VERSION, storage.currentKey, bulkReduceMessages.toArray(NDArray[]::new));
        }
    }

    /**
     * Calling the update function, note that everything except the input feature and agg value is transfered to taskLifeCycleManager
     * @param feature
     * @param agg
     * @param training
     * @return
     */
    public NDArray update(NDArray feature, NDArray agg, boolean training){
        NDManager oldFeatureManager = feature.getManager();
        NDManager oldAggManager = agg.getManager();
        feature.attach(this.storage.manager.getTempManager());
        agg.attach(this.storage.manager.getTempManager());
        NDArray res = this.updateModel.getBlock().forward(this.parameterStore, new NDList(feature, agg), training).get(0);
        feature.attach(oldFeatureManager);
        agg.attach(oldAggManager);
        return res;
    }

    /**
     * Calling the message function, note that everything except the input is transfered to tasklifeCycleManager
     * @param feature
     * @param training
     * @return
     */
    public NDArray message(NDArray feature, boolean training){
        NDManager oldManager = feature.getManager();
        feature.attach(this.storage.manager.getTempManager());
        NDArray res = this.messageModel.getBlock().forward(this.parameterStore, new NDList(feature), training).get(0);
        feature.attach(oldManager);
        return res;
    }

    /**
     * Is the Edge ready to pass on the message
     * @param edge
     * @return
     */
    public boolean messageReady(Edge edge){
        return Objects.nonNull(edge.dest.getFeature("agg")) && Objects.nonNull(edge.src.getFeature("feature")) && ((VTensor) edge.src.getFeature("feature")).isReady(MODEL_VERSION);
    }

    /**
     * Is the Vertex ready to be updated
     * @param vertex
     * @return
     */
    public boolean updateReady(Vertex vertex){
        return vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && ((VTensor) vertex.getFeature("feature")).isReady(MODEL_VERSION) && Objects.nonNull(vertex.getFeature("agg")) && ((BaseAggregator) vertex.getFeature("agg")).isReady(MODEL_VERSION);
    }
}
