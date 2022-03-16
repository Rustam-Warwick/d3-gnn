package plugins;

import aggregators.BaseAggregator;
import aggregators.SumAggregator;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import helpers.NDTensor;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class GNNLayerInference extends Plugin {
    public Model messageModel;
    public Model updateModel;
    public MyParameterStore parameterStore;

    public GNNLayerInference() {
        super("inferencer");
    }

    public abstract Model createMessageModel();

    public abstract Model createUpdateModel();


    @RemoteFunction
    public void forward(String elementId, Tuple2<NDTensor, Integer> embedding) {
        if(embedding._2 >= this.parameterStore.MODEL_VERSION){
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
    public void open() {
        super.open();
        this.parameterStore = new MyParameterStore(this.storage.manager.getGlobalManager());
        this.messageModel = this.createMessageModel();
        this.updateModel = this.createUpdateModel();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()) {
            case VERTEX: {
                if (element.state() == ReplicaState.MASTER) {
                    element.setFeature("agg", new SumAggregator(this.storage.manager.getTempManager().zeros(this.messageModel.describeOutput().get(0).getValue()), true));
                    if (this.storage.isFirst() && Objects.isNull(element.getFeature("feature"))) {
                        NDArray embeddingRandom = this.storage.manager.getTempManager().randomNormal(this.messageModel.describeInput().get(0).getValue());
                        element.setFeature("feature", new VTensor(embeddingRandom, this.parameterStore.MODEL_VERSION));
                    }
                }
                break;
            }
            case EDGE: {
                Edge edge = (Edge) element;
                if(this.messageReady(edge)){
                    NDArray msg = this.message((VTensor) edge.src.getFeature("feature"), false);
                    Rpc.call(edge.dest.getFeature("agg"), "reduce", new NDTensor(msg), 1);
                }
                break;
            }
            case FEATURE: {
                Feature feature = (Feature) element;
                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray update = this.update((VTensor) parent.getFeature("feature"), (BaseAggregator<?>) parent.getFeature("agg"), false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(new NDTensor(update), this.parameterStore.MODEL_VERSION));
                        }
                        this.reduceOutEdges(parent);
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
        switch (newElement.elementType()) {
            case FEATURE: {
                Feature feature = (Feature) newElement;
                Feature oldFeature = (Feature) oldElement;
                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray update = this.update((VTensor) parent.getFeature("feature"), (BaseAggregator<?>) parent.getFeature("agg"), false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(new NDTensor(update), this.parameterStore.MODEL_VERSION));
                        }

                        this.updateOutEdges(parent, (VTensor) oldFeature);
                        break;
                    }

                    case "agg": {
                        Vertex parent = (Vertex) feature.getElement();
                        if(this.updateReady(parent)){
                            NDArray update = this.update((VTensor) parent.getFeature("feature"), (BaseAggregator<?>) parent.getFeature("agg"), false);
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(new NDTensor(update), this.parameterStore.MODEL_VERSION));
                        }
                        break;
                    }
                }
            }
        }
    }

    public void updateOutEdges(Vertex vertex, VTensor oldFeature) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        NDArray msgOld = null;
        NDArray msgNew = null;
        oldFeature.setStorage(this.storage);
        for(Edge edge:outEdges){
            if(this.messageReady(edge)){
                if(Objects.isNull(msgOld)){
                    msgOld = this.message(oldFeature, false);
                    msgNew = this.message((VTensor) vertex.getFeature("feature"), false);
                }
                Rpc.call(edge.dest.getFeature("agg"), "replace", new NDTensor(msgNew), new NDTensor(msgOld));
            }
        }
    }

    public void reduceOutEdges(Vertex vertex) {
        Iterable<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        NDArray msg = null;
        for(Edge edge: outEdges){
            if(this.messageReady(edge)){
                if(Objects.isNull(msg)){
                    msg = this.message((VTensor) vertex.getFeature("feature"), false);
                }
                Rpc.call(edge.dest.getFeature("agg"), "reduce",new NDTensor(msg), 1);
            }
        }
    }

    public void reduceInEdges(Vertex vertex) {
        Iterable<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
        List<NDArray> bulkReduceMessages = new ArrayList<>();
        for(Edge edge: inEdges){
            if(this.messageReady(edge)){
                NDArray msg = this.message((VTensor) edge.src.getFeature("feature"), false);
                bulkReduceMessages.add(msg);
            }
        }

        ((BaseAggregator)vertex.getFeature("agg")).bulkReduce(bulkReduceMessages.toArray(NDArray[]::new));
    }


    public NDArray message(VTensor source, boolean training){
        NDArray msg = this.messageModel.getBlock().forward(this.parameterStore, new NDList(source.getValue()), training).get(0);
        msg.attach(this.storage.manager.getTempManager());
        return msg;
    }

    public NDArray update(VTensor dest, BaseAggregator<?> agg,  boolean training){
        NDArray update = this.updateModel.getBlock().forward(this.parameterStore, new NDList(dest.getValue(), agg.getValue()), training).get(0);
        update.attach(this.storage.manager.getTempManager());
        return update;
    }

    public boolean messageReady(Edge edge){
        return Objects.nonNull(edge.dest.getFeature("agg")) && Objects.nonNull(edge.src.getFeature("feature")) && ((VTensor) edge.src.getFeature("feature")).isReady(0);
    }

    public boolean updateReady(Vertex vertex){
        return vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && ((VTensor) vertex.getFeature("feature")).isReady(0) && Objects.nonNull(vertex.getFeature("agg")) && ((BaseAggregator) vertex.getFeature("agg")).isReady(0);
    }
}
