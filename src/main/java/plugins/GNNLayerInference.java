package plugins;

import aggregators.BaseAggregator;
import aggregators.MeanAggregator;
import aggregators.SumAggregator;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import features.Tensor;
import helpers.MyParameterStore;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import partitioner.HDRF;
import scala.Tuple2;
import storage.BaseStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class GNNLayerInference extends Plugin {
    public Model messageModel;
    public Model updateModel;
    public MyParameterStore parameterStore;

    public GNNLayerInference(String id) {
        super(id);
    }

    public GNNLayerInference() {
        super("inference");
    }

    public abstract Model createMessageModel();

    public abstract Model createUpdateModel();

    @RemoteFunction
    public void forward(String elementId, Tuple2<NDArray, Integer> embedding) {
        if(embedding._2 >= this.parameterStore.MODEL_VERSION){
            Vertex vertex = this.storage.getVertex(elementId);
            if(Objects.isNull(vertex)){
//                System.out.println("How come this is null");
                vertex = new Vertex(elementId, false, this.storage.currentKey);
                vertex.setStorage(this.storage);
                if (!vertex.createElement()) throw new AssertionError("Cannot create element in forward function");
            }
            if(Objects.isNull(vertex.getFeature("feature"))){
                vertex.setFeature("feature", new Tensor(embedding));
            }else{
                vertex.getFeature("feature").externalUpdate(new Tensor(embedding));
            }
        }

    }

    @Override
    public void open() {
        super.open();
        this.parameterStore = new MyParameterStore(this.storage.tensorManager, false);
        this.messageModel = this.createMessageModel();
        this.updateModel = this.createUpdateModel();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()) {
            case VERTEX: {
                if (element.state() == ReplicaState.MASTER) {
                    element.setFeature("agg", new SumAggregator(this.storage.tensorManager.zeros(this.messageModel.describeOutput().get(0).getValue()), true));
                    if (this.storage.isFirst() && Objects.isNull(element.getFeature("feature"))) {
                        NDArray embeddingRandom = this.storage.tensorManager.randomNormal(this.messageModel.describeInput().get(0).getValue());
                        element.setFeature("feature", new Tensor(new Tuple2<>(embeddingRandom, this.parameterStore.MODEL_VERSION)));
                    }
                }
                break;
            }
            case EDGE: {
                Edge edge = (Edge) element;
                NDArray message = this.getMessage(edge);
                if (Objects.nonNull(message)) {
                    Rpc.call(edge.dest.getFeature("agg"), "reduce", message, 1);
                }
                break;
            }
            case FEATURE: {
                Feature feature = (Feature) element;

                switch (feature.getFieldName()) {
                    case "feature": {
                        Vertex parent = (Vertex) feature.getElement();
                        NDArray update = this.getUpdate(parent);
                        if (Objects.nonNull(update)) {
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(update, this.parameterStore.MODEL_VERSION));
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
                        if(this.storage.isLast() && feature.state() == ReplicaState.REPLICA && List.of("114","10798", "8703", "6213", "4649").contains(feature.attachedTo._2)){
                            System.out.println("Feature Update");
                        }
                        this.updateOutEdges(parent,null);
                        NDArray update = this.getUpdate(parent);
                        if(Objects.nonNull(update)) {
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), new Tuple2<>(update, this.parameterStore.MODEL_VERSION));
                        }

                        break;
                    }

                    case "agg": {
                        Vertex parent = (Vertex) feature.getElement();
                        NDArray update = this.getUpdate(parent);
                        if (Objects.nonNull(update)) {
                            Rpc.callProcedure(this, "forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(),new Tuple2<>(update, this.parameterStore.MODEL_VERSION));
                        }

                        break;
                    }
                }
            }
        }
    }

    public void updateOutEdges(Vertex vertex, NDArray oldFeature) {
        Stream<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
//        NDArray msgOld = this.messageModel.getBlock().forward(this.parameterStore, new NDList(oldFeature), false).get(0);
        outEdges.forEach(edge->{
            NDArray msgNew = this.getMessage(edge);
            if(Objects.nonNull(msgNew)){
                Rpc.call(edge.dest.getFeature("agg"), "replace",msgNew, msgNew);
            }
        });
    }

    public void reduceOutEdges(Vertex vertex) {
        Stream<Edge> outEdges = this.storage.getIncidentEdges(vertex, EdgeType.OUT);
        outEdges.forEach(edge->{
            if(this.storage.isLast() && edge.dest.getId().equals("434")){
                System.out.println("");
            }
            NDArray msgNew = this.getMessage(edge);
            if(Objects.nonNull(msgNew)){
                Rpc.call(edge.dest.getFeature("agg"), "reduce",msgNew, 1);
            }
        });
    }

    public void reduceInEdges(Vertex vertex) {
        Stream<Edge> inEdges = this.storage.getIncidentEdges(vertex, EdgeType.IN);
        List<NDArray> bulkReduceMessages = new ArrayList<>();
        inEdges.forEach(edge->{
            NDArray msgNew = this.getMessage(edge);
            if(Objects.nonNull(msgNew)){
                bulkReduceMessages.add(msgNew);
            }
        });
        ((BaseAggregator)vertex.getFeature("agg")).bulkReduce(bulkReduceMessages.toArray(NDArray[]::new));
    }

    public NDArray getMessage(Edge edge) {
        if (Objects.nonNull(edge.dest.getFeature("agg")) && Objects.nonNull(edge.src.getFeature("feature")) && ((Tensor) edge.src.getFeature("feature")).isReady(0)) {
            NDList message = this.messageModel.getBlock().forward(this.parameterStore, new NDList((NDArray) edge.src.getFeature("feature").getValue()), false);
            return message.get(0);
        }
        return null;
    }

    public NDArray getUpdate(Vertex vertex) {
        if (vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && ((Tensor) vertex.getFeature("feature")).isReady(0) && Objects.nonNull(vertex.getFeature("agg")) && ((BaseAggregator) vertex.getFeature("agg")).isReady(0)) {
            NDList inference = this.updateModel.getBlock().forward(this.parameterStore, new NDList((NDArray) vertex.getFeature("feature").getValue(), (NDArray) vertex.getFeature("agg").getValue()), false);
            return inference.get(0);
        }
        return null;
    }
}
