package plugins;

import aggregators.MeanAggregator;
import ai.djl.Device;
import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.training.LocalParameterServer;
import ai.djl.training.ParameterStore;
import ai.djl.translate.NoopTranslator;
import elements.*;
import features.Tensor;
import helpers.MyParameterServer;
import helpers.MyParameterStore;
import iterations.IterationState;
import iterations.RemoteDestination;
import iterations.RemoteFunction;
import iterations.Rpc;
import storage.BaseStorage;

import java.util.Objects;

public abstract class GNNLayerInference extends Plugin {
    public Model messageModel;
    public Model updateModel;
    public MyParameterStore parameterStore;
    public GNNLayerInference(String id) {
        super(id);
    }
    public GNNLayerInference(){
        super();
    }

    public abstract Model createMessageModel();
    public abstract Model createUpdateModel();

    @RemoteFunction
    public void forward(String elementId, NDArray embedding){

    }

    @Override
    public void open() {
        super.open();
        this.parameterStore = new MyParameterStore(BaseStorage.tensorManager, false);
        this.messageModel = this.createMessageModel();
        this.updateModel = this.createUpdateModel();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()){
            case VERTEX:{
                if(element.state() == ReplicaState.MASTER) {
                    element.setFeature("agg", new MeanAggregator(BaseStorage.tensorManager.zeros(new Shape(32)), true));
                    if(this.storage.isFirst() && Objects.isNull(element.getFeature("feature"))){
                        element.setFeature("feature", new Tensor(BaseStorage.tensorManager.zeros(new Shape(7))));
                    }
                }
                break;
            }
            case EDGE:{
                Edge edge = (Edge) element;
                NDArray message = this.getMessage(edge);
                if(Objects.nonNull(message)){
                    Rpc.call(edge.dest.getFeature("agg"), "reduce", message, 1);
                }
                break;
            }
            case FEATURE:{
                Feature feature = (Feature) element;
                switch (feature.getFieldName()){
                    case "feature":{
                        Vertex parent = (Vertex) feature.getElement();
                        NDArray update = this.getUpdate(parent);
                        if(Objects.nonNull(update)){
                            Rpc.callProcedure(this,"forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), update);
                        }
                        this.reduceOutEdges(parent);
                        break;
                    }
                    case "agg":{
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
        switch (newElement.elementType()){
            case FEATURE:{
                Feature feature = (Feature) newElement;
                Feature oldFeature = (Feature) oldElement;
                switch (feature.getFieldName()){
                    case "feature":{
                        Vertex parent = (Vertex) feature.getElement();
                        NDArray update = this.getUpdate(parent);
                        if(Objects.nonNull(update)){
                            Rpc.callProcedure(this,"forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), update);
                        }
                        this.updateOutEdges(parent, (NDArray) oldFeature.getValue());
                        break;
                    }

                    case "agg":{
                        Vertex parent = (Vertex) feature.getElement();
                        NDArray update = this.getUpdate(parent);
                        if(Objects.nonNull(update)){
                            Rpc.callProcedure(this,"forward", IterationState.FORWARD, RemoteDestination.SELF, parent.getId(), update);
                        }
                        break;
                    }
                }
            }
        }
    }

    public void updateOutEdges(Vertex vertex, NDArray oldFeature){

    }

    public void reduceOutEdges(Vertex vertex){

    }
    public boolean tensorReady(Tensor e){
        return true;
    }

    public void reduceInEdges(Vertex vertex){

    }

    public NDArray getMessage(Edge edge){
        if(Objects.nonNull(edge.dest.getFeature("agg")) && Objects.nonNull(edge.src.getFeature("feature")) && this.tensorReady((Tensor) edge.src.getFeature("feature"))){
            NDList message = this.messageModel.getBlock().forward(this.parameterStore, new NDList((NDArray) edge.src.getFeature("feature").getValue()), false);
            return message.get(0);
        }
        return null;
    }

    public NDArray getUpdate(Vertex vertex){
        if(vertex.state() == ReplicaState.MASTER && Objects.nonNull(vertex.getFeature("feature")) && Objects.nonNull(vertex.getFeature("agg"))){
            NDList inference = this.updateModel.getBlock().forward(this.parameterStore, new NDList((NDArray) vertex.getFeature("feature").getValue(),(NDArray) vertex.getFeature("agg").getValue()), false);
            return inference.get(0);
        }

        return null;
    }
}
