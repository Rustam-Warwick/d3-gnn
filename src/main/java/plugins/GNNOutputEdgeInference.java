package plugins;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.Vertex;
import features.VTensor;
import helpers.MyParameterStore;
import iterations.RemoteFunction;
import scala.Tuple2;

import java.util.Objects;

public abstract class GNNOutputEdgeInference extends Plugin {
    public transient Model outputModel;
    public MyParameterStore parameterStore = new MyParameterStore();
    public int MODEL_VERSION = 0;
    public GNNOutputEdgeInference(){super("inferencer");}

    public abstract Model createOutputModel();

    @RemoteFunction
    public void forward(String elementId, Tuple2<NDArray, Integer> embedding){
        if(embedding._2 >= this.MODEL_VERSION){
            Vertex vertex = this.storage.getVertex(elementId);
            if(Objects.isNull(vertex)){
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
    public void add() {
        super.add();
        this.storage.withPlugin(new GNNOutputTraining());
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.loadModel(this.outputModel);
    }

    @Override
    public void open() {
        super.open();
        this.outputModel = this.createOutputModel();
        this.parameterStore.canonizeModel(this.outputModel);
        this.parameterStore.restoreModel(this.outputModel);
        this.parameterStore.setNDManager(this.storage.manager.getLifeCycleManager());
    }

    @Override
    public void close() {
        super.close();
        this.outputModel.close();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()){
            case FEATURE:{
                Feature feature = (Feature) element;
                switch (feature.getFieldName()){
                    case "feature":{
//                        this.makePredictionAndSendForward((VTensor) feature);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        switch (newElement.elementType()){
            case FEATURE:{
                Feature feature = (Feature) newElement;
                switch (feature.getFieldName()){
                    case "feature":{
//                        this.makePredictionAndSendForward((VTensor) feature);
                    }
                }
            }
        }
    }

    public NDArray output(NDArray feature, NDArray feature2, boolean training){
        NDManager oldManager = feature.getManager();
        NDManager oldManager2 = feature2.getManager();
        feature.attach(this.storage.manager.getTempManager());
        feature2.attach(this.storage.manager.getTempManager());
        NDArray res =  this.outputModel.getBlock().forward(this.parameterStore, new NDList(feature, feature2), training).get(0);
        feature.attach(oldManager);
        feature2.attach(oldManager2);
        return res;
    }



}
