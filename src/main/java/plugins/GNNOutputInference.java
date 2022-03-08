package plugins;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphElement;
import elements.Plugin;
import elements.Vertex;
import features.Tensor;
import helpers.MyParameterStore;
import iterations.RemoteFunction;
import scala.Tuple2;
import storage.BaseStorage;

import java.util.Objects;

public abstract class GNNOutputInference extends Plugin {
    public Model outputModel;
    public MyParameterStore parameterStore;

    public GNNOutputInference(){super("inference");}
    public GNNOutputInference(String id){super(id);}

    public abstract Model createOutputModel();

    @RemoteFunction
    public void forward(String elementId, Tuple2<NDArray, Integer> embedding){
        if(embedding._2 >= this.parameterStore.MODEL_VERSION){
            Vertex vertex = this.storage.getVertex(elementId);
            if(Objects.isNull(vertex)){
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
        this.parameterStore = new MyParameterStore(BaseStorage.tensorManager, false);
        this.outputModel = this.createOutputModel();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        switch (element.elementType()){
            case FEATURE:{
                Feature feature = (Feature) element;
                switch (feature.getFieldName()){
                    case "feature":{
                        NDArray msg = this.getPrediction((Tensor) feature);
                        if(Objects.nonNull(msg)){
                            // DO SMT
                        }
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
                        NDArray msg = this.getPrediction((Tensor) feature);
                        if(Objects.nonNull(msg)){
                            // Do smt
                        }
                    }
                }
            }
        }
    }

    public NDArray getPrediction(Tensor embedding){
        NDList message = this.outputModel.getBlock().forward(this.parameterStore, new NDList(embedding.getValue()), false);
        return message.get(0);
    }
}
