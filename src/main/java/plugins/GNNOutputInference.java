package plugins;

import ai.djl.Model;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import elements.*;
import features.VTensor;
import helpers.MyParameterStore;
import helpers.JavaTensor;
import iterations.IterationState;
import iterations.RemoteFunction;
import scala.Tuple2;

import java.util.Objects;

public abstract class GNNOutputInference extends Plugin {
    public Model outputModel;
    public MyParameterStore parameterStore;

    public GNNOutputInference(){super("inferencer");}
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
                vertex.setFeature("feature", new VTensor(embedding));
            }else{
                vertex.getFeature("feature").externalUpdate(new VTensor(embedding));
            }
        }
    }

    @Override
    public void open() {
        super.open();
        this.parameterStore = new MyParameterStore(this.storage.manager.getLifeCycleManager());
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
                        this.makePredictionAndSendForward((VTensor) feature);
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
                        this.makePredictionAndSendForward((VTensor) feature);
                    }
                }
            }
        }
    }

    public void makePredictionAndSendForward(VTensor embedding){
        NDManager oldManager = embedding.getValue().getManager();
        embedding.getValue().attach(this.storage.manager.getTempManager());
        NDArray res = this.outputModel.getBlock().forward(this.parameterStore, new NDList(embedding.getValue()), false).get(0);
        embedding.getValue().attach(oldManager);
        Vertex a = new Vertex(embedding.attachedTo._2);
        a.setFeature("logits", new VTensor(new Tuple2<>(res, 0)));
        this.storage.message(new GraphOp(Op.COMMIT, this.storage.currentKey, a, IterationState.FORWARD));

    }


}
