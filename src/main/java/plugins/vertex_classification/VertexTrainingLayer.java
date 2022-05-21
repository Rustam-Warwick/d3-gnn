package plugins.vertex_classification;

import elements.*;
import operators.coordinators.events.StartTraining;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexTrainingLayer extends Plugin {
    public transient VertexOutputLayer outputLayer;
    public int BATCH_COUNT = 0;
    public final int MAX_BATCH_COUNT;
    public int ACTUAL_BATCH_COUNT;

    public VertexTrainingLayer(int MAX_BATCH_COUNT) {
        super("trainer");
        this.MAX_BATCH_COUNT = MAX_BATCH_COUNT;
    }

    public VertexTrainingLayer() {
       this(100);
    }

    public void incrementBatchCount(){
        BATCH_COUNT++;
        System.out.println(BATCH_COUNT);
        if(BATCH_COUNT % ACTUAL_BATCH_COUNT == 0){
            storage.layerFunction.sendOperatorEvent(new StartTraining());
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.elementType() == ElementType.FEATURE){
            Feature<?,?> feature = (Feature<?, ?>) element;
            if(feature.attachedTo.f0==ElementType.VERTEX && "trainLabel".equals(feature.getName()) && feature.getElement() != null){
                if(trainReady((Vertex) feature.getElement()))incrementBatchCount();
            }
            if(feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName()) && feature.getElement()!=null){
                if(trainReady((Vertex) feature.getElement()))incrementBatchCount();
            }
        }
    }

    public boolean trainReady(Vertex v){
        return v.getFeature("trainLabel") != null && v.getFeature("feature")!=null;
    }

    @Override
    public void open() {
        super.add();
        outputLayer = (VertexOutputLayer) storage.getPlugin("inferencer");
        ACTUAL_BATCH_COUNT = MAX_BATCH_COUNT / storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void close() {
        super.close();
    }


}
