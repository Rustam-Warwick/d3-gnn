package plugins.vertex_classification;

import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.PtNDArray;
import ai.djl.pytorch.jni.JniUtils;
import ai.djl.translate.StackBatchifier;
import elements.*;
import elements.iterations.MessageDirection;
import elements.iterations.RemoteInvoke;
import elements.iterations.Rmi;
import operators.BaseWrapperOperator;
import operators.coordinators.events.StartTraining;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Simply stores and initializes the model, does not do any continuous inference
 */
public class VertexTrainingLayer extends Plugin {
    public transient VertexOutputLayer outputLayer;

    public int BATCH_COUNT = 0;

    public final int MAX_BATCH_COUNT;

    public int ACTUAL_BATCH_COUNT;

    public final SerializableLoss loss;


    public VertexTrainingLayer(SerializableLoss loss, int MAX_BATCH_COUNT) {
        super("trainer");
        this.MAX_BATCH_COUNT = MAX_BATCH_COUNT;
        this.loss = loss;
    }

    public VertexTrainingLayer(SerializableLoss loss) {
       this(loss,1024);
    }

    public void incrementBatchCount(){
        BATCH_COUNT++;
        if(BATCH_COUNT % ACTUAL_BATCH_COUNT == 0){
            storage.layerFunction.operatorEventMessage(new StartTraining());
        }
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.elementType() == ElementType.FEATURE){
            Feature<?,?> feature = (Feature<?, ?>) element;
            if(feature.attachedTo.f0==ElementType.VERTEX && "trainLabel".equals(feature.getName()) && feature.getElement() != null){
                if(isTrainReady((Vertex) feature.getElement()))incrementBatchCount();
            }
            if(feature.attachedTo.f0 == ElementType.VERTEX && "feature".equals(feature.getName()) && feature.getElement()!=null){
                if(isTrainReady((Vertex) feature.getElement()))incrementBatchCount();
            }
        }
    }

    public boolean isTrainReady(Vertex v){
        return v.getFeature("trainLabel") != null && v.getFeature("feature")!=null;
    }

    public void startTraining(){
        List<NDList> inputs = new ArrayList<>();
        List<NDList> labels = new ArrayList<>();
        List<String> vertices = new ArrayList<>();
        StackBatchifier b = new StackBatchifier();
        for(Vertex v: storage.getVertices()){
            if(isTrainReady(v)){
                vertices.add(v.getId());
                inputs.add(new NDList((NDArray) v.getFeature("feature").getValue()));
                labels.add(new NDList((NDArray) v.getFeature("trainLabel").getValue()));
            }
        }
        inputs.forEach(item->item.get(0).setRequiresGradient(true));
        NDList batchedInputs = b.batchify(inputs.toArray(NDList[]::new));
        NDList batchedLabels = b.batchify(labels.toArray(NDList[]::new));
        NDList predictions = outputLayer.output(batchedInputs, true);
        NDArray meanLoss = loss.evaluate(batchedLabels, predictions);
        JniUtils.backward((PtNDArray) meanLoss, (PtNDArray)BaseNDManager.threadNDManager.get().ones(new Shape()), false, false);

        HashMap<String, NDArray> gradients  = new HashMap<>();
        for(int i=0; i < vertices.size(); i++){
            gradients.put(vertices.get(i), inputs.get(i).get(0).getGradient());
        }

        new RemoteInvoke()
                .addDestination(getPartId())
                .noUpdate()
                .method("collect")
                .toElement(getId(), elementType())
                .where(MessageDirection.BACKWARD)
                .withArgs(gradients)
                .buildAndRun(storage);

        if(replicaParts().isEmpty() || getPartId()==replicaParts().get(replicaParts().size() - 1)){
            Rmi synchronize = new Rmi(getId(), "synchronize", new Object[]{},elementType(), false, null);
            storage.layerFunction.sideBroadcastMessage(new GraphOp(Op.RMI, synchronize), BaseWrapperOperator.BACKWARD_OUTPUT_TAG);
        }


    }


    @Override
    public void onOperatorEvent(OperatorEvent event) {
        super.onOperatorEvent(event);
        if(event instanceof StartTraining){
            startTraining();
        }
    }

    @Override
    public void open() {
        super.open();
        outputLayer = (VertexOutputLayer) storage.getPlugin("inferencer");
        ACTUAL_BATCH_COUNT = MAX_BATCH_COUNT / storage.layerFunction.getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void close() {
        super.close();
    }


}
