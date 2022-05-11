package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.*;
import functions.nn.LossWrapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * Reports the model accuracy with new data
 */
public class VertexClassificationTester extends Plugin {
    public static OutputTag<Float> TEST_LOSS_OUTPUT = new OutputTag<>("testingLoss", TypeInformation.of(Float.class));
    public transient VertexClassificationLayer inferencer;
    public LossWrapper lossFunction;

    public VertexClassificationTester(LossWrapper lossFunction) {
        super("tester");
        this.lossFunction = lossFunction;
    }

    @Override
    public void open() {
        super.open();
        inferencer = (VertexClassificationLayer) storage.getPlugin("inferencer");
    }

    public void doTest(Vertex v){
        NDArray prediction = inferencer.output((NDArray) v.getFeature("feature").getValue(), false).expandDims(0);
        NDArray label = storage.manager.getTempManager().create((Integer) v.getFeature("testLabel").getValue()).expandDims(0);
        NDArray loss = lossFunction.evaluate(new NDList(label), new NDList(prediction));
        storage.layerFunction.sideMessage(loss.toFloatArray()[0], TEST_LOSS_OUTPUT); // Side Output this
        v.getFeature("testLabel").deleteElement();
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if(element.elementType() == ElementType.FEATURE){
            Feature<?,?> feature = (Feature<?, ?>) element;
            if("feature".equals(feature.getName()) || "testLabel".equals(feature.getName())) {
                if(feature.getElement()!=null && testReady((Vertex) feature.getElement()))doTest((Vertex) feature.getElement());
            }

        }
    }


    public boolean testReady(Vertex element){
        return inferencer.outputReady(element) && element.getFeature("testLabel")!=null;
    }
}
