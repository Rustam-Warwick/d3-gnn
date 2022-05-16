package plugins.vertex_classification;

import aggregators.MeanAggregator;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import ai.djl.pytorch.engine.PtNDArray;
import elements.*;
import functions.nn.MyParameterStore;
import org.apache.flink.metrics.Gauge;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Objects;


/**
 * Reports the model accuracy with new data
 * Assumes to be in the last layer
 * Expects vertexOutputLayer
 * Feature -> last last vertex embedding
 * testLabel -> Label to test by
 */
public class VertexLossReporter extends Plugin {
    public transient VertexOutputLayer inference;
    public transient MeanAggregator lossAggregator; // Aggregate the loss function
    public SerializableLoss lossFunction;

    public VertexLossReporter(SerializableLoss lossFunction) {
        super("tester");
        this.lossFunction = lossFunction;
    }

    public NDArray calculateLoss(NDArray prediction, NDArray label) {
        NDArray loss = lossFunction.evaluate(new NDList(label.expandDims(0)), new NDList(prediction.expandDims(0)));
        return loss;
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.elementType() == ElementType.FEATURE) {
            Feature<?, ?> feature = (Feature<?, ?>) element;
            if (("testLabel".equals(feature.getName()) || "feature".equals(feature.getName())) && feature.attachedTo.f0 == ElementType.VERTEX) {
                Vertex parent = (Vertex) feature.getElement();
                if (trainLossReady(parent)) {
                    NDArray loss = calculateLoss(inference.output((NDArray) parent.getFeature("feature").getValue(), false), (NDArray) parent.getFeature("testLabel").getValue());
                    if(PtNDArray.isValid(loss)) lossAggregator.reduce(loss,1);
                }
            }
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.elementType() == ElementType.FEATURE) {
            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
            if ("feature".equals(newFeature.getName()) && newFeature.attachedTo.f0 == ElementType.VERTEX) {
                Vertex parent = (Vertex) newFeature.getElement();
                if (trainLossReady(parent)) {
                    NDArray oldLoss = calculateLoss(inference.output((NDArray) oldFeature.getValue(), false), (NDArray) parent.getFeature("testLabel").getValue());
                    NDArray newLoss = calculateLoss(inference.output((NDArray) newFeature.getValue(), false), (NDArray) parent.getFeature("testLabel").getValue());
                    if(PtNDArray.isValid(newLoss)){
                        if(PtNDArray.isValid(oldLoss)){
                            lossAggregator.replace(newLoss, oldLoss);
                        }
                        else{
                            lossAggregator.reduce(newLoss,1);
                        }
                    }
                }
            }
        }
    }

    public boolean trainLossReady(Vertex v) {
        return Objects.nonNull(v) && Objects.nonNull(v.getFeature("feature")) && Objects.nonNull(v.getFeature("testLabel"));
    }

    @Override
    public void open() {
        super.open();
        inference = (VertexOutputLayer) storage.getPlugin("inferencer");
        lossAggregator = new MeanAggregator(inference.model.getNDManager().zeros(new Shape(), DataType.FLOAT32), true);
        storage.layerFunction
                .getRuntimeContext()
                .getMetricGroup()
                .gauge("loss", new MyGauge());
    }

    class MyGauge implements Gauge<Integer> {
        private final transient File outputFile;

        public MyGauge() {
            outputFile = new File("/Users/rustamwarwick/Desktop/output.txt");
            try {
                outputFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public Integer getValue() {
            float loss = lossAggregator.getValue().getFloat();
            try {
                Files.write(outputFile.toPath(), String.valueOf(loss).concat("\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return (int) (loss * 1000);
        }
    }


}
