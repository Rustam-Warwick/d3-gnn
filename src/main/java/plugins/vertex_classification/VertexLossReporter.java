package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.SerializableLoss;
import elements.*;
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
    public int totalCorrect = 0;
    public int totalTested = 0;
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
                    NDArray maxArg = inference.output(new NDList((NDArray) parent.getFeature("feature").getValue()), false).get(0).argMax();
                    NDArray label = (NDArray) parent.getFeature("testLabel").getValue();
                    if (maxArg.eq(label).getBoolean()) {
                        totalCorrect++;
                    }
                    totalTested++;
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
                    NDArray maxArgNew = inference.output(new NDList((NDArray) newFeature.getValue()), false).get(0).argMax();
                    NDArray maxArgOld = inference.output(new NDList((NDArray) oldFeature.getValue()), false).get(0).argMax();
                    NDArray label = (NDArray) parent.getFeature("testLabel").getValue();
                    if (maxArgOld.eq(label).getBoolean() && !maxArgNew.eq(label).getBoolean()) {
                        // Old was correct now it is not correct
                        totalCorrect--;
                    }
                    if (!maxArgOld.eq(label).getBoolean() && maxArgNew.eq(label).getBoolean()) {
                        // Old was wrong now it is correct
                        totalCorrect++;
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
            float accuracy = (float) totalCorrect / totalTested;
            try {
                Files.write(outputFile.toPath(), String.valueOf(accuracy).concat("\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return (int) (accuracy * 1000);
        }
    }


}
