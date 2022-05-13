package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.GraphElement;
import elements.Plugin;
import elements.Vertex;
import ai.djl.ndarray.SerializableLoss;
import org.apache.flink.metrics.Gauge;

/**
 * Reports the model accuracy with new data
 * Assumes to be in the last layer
 * Expects vertexOutputLayer
 * Feature -> last last vertex embedding
 * testLabel -> Label to test by
 */
public class VertexLossReporter extends Plugin {
    public transient VertexOutputLayer inferencer;
    public SerializableLoss lossFunction;

    public VertexLossReporter(SerializableLoss lossFunction) {
        super("tester");
        this.lossFunction = lossFunction;
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
    }

    public Gauge<Integer> createGauge() {
        return new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                Float totalLoss = 0f;
                int sum = 0;
                for (Vertex v : storage.getVertices()) {
                    if (inferencer.outputReady(v) && v.getFeature("testLabel") != null) {
                        NDArray prediction = inferencer.output((NDArray) v.getFeature("feature").getValue(), false);
                        NDArray label = (NDArray) v.getFeature("testLabel").getValue();
                        NDArray loss = lossFunction.evaluate(new NDList(label.expandDims(0)), new NDList(prediction.expandDims(0)));
                        totalLoss += loss.getFloat();
                        sum++;
                    }
                }

                if (sum == 0) return 0;
                else{
                    float value = totalLoss / sum;
                    System.out.println(value);
                    return (int)(value * 1000);
                }
            }
        };
    }

    @Override
    public void open() {
        super.open();
        inferencer = (VertexOutputLayer) storage.getPlugin("inferencer");
        storage.layerFunction
                .getRuntimeContext()
                .getMetricGroup()
                .gauge("VertexOutputLoss", createGauge());
    }

}
