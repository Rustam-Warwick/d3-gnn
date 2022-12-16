package plugins.vertex_classification;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import elements.Feature;
import elements.GraphElement;
import elements.Vertex;
import elements.enums.ElementType;
import elements.enums.ReplicaState;
import elements.features.Tensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

public class VertexClassificationAccuracyReporter extends BaseVertexOutputPlugin {

    Tuple2<Integer, Integer> correctVsIncorrect = Tuple2.of(0, 0);

    public VertexClassificationAccuracyReporter(String modelName) {
        super(modelName, "accuracy-reporter");
    }

    public VertexClassificationAccuracyReporter(String modelName, boolean IS_ACTIVE) {
        super(modelName, "accuracy-reporter", IS_ACTIVE);
    }

    @Override
    public void open(Configuration params) throws Exception {
        super.open(params);
        getRuntimeContext().getMetricGroup().gauge("accuracy", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                int sum = correctVsIncorrect.f1 + correctVsIncorrect.f0;
                if (sum == 0) return 0;
                return (int) ((float) correctVsIncorrect.f0 / sum * 1000);
            }
        });
    }

    @Override
    public void addElementCallback(GraphElement element) {
        super.addElementCallback(element);
        if (element.getType() == ElementType.ATTACHED_FEATURE && element.state() == ReplicaState.MASTER) {
            Feature<?, ?> ft = (Feature<?, ?>) element;
            if (ft.getName().equals("f") || ft.getName().equals("l")) addNewTrainingSample((Vertex) ft.getElement());
        }
    }

    @Override
    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
        super.updateElementCallback(newElement, oldElement);
        if (newElement.getType() == ElementType.ATTACHED_FEATURE && newElement.state() == ReplicaState.MASTER) {
            Feature<?, ?> ft = (Feature<?, ?>) newElement;
            if (ft.getName().equals("f")) updateOldAccuracySample((Tensor) newElement, (Tensor) oldElement);
        }
    }

    public void updateOldAccuracySample(Tensor newFeature, Tensor oldFeature) {
        if (newFeature.getElement().containsFeature("l")) {
            int label = (int) newFeature.getElement().getFeature("l").getValue();
            boolean wasCorrect = isCorrect(new NDList(oldFeature.getValue()), label);
            boolean isCorrect = isCorrect(new NDList(newFeature.getValue()), label);
            if (wasCorrect != isCorrect) {
                if (wasCorrect) {
                    // Was but not now
                    correctVsIncorrect.f0--;
                    correctVsIncorrect.f1++;
                } else {
                    // Was incorrect not correct
                    correctVsIncorrect.f0++;
                    correctVsIncorrect.f1--;
                }
            }
        }
    }

    public void addNewTrainingSample(Vertex v) {
        if (v.containsFeature("f") && v.containsFeature("l")) {
            boolean isCorrect = isCorrect(new NDList((NDArray) v.getFeature("f").getValue()), (int) v.getFeature("l").getValue());
            if (isCorrect) correctVsIncorrect.f0++;
            else correctVsIncorrect.f1++;
        }
    }

    public boolean isCorrect(NDList input, int label) {
        NDArray res = output(input, false).get(0);
        return res.argMax().getLong() == label;
    }


}
