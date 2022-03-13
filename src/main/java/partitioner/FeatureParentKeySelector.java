package partitioner;

import elements.Feature;
import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

public class FeatureParentKeySelector implements KeySelector<GraphOp, String> {
    @Override
    public String getKey(GraphOp value) throws Exception {
        Feature feature = (Feature) value.element;
        return (String) feature.attachedTo._2;
    }
}
