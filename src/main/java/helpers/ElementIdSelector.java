package helpers;

import elements.Feature;
import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

public class ElementIdSelector implements KeySelector<GraphOp, String> {
    @Override
    public String getKey(GraphOp value) throws Exception {
        return value.element.getId();
    }
}
