package helpers;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

public class PartKeySelector implements KeySelector<GraphOp, String> {
    @Override
    public String getKey(GraphOp value) throws Exception {
        return String.valueOf(value.part_id);
    }
}
