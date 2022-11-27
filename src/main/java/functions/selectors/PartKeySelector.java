package functions.selectors;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.PartNumber;

public class PartKeySelector implements KeySelector<GraphOp, PartNumber> {
    @Override
    public PartNumber getKey(GraphOp value) throws Exception {
        return new PartNumber(value.partId);
    }
}
