package functions.selectors;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.PartNumber;

public class PartKeySelector implements KeySelector<GraphOp, PartNumber> {
    private final PartNumber reuse;

    public PartKeySelector() {
        reuse = new PartNumber((short) 0);
    }

    @Override
    public PartNumber getKey(GraphOp value) throws Exception {
        reuse.setPartId(value.getPartId());
        return reuse;
    }
}
