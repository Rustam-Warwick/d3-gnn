package functions.selectors;

import elements.GraphOp;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Select exact elements of the same type going to the same parts
 */
public class ElementForPartKeySelector implements KeySelector<GraphOp, String> {
    @Override
    public String getKey(GraphOp value) throws Exception {
        return value.element.getId() + ":" + value.op.ordinal() + ":" + value.part_id;
    }
}
