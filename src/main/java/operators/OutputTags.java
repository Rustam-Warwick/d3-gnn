package operators;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class OutputTags {

    public static final OutputTag<GraphOp> FORWARD_OUTPUT_TAG = new OutputTag<>("forward", TypeInformation.of(GraphOp.class)); // used to retrive forward output, since hashmap cannot have null values

    public static OutputTag<GraphOp> ITERATE_OUTPUT_TAG = new OutputTag<>("startIteration", TypeInformation.of(GraphOp.class));

    public static OutputTag<GraphOp> BACKWARD_OUTPUT_TAG = new OutputTag<>("backward", TypeInformation.of(GraphOp.class));

    public static OutputTag<GraphOp> FULL_ITERATE_OUTPUT_TAG = new OutputTag<>("full-startIteration", TypeInformation.of(GraphOp.class));
}
