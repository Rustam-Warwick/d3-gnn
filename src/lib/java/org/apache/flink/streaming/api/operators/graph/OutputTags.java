package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * Collection of {@link OutputTag} using in D3
 */
public class OutputTags {

    /**
     * Output tag for forward messages
     * @implNote DO NOT USE THIS DIRECTLY
     */
    public static final OutputTag<GraphOp> FORWARD_OUTPUT_TAG = new OutputTag<>("forward", TypeInformation.of(GraphOp.class)); // used to retrive forward output, since hashmap cannot have null values

    /**
     * Output Tag for same operator Iterations
     */
    public static final OutputTag<GraphOp> ITERATE_OUTPUT_TAG = new OutputTag<>("iteration", TypeInformation.of(GraphOp.class));

    /**
     * Output Tag for backward messages
     */
    public static final OutputTag<GraphOp> BACKWARD_OUTPUT_TAG = new OutputTag<>("backward", TypeInformation.of(GraphOp.class));

    /**
     * Output tag for full loop iterations
     */
    public static final OutputTag<GraphOp> FULL_ITERATE_OUTPUT_TAG = new OutputTag<>("full-iteration", TypeInformation.of(GraphOp.class));

    /**
     * OutputTag for <strong>train</strong> and <strong>test</strong> data dedicated for the last storage layer
     */
    public static final OutputTag<GraphOp> TRAIN_TEST_SPLIT_OUTPUT = new OutputTag<>("trainData", TypeInformation.of(GraphOp.class));

    /**
     * OutputTag for <strong>topology only data</strong> data dedicated for mid storage layers
     */
    public static final OutputTag<GraphOp> TOPOLOGY_ONLY_DATA_OUTPUT = new OutputTag<>("topologyData", TypeInformation.of(GraphOp.class));
}
