package org.apache.flink.streaming.api.operators.graph;

import elements.GraphOp;
import org.apache.flink.runtime.state.PartNumber;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;

public class DatasetSplitterOperator extends KeyedProcessOperator<PartNumber, GraphOp, GraphOp> {
    public DatasetSplitterOperator(KeyedProcessFunction<PartNumber, GraphOp, GraphOp> function) {
        super(function);
    }

}
